"""来源进度回执：采集归属正确性的核心领域模块。

数据流（沿采集链路逐段修复归属）：

1. 采集批次启动时按「数据源 + 批次编号 + 时间窗口」生成**来源进度回执**，
   回执只归属本源，作为该批次可信的来源凭证。
2. 回调端推进进度时必须出示回执编号；本模块先核对回执、批次、数据源、采集区域
   四者一致，再决定是否允许推进该源的下次取数位置（水位）。
3. 确认结果回写到待处理清单（回执的 confirmation 状态 + 批次 pending 标记），
   调用方读取进度时只能读到本源已确认的回执。
4. 采集批次入口通过 get_confirmation_point / plan_ingest_window 读取本源回执，
   计算「上次确认点之后」的采集范围，避免从错误位置开始取数。

并发与权限：
- 进度提交与回执归属确认共用每源一把锁，串行裁决；只有资格核验通过的批次
  才能改变下次取数位置，归属待核对的请求也会落一条可查结论（不静默丢弃）。
- 提交权限按采集区域隔离（region_committers），跨源诊断角色只读各区域推进历史，
  任何写操作都被拒绝。
- 本源续传若再次收到已确认批号，直接指回上一次推进，不产生新水位事件。
"""
from __future__ import annotations

import threading
import time
from typing import Any

from app.store import store

# 回执/确认结论的状态机
RECEIPT_PENDING = "待确认"
RECEIPT_CONFIRMED = "已确认"
RECEIPT_REJECTED = "归属冲突"
RECEIPT_DUPLICATE = "重复确认"

# 推进历史（progress_history）的裁决结论
DECISION_CONFIRMED = "已确认推进"
DECISION_REJECTED = "拒绝推进"
DECISION_DUPLICATE = "重复确认指回"
DECISION_FOREIGN = "跨源只读拒绝"

# 待处理清单（pending_confirmations）处理结论
TODO_RESOLVED = "已确认"
TODO_REJECTED = "已驳回"

# 诊断角色：可读各区域推进历史，但无写权限
DIAGNOSTIC_ROLE = "cross_source_diagnostic"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


class ProgressReceiptService:
    """来源进度回执服务：管理回执签发、归属确认、取数位置与权限裁决。"""

    MODULE = "progress_receipt"

    def __init__(self) -> None:
        # 每个数据源一把锁：提交进度与归属确认并发时串行裁决，
        # 保证只有确有资格的批次能改变下次取数位置。
        self._locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()
        # 区域 -> 允许提交的角色集合；两个区域各有独立的提交权限。
        self.region_committers: dict[str, set[str]] = {
            "north": {"north-ingest", "north-callback"},
            "south": {"south-ingest", "south-callback"},
            # 全链路流水线等内部系统任务使用的默认区域权限。
            "default": {"system-ingest"},
        }

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------
    def _lock_for(self, source_id: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._locks.get(source_id)
            if lock is None:
                lock = threading.RLock()
                self._locks[source_id] = lock
            return lock

    def _table(self, name: str) -> list[dict[str, Any]]:
        return store.rows(name)

    def _get_source(self, source_id: str) -> dict[str, Any] | None:
        for src in self._table("source_registry"):
            if src.get("来源标识") == source_id:
                return src
        return None

    def _get_batch(self, batch_no: str) -> dict[str, Any] | None:
        for batch in self._table("ingest_batch"):
            if batch.get("批次编号") == batch_no:
                return batch
        return None

    def _find_receipt(self, receipt_no: str) -> dict[str, Any] | None:
        for receipt in self._table(self.MODULE):
            if receipt.get("receipt_no") == receipt_no:
                return receipt
        return None

    def _find_receipt_by_batch(self, source_id: str, batch_no: str) -> dict[str, Any] | None:
        for receipt in self._table(self.MODULE):
            if receipt.get("source_id") == source_id and receipt.get("batch_no") == batch_no:
                return receipt
        return None

    def _confirmed_receipts(self, source_id: str) -> list[dict[str, Any]]:
        return [
            r for r in self._table(self.MODULE)
            if r.get("source_id") == source_id and r.get("status") == RECEIPT_CONFIRMED
        ]

    def _append_history(self, entry: dict[str, Any]) -> None:
        entry.setdefault("id", _next_id("progress_history"))
        entry.setdefault("decided_at", _now_iso())
        self._table("progress_history").append(entry)

    def _append_todo(self, entry: dict[str, Any]) -> None:
        entry.setdefault("id", _next_id("pending_confirmations"))
        entry.setdefault("created_at", _now_iso())
        self._table("pending_confirmations").append(entry)

    # ------------------------------------------------------------------
    # 阶段 A：为每个接入源的每个批次生成可信的来源进度回执
    # ------------------------------------------------------------------
    def issue_receipt(
        self,
        source_id: str,
        batch_no: str,
        window_start: str,
        window_end: str,
        region: str,
    ) -> dict[str, Any]:
        """采集批次启动时签发来源进度回执。

        回执绑定数据源、批次、区域与时间窗口，是后续回调确认的唯一可信凭证。
        重复签发同一批次时幂等返回已有回执，不另造凭证。
        """
        with self._lock_for(source_id):
            existing = self._find_receipt_by_batch(source_id, batch_no)
            if existing is not None:
                return {"ok": True, "message": "回执已存在，幂等返回", "receipt": existing,
                        "duplicate": True}

            source = self._get_source(source_id)
            batch = self._get_batch(batch_no)
            # 批次必须真实归属该数据源，防止从入口就伪造归属。
            if batch is None or batch.get("所属数据源") != source_id:
                return {"ok": False,
                        "message": f"批次 {batch_no} 不属于数据源 {source_id}，无法签发回执"}
            source_region = source.get("采集区域") if source else region
            receipt_no = f"RCPT-{int(batch_no.split('-')[-1]):04d}"
            receipt = {
                "id": _next_id(self.MODULE),
                "receipt_no": receipt_no,
                "source_id": source_id,
                "batch_no": batch_no,
                "region": source_region or region,
                "window_start": window_start,
                "window_end": window_end,
                "watermark_value": window_end,
                "status": RECEIPT_PENDING,
                "pending": True,
                "abnormal": False,
                "issued_at": _now_iso(),
                "confirmed_at": "",
                "confirmation_reason": "",
                "confirm_count": 0,
            }
            self._table(self.MODULE).append(receipt)

            # 确认请求进入待处理清单，等待回调端出示回执确认。
            self._append_todo({
                "source_id": source_id,
                "batch_no": batch_no,
                "receipt_no": receipt_no,
                "region": receipt["region"],
                "window_start": window_start,
                "window_end": window_end,
                "status": "待处理",
                "pending": True,
                "resolution": "",
            })
            return {"ok": True, "message": f"来源进度回执 {receipt_no} 已签发",
                    "receipt": receipt, "duplicate": False}

    # ------------------------------------------------------------------
    # 阶段 B：回调端确认归属，只有资格核验通过的批次能推进取数位置
    # ------------------------------------------------------------------
    def confirm_progress(
        self,
        *,
        source_id: str,
        batch_no: str,
        receipt_no: str,
        region: str,
        role: str,
        claimed_window_end: str | None = None,
    ) -> dict[str, Any]:
        """确认一次采集进度并裁决是否推进该源的下次取数位置。

        核验链条（任一不通过即拒绝，但仍写可查结论）：
        1. 跨源诊断角色只读，直接拒绝写入；
        2. 角色必须具备该采集区域的独立提交权限；
        3. 回执必须存在且与数据源、批次、区域全部一致；
        4. 回调批号与回执/采集来源冲突时拒绝，禁止改动下次取数位置；
        5. 已确认批号再次送达（本源续传/重复回执）时指回上一次推进。
        """
        # 诊断角色在任何区域都只有读权限。
        if role == DIAGNOSTIC_ROLE:
            self._append_history({
                "source_id": source_id, "batch_no": batch_no, "receipt_no": receipt_no,
                "region": region, "role": role, "decision": DECISION_FOREIGN,
                "watermark_before": "", "watermark_after": "",
                "reason": "跨源诊断角色只读，无提交权限",
            })
            return {"ok": False, "qualified": False,
                    "decision": DECISION_FOREIGN,
                    "message": "跨源诊断仅可读推进历史，无提交权限"}

        with self._lock_for(source_id):
            source = self._get_source(source_id)
            source_region = source.get("采集区域") if source else region
            watermark_before = self.get_confirmation_point(source_id)["watermark_value"]

            # 区域独立提交权限校验。
            allowed = self.region_committers.get(source_region or region, set())
            if role not in allowed:
                reason = f"角色 {role} 无区域 {source_region or region} 的提交权限"
                self._append_history({
                    "source_id": source_id, "batch_no": batch_no, "receipt_no": receipt_no,
                    "region": source_region or region, "role": role,
                    "decision": DECISION_REJECTED,
                    "watermark_before": watermark_before, "watermark_after": watermark_before,
                    "reason": reason,
                })
                self._mark_todo(source_id, batch_no, TODO_REJECTED, reason,
                                receipt_no, append_only=True)
                return {"ok": False, "qualified": False,
                        "decision": DECISION_REJECTED, "message": reason}

            receipt = self._find_receipt(receipt_no)
            batch = self._get_batch(batch_no)

            # 回执不存在 / 批号与来源冲突 / 区域冲突：归属尚待核对，给出可查结论。
            conflicts: list[str] = []
            if receipt is None:
                conflicts.append(f"回执 {receipt_no} 不存在")
            else:
                if receipt.get("source_id") != source_id:
                    conflicts.append(
                        f"回执归属数据源 {receipt.get('source_id')} 与提交来源 {source_id} 冲突")
                if receipt.get("batch_no") != batch_no:
                    conflicts.append(
                        f"回调批号 {batch_no} 与回执批号 {receipt.get('batch_no')} 冲突")
                if receipt.get("region") != source_region:
                    conflicts.append(
                        f"回调区域 {source_region} 与回执区域 {receipt.get('region')} 冲突")
            if batch is None or batch.get("所属数据源") != source_id:
                conflicts.append(f"批次 {batch_no} 不属于数据源 {source_id}")
            if claimed_window_end and receipt and claimed_window_end != receipt.get("window_end"):
                conflicts.append(
                    f"回调窗口终点 {claimed_window_end} 与回执 {receipt.get('window_end')} 冲突")

            if conflicts:
                reason = "；".join(conflicts)
                # 注意：用「其他批号」发起的冲突请求不能污染本回执的归属状态，
                # 回执仍是该批次待合法确认的可信凭证；冲突结论只进历史与待办。
                # 仅当回调批号就是回执自身批号、却出现来源/区域错配时才标记回执异常。
                if receipt is not None and receipt.get("batch_no") == batch_no:
                    receipt["status"] = RECEIPT_REJECTED
                    receipt["abnormal"] = True
                    receipt["confirmation_reason"] = reason
                if batch is not None and batch.get("所属数据源") == source_id:
                    batch["abnormal"] = True
                self._append_history({
                    "source_id": source_id, "batch_no": batch_no, "receipt_no": receipt_no,
                    "region": source_region or region, "role": role,
                    "decision": DECISION_REJECTED,
                    "watermark_before": watermark_before, "watermark_after": watermark_before,
                    "reason": reason,
                })
                self._mark_todo(source_id, batch_no, TODO_REJECTED, reason,
                                receipt_no, append_only=True)
                return {"ok": False, "qualified": False,
                        "decision": DECISION_REJECTED, "message": reason,
                        "conflicts": conflicts}

            # 本源续传：曾确认的批号再次送达，指回那次推进，不产生新水位事件。
            if receipt.get("status") == RECEIPT_CONFIRMED:
                receipt["confirm_count"] = int(receipt.get("confirm_count", 1)) + 1
                pointed = self._find_history_confirmed(source_id, batch_no)
                self._append_history({
                    "source_id": source_id, "batch_no": batch_no, "receipt_no": receipt_no,
                    "region": receipt.get("region"), "role": role,
                    "decision": DECISION_DUPLICATE,
                    "watermark_before": watermark_before, "watermark_after": watermark_before,
                    "reason": f"批号 {batch_no} 已确认，指回既有推进",
                    "pointed_history_id": pointed.get("id") if pointed else None,
                })
                return {"ok": True, "qualified": True,
                        "decision": DECISION_DUPLICATE,
                        "message": f"批号 {batch_no} 已确认，指回上一次推进",
                        "receipt": receipt,
                        "watermark_value": watermark_before,
                        "pointed_history_id": pointed.get("id") if pointed else None}

            # 资格齐备：确认回执并推进下次取数位置（仅本源、本批号可改）。
            receipt["status"] = RECEIPT_CONFIRMED
            receipt["pending"] = False
            receipt["abnormal"] = False
            receipt["confirmed_at"] = _now_iso()
            receipt["confirmation_reason"] = "归属核验通过"
            receipt["confirm_count"] = int(receipt.get("confirm_count", 0)) + 1

            advanced = self._apply_watermark(source_id, batch_no, receipt)
            batch["status"] = "已完成"
            batch["pending"] = False
            batch["abnormal"] = False

            history_entry = {
                "source_id": source_id, "batch_no": batch_no, "receipt_no": receipt_no,
                "region": receipt.get("region"), "role": role,
                "decision": DECISION_CONFIRMED,
                "watermark_before": watermark_before,
                "watermark_after": receipt.get("watermark_value"),
                "window_start": receipt.get("window_start"),
                "window_end": receipt.get("window_end"),
                "reason": "回执、批号、来源、区域核验一致",
            }
            self._append_history(history_entry)
            self._mark_todo(source_id, batch_no, TODO_RESOLVED, "归属核验通过",
                           receipt_no)

            return {"ok": True, "qualified": True,
                    "decision": DECISION_CONFIRMED,
                    "message": f"批次 {batch_no} 归属确认，取数位置推进至 {receipt['window_end']}",
                    "receipt": receipt,
                    "watermark_before": watermark_before,
                    "watermark_value": receipt.get("watermark_value"),
                    "watermark_advanced": advanced,
                    "history_id": history_entry["id"]}

    def _apply_watermark(self, source_id: str, batch_no: str,
                         receipt: dict[str, Any]) -> bool:
        """把确认结果写回水位表（下次取数位置），并保持防倒退。"""
        new_value = receipt.get("watermark_value") or receipt.get("window_end")
        watermarks = self._table("watermark_mgr")
        existing = next((w for w in watermarks if w.get("数据源") == source_id), None)
        if existing is None:
            watermarks.append({
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": new_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
                "receipt_no": receipt.get("receipt_no"),
                "region": receipt.get("region"),
            })
            return True
        prev = existing.get("当前水位值", "")
        if prev and new_value < prev:
            # 已确认回执也不允许水位倒退；记录异常但不推进。
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["is_rollback"] = True
            return False
        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = new_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常"
        existing["abnormal"] = False
        existing["is_rollback"] = False
        existing["previous_value"] = prev
        existing["receipt_no"] = receipt.get("receipt_no")
        existing["region"] = receipt.get("region")
        return True

    def _mark_todo(self, source_id: str, batch_no: str, resolution: str,
                   reason: str, receipt_no: str = "",
                   append_only: bool = False) -> None:
        """确认结果回写待处理清单。

        必须同时匹配数据源、批号与回执号：换源冲突会给其他源的批号补登待办，
        只按 (数据源, 批号) 匹配可能错误驳回本源合法批次的待办。
        - append_only=True（归属冲突/越权路径）：绝不改写任何既有待办，只补登
          一条可查结论；但若同一回执已有终态待办（如已确认），拒绝结论只进
          推进历史，不再重复补登；
        - 正常确认路径：回执号完全相等才更新，否则同样补登。
        """
        if not append_only and receipt_no:
            for todo in self._table("pending_confirmations"):
                if todo.get("source_id") == source_id \
                        and todo.get("batch_no") == batch_no \
                        and todo.get("receipt_no") == receipt_no \
                        and todo.get("pending"):
                    todo["status"] = resolution
                    todo["pending"] = False
                    todo["resolution"] = reason
                    todo["resolved_at"] = _now_iso()
                    return
        if append_only and receipt_no:
            for todo in self._table("pending_confirmations"):
                # 同一回执已有终态结论，拒绝请求不再制造重复待办（历史仍留痕）。
                if todo.get("source_id") == source_id \
                        and todo.get("batch_no") == batch_no \
                        and todo.get("receipt_no") == receipt_no \
                        and not todo.get("pending"):
                    return
        # 待处理清单缺项或归属对不上时补登一条，保证每个结论都可查。
        self._append_todo({
            "source_id": source_id,
            "batch_no": batch_no,
            "receipt_no": receipt_no,
            "region": "",
            "window_start": "",
            "window_end": "",
            "status": resolution,
            "pending": False,
            "resolution": reason,
            "resolved_at": _now_iso(),
        })

    def _find_history_confirmed(self, source_id: str,
                                batch_no: str) -> dict[str, Any] | None:
        for event in reversed(self._table("progress_history")):
            if event.get("source_id") == source_id and event.get("batch_no") == batch_no \
                    and event.get("decision") == DECISION_CONFIRMED:
                return event
        return None

    # ------------------------------------------------------------------
    # 阶段 C：采集批次入口读取本源回执，计算上次确认点之后的采集范围
    # ------------------------------------------------------------------
    def get_confirmation_point(self, source_id: str) -> dict[str, Any]:
        """调用方只读本源可确认的进度：返回该源最后一个已确认回执作为取数锚点。"""
        confirmed = self._confirmed_receipts(source_id)
        if not confirmed:
            return {"source_id": source_id, "confirmed": False,
                    "watermark_value": "", "batch_no": "", "receipt_no": "",
                    "window_start": "", "window_end": ""}
        last = confirmed[-1]
        return {
            "source_id": source_id,
            "confirmed": True,
            "watermark_value": last.get("watermark_value") or last.get("window_end"),
            "batch_no": last.get("batch_no"),
            "receipt_no": last.get("receipt_no"),
            "window_start": last.get("window_start"),
            "window_end": last.get("window_end"),
            "confirmed_at": last.get("confirmed_at"),
        }

    def plan_ingest_window(self, source_id: str,
                           next_window_end: str) -> dict[str, Any]:
        """计算上次确认点之后的采集范围 [确认点, 新窗口终点]。

        采集入口只能从本源已确认回执之后取数；尚无确认点时从空起点开始。
        """
        point = self.get_confirmation_point(source_id)
        start = point["watermark_value"]
        return {
            "source_id": source_id,
            "window_start": start,
            "window_end": next_window_end,
            "from_batch": point.get("batch_no", ""),
            "from_receipt": point.get("receipt_no", ""),
            "has_confirmation_point": point["confirmed"],
        }

    # ------------------------------------------------------------------
    # 只读视图：调用方读本源进度；跨源诊断读各区域推进历史
    # ------------------------------------------------------------------
    def list_source_receipts(self, source_id: str) -> list[dict[str, Any]]:
        """调用方只能读取本源的回执（含待确认与已确认），看不到其他接入源。"""
        return [dict(r) for r in self._table(self.MODULE) if r.get("source_id") == source_id]

    def list_pending_confirmations(self, source_id: str | None = None) -> list[dict[str, Any]]:
        rows = self._table("pending_confirmations")
        if source_id is None:
            return [dict(r) for r in rows]
        return [dict(r) for r in rows if r.get("source_id") == source_id]

    def read_region_history(self, region: str, role: str) -> dict[str, Any]:
        """跨源诊断：可读各区域推进历史，但无写权限（写操作在 confirm 中被拒）。"""
        if role != DIAGNOSTIC_ROLE:
            return {"ok": False, "message": f"角色 {role} 无权读取区域 {region} 诊断历史"}
        events = [
            dict(e) for e in self._table("progress_history")
            if e.get("region") == region
        ]
        return {"ok": True, "region": region, "role": role, "read_only": True,
                "events": events, "total": len(events)}

    def list_progress_history(self, source_id: str | None = None) -> list[dict[str, Any]]:
        rows = self._table("progress_history")
        if source_id is None:
            return [dict(r) for r in rows]
        return [dict(r) for r in rows if r.get("source_id") == source_id]


progress_receipt_service = ProgressReceiptService()
