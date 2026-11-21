"""来源进度回执与采集归属正确性测试。

覆盖题述场景：
- 采集批次启动即为每个接入源生成可信来源进度回执；
- 传入其他接入源的批次/与采集来源冲突的批号不能确认成功，不移动取数位置；
- 采集入口从来源进度回执读取上次确认点，计算之后的采集起止时间；
- 确认结果回写待处理清单，调用方只读本源可确认的进度；
- 南北两区各有独立提交权限，跨源诊断只读推进历史；
- 提交与确认并发时，只有确有资格的批次改变下次取数位置，待核对请求也有可查结论；
- 本源续传收到曾确认的批号时指回那次推进，正确回执连送两次结论一致。
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.services.pipeline import pipeline
from app.services.progress_receipt import (
    DECISION_CONFIRMED,
    DECISION_DUPLICATE,
    DECISION_FOREIGN,
    DECISION_REJECTED,
    DIAGNOSTIC_ROLE,
    RECEIPT_CONFIRMED,
    RECEIPT_PENDING,
    RECEIPT_REJECTED,
    progress_receipt_service as svc,
)
from app.store import store


def _reset_receipt_tables() -> None:
    for name in ("progress_receipt", "progress_history", "pending_confirmations"):
        store.rows(name).clear()


def _register_validated(source_id: str, region: str) -> None:
    pipeline.register_source(source_id, "http", region)
    pipeline.validate_source(source_id)


def _start(source_id: str, start: str, end: str, logs: list[dict] | None = None) -> dict:
    result = pipeline.start_ingest_batch(
        source_id, end, logs or [{"body": f"{start}|INFO|msg-{source_id}"}],
        window_start=start, window_end=end,
    )
    assert result["ok"] is True, result["message"]
    return result["batch"]


def _receipt(source_id: str, batch_no: str) -> dict:
    found = svc._find_receipt_by_batch(source_id, batch_no)
    assert found is not None
    return found


def _watermark(source_id: str) -> str:
    """直接读水位表（下次取数位置的真实存储）。"""
    rows = [w for w in store.rows("watermark_mgr") if w.get("数据源") == source_id]
    return rows[0]["当前水位值"] if rows else ""


def _confirmation_value(source_id: str) -> str:
    """读来源进度回执视角的确认点（只统计仍处于已确认态的回执）。"""
    return svc.get_confirmation_point(source_id)["watermark_value"]


@pytest.fixture(autouse=True)
def reset_receipt_state(reset_store):
    _reset_receipt_tables()
    yield
    _reset_receipt_tables()


class TestReceiptIssuance:
    """每个接入源的每个批次都生成可信来源进度回执。"""

    def test_start_batch_issues_receipt(self):
        _register_validated("src-a", "north")
        batch = _start("src-a", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        receipt = _receipt("src-a", batch["批次编号"])
        assert receipt["receipt_no"].startswith("RCPT-")
        # 沿用已有批次编号：回执编号与批次编号尾号一致。
        assert receipt["receipt_no"].split("-")[1] == batch["批次编号"].split("-")[1]
        assert receipt["source_id"] == "src-a"
        assert receipt["batch_no"] == batch["批次编号"]
        assert receipt["region"] == "north"
        assert receipt["window_start"] == "2026-11-01T00:00:00"
        assert receipt["window_end"] == "2026-11-01T01:00:00"
        assert receipt["status"] == RECEIPT_PENDING
        assert receipt["pending"] is True

    def test_receipt_lands_in_pending_list(self):
        _register_validated("src-b", "south")
        batch = _start("src-b", "2026-11-02T00:00:00", "2026-11-02T02:00:00")
        todos = svc.list_pending_confirmations("src-b")
        assert len(todos) == 1
        todo = todos[0]
        assert todo["batch_no"] == batch["批次编号"]
        assert todo["receipt_no"] == batch["receipt_no"]
        assert todo["status"] == "待处理"
        assert todo["pending"] is True

    def test_receipt_rejects_batch_from_other_source(self):
        _register_validated("src-x", "north")
        # 不存在的批次或不属于本源的批次，不能签发出回执。
        result = svc.issue_receipt("src-x", "INGE-9999", "s", "e", "north")
        assert result["ok"] is False

    def test_issue_receipt_is_idempotent(self):
        _register_validated("src-c", "north")
        batch = _start("src-c", "2026-11-03T00:00:00", "2026-11-03T01:00:00")
        again = svc.issue_receipt(
            "src-c", batch["批次编号"], "2026-11-03T00:00:00",
            "2026-11-03T01:00:00", "north")
        assert again["ok"] is True and again["duplicate"] is True
        receipts = [r for r in store.rows("progress_receipt") if r["source_id"] == "src-c"]
        assert len(receipts) == 1


class TestAttributionConfirmation:
    """归属核验：只有确有资格的批次能确认成功并推进取数位置。"""

    def test_valid_confirmation_advances_watermark(self):
        _register_validated("src-n1", "north")
        batch = _start("src-n1", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        result = svc.confirm_progress(
            source_id="src-n1", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="north", role="north-callback")
        assert result["ok"] is True
        assert result["decision"] == DECISION_CONFIRMED
        assert result["watermark_advanced"] is True
        assert _watermark("src-n1") == "2026-11-01T01:00:00"
        # 回执与批次同步落确认态。
        assert _receipt("src-n1", batch["批次编号"])["status"] == RECEIPT_CONFIRMED
        assert store.rows("ingest_batch")[-1]["status"] == "已完成"
        # 确认结果回写待处理清单。
        todo = svc.list_pending_confirmations("src-n1")[0]
        assert todo["status"] == "已确认" and todo["pending"] is False

    def test_foreign_batch_cannot_confirm(self):
        _register_validated("src-north", "north")
        _register_validated("src-south", "south")
        b_north = _start("src-north", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        # 先完成北区批次，再启动南区批次（同源只允许一个进行中批次）。
        svc.confirm_progress(
            source_id="src-north", batch_no=b_north["批次编号"],
            receipt_no=b_north["receipt_no"], region="north", role="north-callback")
        b_south = _start("src-south", "2026-11-02T00:00:00", "2026-11-02T01:00:00")

        # 南区回调端拿南区批号、却出示北区回执，提交到北区来源。
        result = svc.confirm_progress(
            source_id="src-north", batch_no=b_south["批次编号"],
            receipt_no=b_north["receipt_no"], region="north", role="north-callback")
        assert result["ok"] is False
        assert result["decision"] == DECISION_REJECTED
        assert "冲突" in result["message"]
        # 北区取数位置没有被错误批号移动（仍停在本源自有的确认点）。
        assert _watermark("src-north") == "2026-11-01T01:00:00"
        # 被冒用的北区回执本身不被污染，仍是已确认的可信凭证。
        assert _receipt("src-north", b_north["批次编号"])["status"] == RECEIPT_CONFIRMED
        # 待核对请求也有可查结论：推进历史留痕，且补登一条驳回待办。
        history = svc.list_progress_history("src-north")
        assert any(e["decision"] == DECISION_REJECTED for e in history)
        rejected_todos = [t for t in svc.list_pending_confirmations("src-north")
                          if t["batch_no"] == b_south["批次编号"]]
        assert rejected_todos and rejected_todos[0]["status"] == "已驳回"

    def test_unknown_receipt_number_rejected_with_trace(self):
        _register_validated("src-n2", "north")
        batch = _start("src-n2", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        result = svc.confirm_progress(
            source_id="src-n2", batch_no=batch["批次编号"],
            receipt_no="RCPT-0000", region="north", role="north-callback")
        assert result["ok"] is False and result["decision"] == DECISION_REJECTED
        assert any("不存在" in c for c in result["conflicts"])
        assert _watermark("src-n2") == ""

    def test_window_end_mismatch_rejected(self):
        _register_validated("src-n3", "north")
        batch = _start("src-n3", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        result = svc.confirm_progress(
            source_id="src-n3", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="north", role="north-callback",
            claimed_window_end="2026-11-01T09:00:00")
        assert result["ok"] is False and result["decision"] == DECISION_REJECTED
        assert _watermark("src-n3") == ""

    def test_self_batch_conflict_marks_receipt_abnormal(self):
        # 回调批号就是回执自身批号，但回执归属来源与提交来源错配：此时标记回执异常。
        _register_validated("src-n4", "north")
        _register_validated("src-other4", "north")
        batch = _start("src-n4", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        receipt_no = batch["receipt_no"]
        # 篡改回执，使其批号与回调批号相同但归属另一个数据源。
        forged = svc._find_receipt(receipt_no)
        forged["source_id"] = "src-other4"
        result = svc.confirm_progress(
            source_id="src-n4", batch_no=batch["批次编号"],
            receipt_no=receipt_no, region="north", role="north-callback")
        assert result["ok"] is False and result["decision"] == DECISION_REJECTED
        receipt = svc._find_receipt(receipt_no)
        assert receipt["status"] == RECEIPT_REJECTED
        assert receipt["abnormal"] is True

    def test_foreign_batch_conflict_keeps_receipt_clean(self):
        # 用其他接入源的批号冲突时，被冒用回执保持原状态不被污染。
        _register_validated("src-n5", "north")
        _register_validated("src-s5", "south")
        b_north = _start("src-n5", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        b_south = _start("src-s5", "2026-11-02T00:00:00", "2026-11-02T01:00:00")
        svc.confirm_progress(
            source_id="src-n5", batch_no=b_south["批次编号"],
            receipt_no=b_north["receipt_no"], region="north", role="north-callback")
        receipt = _receipt("src-n5", b_north["批次编号"])
        assert receipt["status"] == RECEIPT_PENDING
        assert receipt["abnormal"] is False

    def test_conflict_todo_does_not_hijack_native_todo(self):
        # 回归：换源冲突补登的待办不能抢先匹配本源合法批次的待办。
        _register_validated("src-n6", "north")
        _register_validated("src-s6", "south")
        b_north = _start("src-n6", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        b_south = _start("src-s6", "2026-11-02T00:00:00", "2026-11-02T01:00:00")
        # 先发生一次换源冲突（南区批号送到北区）。
        svc.confirm_progress(
            source_id="src-n6", batch_no=b_south["批次编号"],
            receipt_no=b_north["receipt_no"], region="north", role="north-callback")
        # 本源批次随后合法确认，其待办必须落「已确认」而非被冲突结论带偏。
        ok = svc.confirm_progress(
            source_id="src-n6", batch_no=b_north["批次编号"],
            receipt_no=b_north["receipt_no"], region="north", role="north-callback")
        assert ok["decision"] == DECISION_CONFIRMED
        native_todos = [t for t in svc.list_pending_confirmations("src-n6")
                        if t["batch_no"] == b_north["批次编号"]]
        # 本源批号只有一条待办，且状态为已确认（未被冲突补登条目污染）。
        assert len(native_todos) == 1
        assert native_todos[0]["status"] == "已确认"
        assert native_todos[0]["receipt_no"] == b_north["receipt_no"]
        foreign_todos = [t for t in svc.list_pending_confirmations("src-n6")
                         if t["batch_no"] == b_south["批次编号"]]
        assert len(foreign_todos) == 1
        assert foreign_todos[0]["status"] == "已驳回"


class TestRegionPermissions:
    """两个区域各有独立提交权限；跨源诊断只读。"""

    def test_south_role_cannot_commit_north(self):
        _register_validated("src-p1", "north")
        batch = _start("src-p1", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        result = svc.confirm_progress(
            source_id="src-p1", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="north", role="south-callback")
        assert result["ok"] is False and result["decision"] == DECISION_REJECTED
        assert "提交权限" in result["message"]
        assert _watermark("src-p1") == ""

    def test_north_role_cannot_commit_south(self):
        _register_validated("src-p2", "south")
        batch = _start("src-p2", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        result = svc.confirm_progress(
            source_id="src-p2", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="south", role="north-ingest")
        assert result["ok"] is False and result["decision"] == DECISION_REJECTED
        assert _watermark("src-p2") == ""

    def test_diagnostic_role_is_read_only(self):
        _register_validated("src-p3", "north")
        batch = _start("src-p3", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        # 诊断角色可以读各区域推进历史。
        svc.confirm_progress(
            source_id="src-p3", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="north", role="north-callback")
        view = svc.read_region_history("north", DIAGNOSTIC_ROLE)
        assert view["ok"] is True and view["read_only"] is True
        assert view["total"] >= 1
        # 但诊断角色的写入（确认）一律被拒。
        denied = svc.confirm_progress(
            source_id="src-p3", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="north", role=DIAGNOSTIC_ROLE)
        assert denied["ok"] is False and denied["decision"] == DECISION_FOREIGN
        history = svc.list_progress_history("src-p3")
        assert any(e["decision"] == DECISION_FOREIGN for e in history)

    def test_non_diagnostic_role_cannot_read_region_diagnostics(self):
        view = svc.read_region_history("north", "north-callback")
        assert view["ok"] is False

    def test_rejection_after_confirmation_adds_no_duplicate_todo(self):
        # 同一回执已确认后，越权/冲突拒绝只进推进历史，不制造重复待办。
        _register_validated("src-p4", "north")
        batch = _start("src-p4", "2026-11-01T00:00:00", "2026-11-01T01:00:00")
        svc.confirm_progress(
            source_id="src-p4", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="north", role="north-callback")
        todos_after_confirm = len(svc.list_pending_confirmations("src-p4"))
        # 同源同批号同回执，但角色越权。
        denied = svc.confirm_progress(
            source_id="src-p4", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="north", role="south-callback")
        assert denied["decision"] == DECISION_REJECTED
        assert len(svc.list_pending_confirmations("src-p4")) == todos_after_confirm
        # 拒绝结论仍可在推进历史中查到。
        assert any(e["decision"] == DECISION_REJECTED
                   for e in svc.list_progress_history("src-p4"))


class TestDuplicateAndResume:
    """本源续传：曾确认的批号再次送达，指回那次推进。"""

    def test_correct_receipt_sent_twice_points_back(self):
        _register_validated("src-d1", "south")
        batch = _start("src-d1", "2026-11-05T00:00:00", "2026-11-05T02:00:00")
        first = svc.confirm_progress(
            source_id="src-d1", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="south", role="south-callback")
        assert first["decision"] == DECISION_CONFIRMED
        first_history_id = first["history_id"]

        # 南区正确的回执连送第二次。
        second = svc.confirm_progress(
            source_id="src-d1", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="south", role="south-callback")
        assert second["ok"] is True
        assert second["decision"] == DECISION_DUPLICATE
        # 指回第一次推进，不产生新的水位移动。
        assert second["pointed_history_id"] == first_history_id
        assert second["watermark_value"] == "2026-11-05T02:00:00"
        receipt = _receipt("src-d1", batch["批次编号"])
        assert receipt["confirm_count"] == 2
        assert receipt["status"] == RECEIPT_CONFIRMED
        # 推进历史中两次结论都可查。
        history = svc.list_progress_history("src-d1")
        decisions = [e["decision"] for e in history]
        assert decisions.count(DECISION_CONFIRMED) == 1
        assert decisions.count(DECISION_DUPLICATE) == 1

    def test_duplicate_does_not_change_watermark_or_todo(self):
        _register_validated("src-d2", "south")
        batch = _start("src-d2", "2026-11-05T00:00:00", "2026-11-05T02:00:00")
        svc.confirm_progress(
            source_id="src-d2", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="south", role="south-callback")
        todos_before = len(svc.list_pending_confirmations("src-d2"))
        svc.confirm_progress(
            source_id="src-d2", batch_no=batch["批次编号"],
            receipt_no=batch["receipt_no"], region="south", role="south-callback")
        assert len(svc.list_pending_confirmations("src-d2")) == todos_before
        assert _watermark("src-d2") == "2026-11-05T02:00:00"


class TestNextIngestWindow:
    """采集批次入口读取来源进度回执，计算上次确认点之后的采集范围。"""

    def test_first_batch_starts_from_empty_point(self):
        _register_validated("src-w1", "north")
        plan = pipeline.next_ingest_window("src-w1", "2026-11-10T01:00:00")
        assert plan["has_confirmation_point"] is False
        assert plan["window_start"] == ""
        assert plan["window_end"] == "2026-11-10T01:00:00"

    def test_window_starts_after_last_confirmation(self):
        _register_validated("src-w2", "north")
        b1 = _start("src-w2", "2026-11-10T00:00:00", "2026-11-10T01:00:00")
        svc.confirm_progress(
            source_id="src-w2", batch_no=b1["批次编号"],
            receipt_no=b1["receipt_no"], region="north", role="north-ingest")

        plan = pipeline.next_ingest_window("src-w2", "2026-11-10T02:00:00")
        assert plan["has_confirmation_point"] is True
        # 下一次采集从上次确认点开始，到新窗口终点结束。
        assert plan["window_start"] == "2026-11-10T01:00:00"
        assert plan["window_end"] == "2026-11-10T02:00:00"
        assert plan["from_receipt"] == b1["receipt_no"]

        # 实际启动的下一批次携带同样的起止时间，且锚定到上一张回执。
        b2 = _start("src-w2", plan["window_start"], plan["window_end"])
        assert b2["window_start"] == "2026-11-10T01:00:00"
        assert b2["window_end"] == "2026-11-10T02:00:00"
        assert b2["ingest_from_confirmation"] is True
        assert b2["from_receipt"] == b1["receipt_no"]

    def test_unconfirmed_receipt_does_not_move_start_point(self):
        _register_validated("src-w3", "north")
        _start("src-w3", "2026-11-10T00:00:00", "2026-11-10T01:00:00")
        # 只启动、未确认：确认点仍为空，入口不会从未确认位置取数。
        plan = pipeline.next_ingest_window("src-w3", "2026-11-10T03:00:00")
        assert plan["has_confirmation_point"] is False
        assert plan["window_start"] == ""


class TestCallerIsolation:
    """调用方只读本源可确认的进度，看不到其他接入源。"""

    def test_list_receipts_scoped_to_source(self):
        _register_validated("src-iso1", "north")
        _register_validated("src-iso2", "south")
        b1 = _start("src-iso1", "2026-11-11T00:00:00", "2026-11-11T01:00:00")
        _start("src-iso2", "2026-11-11T00:00:00", "2026-11-11T01:00:00")
        mine = svc.list_source_receipts("src-iso1")
        assert len(mine) == 1
        assert mine[0]["receipt_no"] == b1["receipt_no"]
        assert all(r["source_id"] == "src-iso1" for r in mine)

    def test_confirmation_point_scoped_to_source(self):
        _register_validated("src-iso3", "north")
        _register_validated("src-iso4", "south")
        b = _start("src-iso3", "2026-11-11T00:00:00", "2026-11-11T01:00:00")
        svc.confirm_progress(
            source_id="src-iso3", batch_no=b["批次编号"],
            receipt_no=b["receipt_no"], region="north", role="north-ingest")
        other = svc.get_confirmation_point("src-iso4")
        assert other["confirmed"] is False and other["watermark_value"] == ""


class TestConcurrentConfirmation:
    """提交进度与回执归属确认并发：只有合格批次改变取数位置。"""

    def test_concurrent_mixed_requests_only_qualified_advances(self):
        _register_validated("src-cn", "north")
        _register_validated("src-cs", "south")
        # 北区两批、南区两批，时间不同；逐批启动并确认（同源同时只允许一个进行中批次）。
        bn1 = _start("src-cn", "2026-11-20T00:00:00", "2026-11-20T01:00:00")
        svc.confirm_progress(
            source_id="src-cn", batch_no=bn1["批次编号"],
            receipt_no=bn1["receipt_no"], region="north", role="north-ingest")
        bn2 = _start("src-cn", "2026-11-20T01:00:00", "2026-11-20T02:00:00")
        bs1 = _start("src-cs", "2026-11-21T00:00:00", "2026-11-21T01:00:00")
        svc.confirm_progress(
            source_id="src-cs", batch_no=bs1["批次编号"],
            receipt_no=bs1["receipt_no"], region="south", role="south-ingest")
        bs2 = _start("src-cs", "2026-11-21T01:00:00", "2026-11-21T02:00:00")

        jobs = [
            # 合格：北区在途批次由北区角色确认（bn1 已确认，这里并发的是 bn2）。
            dict(source_id="src-cn", batch_no=bn2["批次编号"],
                 receipt_no=bn2["receipt_no"], region="north", role="north-callback"),
            # 合格：南区在途批次。
            dict(source_id="src-cs", batch_no=bs2["批次编号"],
                 receipt_no=bs2["receipt_no"], region="south", role="south-callback"),
            # 冲突：南区批号配北区回执，往北区来源提交。
            dict(source_id="src-cn", batch_no=bs1["批次编号"],
                 receipt_no=bn1["receipt_no"], region="north", role="north-callback"),
            # 越权：南区角色提交北区。
            dict(source_id="src-cn", batch_no=bn2["批次编号"],
                 receipt_no=bn2["receipt_no"], region="north", role="south-ingest"),
        ]

        with ThreadPoolExecutor(max_workers=4) as pool:
            outcomes = list(pool.map(lambda j: svc.confirm_progress(**j), jobs))

        decisions = [o["decision"] for o in outcomes]
        assert decisions.count(DECISION_CONFIRMED) == 2
        assert decisions.count(DECISION_REJECTED) == 2
        # 北区取数位置只被自己的合格批次推进到最晚窗口。
        assert _watermark("src-cn") == "2026-11-20T02:00:00"
        assert _watermark("src-cs") == "2026-11-21T02:00:00"
        # 每个并发请求都有可查结论（含此前的两次串行确认）。
        assert len(svc.list_progress_history()) == 6
        # 待处理清单里合格的全部确认、冲突的全部驳回，没有悬挂项。
        todos = svc.list_pending_confirmations()
        assert all(t["pending"] is False for t in todos)
        assert {t["status"] for t in todos} == {"已确认", "已驳回"}

    def test_concurrent_duplicate_confirmations_advance_once(self):
        _register_validated("src-cd", "south")
        batch = _start("src-cd", "2026-11-22T00:00:00", "2026-11-22T01:00:00")
        job = dict(source_id="src-cd", batch_no=batch["批次编号"],
                   receipt_no=batch["receipt_no"], region="south", role="south-callback")
        with ThreadPoolExecutor(max_workers=4) as pool:
            outcomes = list(pool.map(lambda _: svc.confirm_progress(**job), range(4)))
        decisions = [o["decision"] for o in outcomes]
        assert decisions.count(DECISION_CONFIRMED) == 1
        assert decisions.count(DECISION_DUPLICATE) == 3
        assert _watermark("src-cd") == "2026-11-22T01:00:00"
        assert _receipt("src-cd", batch["批次编号"])["confirm_count"] == 4


class TestNorthSouthSwapScenario:
    """题述端到端：南北区两批时间不同的原文换源提交，南区正确回执连送两次，
    再从下一次采集的起止时间和两边批号核对。"""

    def test_full_swap_then_duplicate_then_next_window(self):
        _register_validated("north-src", "north")
        _register_validated("south-src", "south")

        # 两批时间不同的原文。
        north_batch = _start(
            "north-src", "2026-11-20T08:00:00", "2026-11-20T09:00:00",
            [{"body": "2026-11-20T08:00:00|INFO|north raw"}])
        south_batch = _start(
            "south-src", "2026-11-21T08:00:00", "2026-11-21T10:00:00",
            [{"body": "2026-11-21T08:00:00|INFO|south raw"}])

        # 换源提交：把南区批号配北区回执送到北区来源 —— 必须失败。
        swapped = svc.confirm_progress(
            source_id="north-src", batch_no=south_batch["批次编号"],
            receipt_no=north_batch["receipt_no"], region="north", role="north-callback")
        assert swapped["ok"] is False
        assert swapped["decision"] == DECISION_REJECTED
        # 批号与来源对得上的信息出现在可查结论里。
        assert south_batch["批次编号"] in swapped["message"]
        assert _watermark("north-src") == ""

        # 反向换源：北区批号配南区回执送到南区来源 —— 同样失败。
        swapped_reverse = svc.confirm_progress(
            source_id="south-src", batch_no=north_batch["批次编号"],
            receipt_no=south_batch["receipt_no"], region="south", role="south-callback")
        assert swapped_reverse["ok"] is False
        assert _watermark("south-src") == ""

        # 南北各自用本源批号+本源回执正常确认。
        ok_north = svc.confirm_progress(
            source_id="north-src", batch_no=north_batch["批次编号"],
            receipt_no=north_batch["receipt_no"], region="north", role="north-callback")
        ok_south = svc.confirm_progress(
            source_id="south-src", batch_no=south_batch["批次编号"],
            receipt_no=south_batch["receipt_no"], region="south", role="south-callback")
        assert ok_north["decision"] == DECISION_CONFIRMED
        assert ok_south["decision"] == DECISION_CONFIRMED

        # 南区正确的回执连送两次：第二次指回第一次推进。
        south_again = svc.confirm_progress(
            source_id="south-src", batch_no=south_batch["批次编号"],
            receipt_no=south_batch["receipt_no"], region="south", role="south-callback")
        assert south_again["decision"] == DECISION_DUPLICATE
        assert south_again["pointed_history_id"] == ok_south["history_id"]

        # 核对两边批号与水位：各自独立、互不串号。
        north_point = svc.get_confirmation_point("north-src")
        south_point = svc.get_confirmation_point("south-src")
        assert north_point["batch_no"] == north_batch["批次编号"]
        assert north_point["receipt_no"] == north_batch["receipt_no"]
        assert north_point["watermark_value"] == "2026-11-20T09:00:00"
        assert south_point["batch_no"] == south_batch["批次编号"]
        assert south_point["receipt_no"] == south_batch["receipt_no"]
        assert south_point["watermark_value"] == "2026-11-21T10:00:00"

        # 下一次采集的起止时间必须从各自上次确认点之后开始。
        next_north = pipeline.next_ingest_window("north-src", "2026-11-20T10:00:00")
        next_south = pipeline.next_ingest_window("south-src", "2026-11-21T12:00:00")
        assert (next_north["window_start"], next_north["window_end"]) == (
            "2026-11-20T09:00:00", "2026-11-20T10:00:00")
        assert (next_south["window_start"], next_south["window_end"]) == (
            "2026-11-21T10:00:00", "2026-11-21T12:00:00")
        # 锚点回执分属两边，没有错源。
        assert next_north["from_receipt"] == north_batch["receipt_no"]
        assert next_south["from_receipt"] == south_batch["receipt_no"]

        # 待处理清单：换源请求被驳回，正常确认落「已确认」，全部可查。
        todos = svc.list_pending_confirmations()
        native_north = [t for t in todos
                        if (t["source_id"], t["batch_no"]) == ("north-src", north_batch["批次编号"])]
        native_south = [t for t in todos
                        if (t["source_id"], t["batch_no"]) == ("south-src", south_batch["批次编号"])]
        swapped_north = [t for t in todos
                         if (t["source_id"], t["batch_no"]) == ("north-src", south_batch["批次编号"])]
        swapped_south = [t for t in todos
                         if (t["source_id"], t["batch_no"]) == ("south-src", north_batch["批次编号"])]
        assert len(native_north) == 1 and native_north[0]["status"] == "已确认"
        assert len(native_south) == 1 and native_south[0]["status"] == "已确认"
        assert len(swapped_north) == 1 and swapped_north[0]["status"] == "已驳回"
        assert len(swapped_south) == 1 and swapped_south[0]["status"] == "已驳回"
        assert all(t["pending"] is False for t in todos)

        # 跨源诊断能读两个区域的推进历史，但总数包含拒绝/重复等全部结论。
        north_view = svc.read_region_history("north", DIAGNOSTIC_ROLE)
        south_view = svc.read_region_history("south", DIAGNOSTIC_ROLE)
        assert north_view["total"] >= 2 and south_view["total"] >= 3
        assert all(e["region"] == "north" for e in north_view["events"])
        assert all(e["region"] == "south" for e in south_view["events"])
