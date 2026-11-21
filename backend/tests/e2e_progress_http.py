"""题述端到端场景的 HTTP 核验脚本（独立运行，不依赖被测进程内状态）。"""
import json
import urllib.request

BASE = "http://127.0.0.1:8013/api"


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def main() -> None:
    # 1. 登记南北区两个接入源。
    for source_id, region in (("north-src", "north"), ("south-src", "south")):
        call("POST", "/pipeline/source/register",
             {"source_id": source_id, "region": region})
        call("POST", f"/pipeline/source/validate?source_id={source_id}")

    # 2. 两批时间不同的原文，各自生成来源进度回执。
    north = call("POST", "/pipeline/batch/start", {
        "source_id": "north-src",
        "time_window": "2026-11-20T09:00:00",
        "raw_logs": [{"body": "2026-11-20T08:00:00|INFO|north raw"}]})
    south = call("POST", "/pipeline/batch/start", {
        "source_id": "south-src",
        "time_window": "2026-11-21T10:00:00",
        "raw_logs": [{"body": "2026-11-21T08:00:00|INFO|south raw"}]})
    nb, nr = north["batch"]["批次编号"], north["receipt_no"]
    sb, sr = south["batch"]["批次编号"], south["receipt_no"]
    print(f"批号: 北={nb} 南={sb} | 回执: 北={nr} 南={sr}")

    def confirm(source_id, batch_no, receipt_no, region, role, end=None):
        return call("POST", "/progress/confirm", {
            "source_id": source_id, "batch_no": batch_no,
            "receipt_no": receipt_no, "region": region, "role": role,
            "claimed_window_end": end})

    # 3. 换源提交：南区批号 + 北区回执 → 北区来源，必须拒绝且不移动水位。
    swapped = confirm("north-src", sb, nr, "north", "north-callback")
    print("换源提交:", swapped["decision"], "|", swapped["message"])
    assert swapped["ok"] is False and swapped["decision"] == "拒绝推进"

    # 4. 南北各自用本源批号 + 本源回执合法确认。
    ok_n = confirm("north-src", nb, nr, "north", "north-callback")
    ok_s = confirm("south-src", sb, sr, "south", "south-callback")
    print("北区确认:", ok_n["decision"], ok_n["watermark_value"])
    print("南区确认:", ok_s["decision"], ok_s["watermark_value"])
    assert ok_n["decision"] == "已确认推进"
    assert ok_s["decision"] == "已确认推进"

    # 5. 南区正确的回执连送两次：第二次必须指回第一次推进。
    again = confirm("south-src", sb, sr, "south", "south-callback")
    print("南区重复送达:", again["decision"],
          "pointed=", again["pointed_history_id"], "wm=", again["watermark_value"])
    assert again["decision"] == "重复确认指回"
    assert again["pointed_history_id"] == ok_s["history_id"]
    assert again["watermark_value"] == "2026-11-21T10:00:00"

    # 6. 越权与诊断只读。
    denied = confirm("north-src", nb, nr, "north", "south-callback")
    diag = confirm("north-src", nb, nr, "north", "cross_source_diagnostic")
    print("跨区角色:", denied["decision"], "| 诊断角色写入:", diag["decision"])
    assert denied["decision"] == "拒绝推进"
    assert diag["decision"] == "跨源只读拒绝"

    # 7. 下一次采集起止时间：从各自上次确认点之后开始。
    wn = call("POST", "/progress/next-window",
              {"source_id": "north-src", "window_end": "2026-11-20T10:00:00"})
    ws = call("POST", "/progress/next-window",
              {"source_id": "south-src", "window_end": "2026-11-21T12:00:00"})
    print("北区下次范围:", wn["window_start"], "~", wn["window_end"],
          "锚点", wn["from_receipt"])
    print("南区下次范围:", ws["window_start"], "~", ws["window_end"],
          "锚点", ws["from_receipt"])
    assert (wn["window_start"], wn["window_end"]) == (
        "2026-11-20T09:00:00", "2026-11-20T10:00:00")
    assert (ws["window_start"], ws["window_end"]) == (
        "2026-11-21T10:00:00", "2026-11-21T12:00:00")
    assert wn["from_receipt"] == nr and ws["from_receipt"] == sr

    # 8. 两边批号与确认点核对：互不串号。
    pn = call("GET", "/progress/confirmation-point?source_id=north-src")
    ps = call("GET", "/progress/confirmation-point?source_id=south-src")
    print("北区确认点:", pn["batch_no"], pn["receipt_no"], pn["watermark_value"])
    print("南区确认点:", ps["batch_no"], ps["receipt_no"], ps["watermark_value"])
    assert (pn["batch_no"], pn["receipt_no"]) == (nb, nr)
    assert (ps["batch_no"], ps["receipt_no"]) == (sb, sr)

    # 9. 调用方只读本源回执。
    mine = call("GET", "/progress/receipts?source_id=north-src")
    assert all(r["source_id"] == "north-src" for r in mine["items"])
    print("本源回执可见性: 北区调用方只看到",
          [r["receipt_no"] for r in mine["items"]])

    # 10. 待处理清单：换源请求被驳回，合法确认落已确认，全部可查。
    todos = call("GET", "/progress/pending")["items"]
    # 同一批号可能同时存在本源待办与冲突补登待办，按完整条目核对而非按键聚合。
    native_north = [t for t in todos
                    if (t["source_id"], t["batch_no"]) == ("north-src", nb)]
    foreign_north = [t for t in todos
                     if (t["source_id"], t["batch_no"]) == ("north-src", sb)]
    native_south = [t for t in todos
                    if (t["source_id"], t["batch_no"]) == ("south-src", sb)]
    print("待处理清单:")
    for t in todos:
        print("  ", t["source_id"], t["batch_no"], t["receipt_no"], "->", t["status"])
    assert len(native_north) == 1 and native_north[0]["status"] == "已确认"
    assert len(native_south) == 1 and native_south[0]["status"] == "已确认"
    assert len(foreign_north) == 1 and foreign_north[0]["status"] == "已驳回"
    assert all(t["pending"] is False for t in todos)

    # 11. 跨源诊断读两区推进历史。
    hn = call("GET",
              "/progress/diagnostic/region-history?region=north&role=cross_source_diagnostic")
    hs = call("GET",
              "/progress/diagnostic/region-history?region=south&role=cross_source_diagnostic")
    print(f"诊断历史: 北区 {hn['total']} 条, 南区 {hs['total']} 条（只读={hn['read_only']}）")
    assert hn["read_only"] is True
    assert all(e["region"] == "north" for e in hn["events"])

    print("\n全部端到端断言通过 ✔")


if __name__ == "__main__":
    main()
