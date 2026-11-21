"""来源进度回执路由：签发回执、回调确认、取数范围与只读诊断接口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services.pipeline import pipeline
from app.services.progress_receipt import DIAGNOSTIC_ROLE, progress_receipt_service

router = APIRouter(prefix="/api/progress", tags=["来源进度回执"])


class ConfirmProgressPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    batch_no: str = Field(..., min_length=1)
    receipt_no: str = Field(..., min_length=1)
    region: str = Field(..., min_length=1)
    role: str = Field(..., min_length=1)
    claimed_window_end: str | None = None


class NextWindowPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    window_end: str = Field(..., min_length=1)


@router.post("/confirm")
def confirm_progress(payload: ConfirmProgressPayload) -> dict[str, Any]:
    """回调端提交进度确认；归属冲突时返回可查结论且不推进取数位置。"""
    return pipeline.confirm_ingest_progress(
        source_id=payload.source_id,
        batch_no=payload.batch_no,
        receipt_no=payload.receipt_no,
        region=payload.region,
        role=payload.role,
        claimed_window_end=payload.claimed_window_end,
    )


@router.post("/next-window")
def next_ingest_window(payload: NextWindowPayload) -> dict[str, Any]:
    """采集批次入口：返回上次确认点之后的采集范围。"""
    return pipeline.next_ingest_window(payload.source_id, payload.window_end)


@router.get("/receipts")
def list_receipts(source_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    """调用方只读本源可确认的进度回执。"""
    items = progress_receipt_service.list_source_receipts(source_id)
    return {"source_id": source_id, "items": items, "total": len(items)}


@router.get("/confirmation-point")
def get_confirmation_point(source_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    """读取本源最后一个已确认回执（下次取数锚点）。"""
    return progress_receipt_service.get_confirmation_point(source_id)


@router.get("/pending")
def list_pending(source_id: str | None = Query(default=None)) -> dict[str, Any]:
    """待处理清单：确认结果回写处，可按数据源过滤。"""
    items = progress_receipt_service.list_pending_confirmations(source_id)
    return {"items": items, "total": len(items)}


@router.get("/history")
def list_history(source_id: str | None = Query(default=None)) -> dict[str, Any]:
    """推进历史（本源自用视角）。"""
    items = progress_receipt_service.list_progress_history(source_id)
    return {"items": items, "total": len(items)}


@router.get("/diagnostic/region-history")
def read_region_history(
    region: str = Query(..., min_length=1),
    role: str = Query(default=DIAGNOSTIC_ROLE),
) -> dict[str, Any]:
    """跨源诊断：可读各区域推进历史；该角色的任何写入在确认接口被拒。"""
    return progress_receipt_service.read_region_history(region, role)
