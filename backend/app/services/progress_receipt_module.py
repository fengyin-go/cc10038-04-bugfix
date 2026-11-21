"""来源进度回执模块服务：接入通用资源路由的查询/导出骨架。

归属确认、取数位置推进等领域行为在 progress_receipt.ProgressReceiptService
中实现；这里只提供标准列表字段与只读查询能力。
"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ProgressReceiptModuleService(BaseModuleService):
    MODULE = "progress_receipt"
    ENTITY = "来源进度回执"
    OPERATION_TERM = "回执"
    LIST_FIELDS = [
        "receipt_no", "source_id", "batch_no", "region",
        "window_start", "window_end", "status",
    ]
    REQUIRED_FIELDS = ["receipt_no", "source_id", "batch_no"]
    STATUS_ORDER = ["待确认", "已确认", "归属冲突", "重复确认"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []
