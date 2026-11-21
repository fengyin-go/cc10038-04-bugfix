"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline_router, progress_router

# progress_router 含 /api/progress/* 固定路径，必须排在 resource 的
# /api/{module}/{entry_id} 动态路径之前，否则固定路径会被抢先匹配。
ROUTERS = [progress_router, resource, pipeline_router]
