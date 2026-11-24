"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import chains, incremental, resource

# 增量回执路由必须排在通用资源路由 /api/{module} 之前，避免固定路径被动态匹配抢占。
ROUTERS = [incremental, resource, chains]
