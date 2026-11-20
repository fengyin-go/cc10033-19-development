"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

转换验证路由含 /api/etl-mappings/... 固定路径，必须先于资源路由注册，
否则会被资源路由的 /api/{module}/{entry_id} 动态路径抢先匹配。
"""
from __future__ import annotations

from app.routers import resource, verification

ROUTERS = [verification, resource]
