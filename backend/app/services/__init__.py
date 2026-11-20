"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.dim_definition import DimDefinitionService
from app.services.dim_attr import DimAttrService
from app.services.fact_design import FactDesignService
from app.services.measure_config import MeasureConfigService
from app.services.scd_handler import ScdHandlerService
from app.services.surrogate_key import SurrogateKeyService
from app.services.hierarchy_build import HierarchyBuildService
from app.services.star_query import StarQueryService
from app.services.agg_summary import AggSummaryService
from app.services.dim_bridge import DimBridgeService
from app.services.etl_mapping import EtlMappingService
from app.services.transform_verification import TransformVerificationService
from app.services.snapshot_plan import SnapshotPlanService
from app.services.model_lineage import ModelLineageService
from app.services.conform_dim import ConformDimService
from app.services.data_vault import DataVaultService
from app.services.cube_builder import CubeBuilderService
from app.services.slow_change_log import SlowChangeLogService
from app.services.date_dim_gen import DateDimGenService
from app.services.model_validate import ModelValidateService
from app.services.deploy_pipeline import DeployPipelineService

SERVICES: dict[str, BaseModuleService] = {
    "dim_definition": DimDefinitionService(),
    "dim_attr": DimAttrService(),
    "fact_design": FactDesignService(),
    "measure_config": MeasureConfigService(),
    "scd_handler": ScdHandlerService(),
    "surrogate_key": SurrogateKeyService(),
    "hierarchy_build": HierarchyBuildService(),
    "star_query": StarQueryService(),
    "agg_summary": AggSummaryService(),
    "dim_bridge": DimBridgeService(),
    "etl_mapping": EtlMappingService(),
    "transform_verification": TransformVerificationService(),
    "snapshot_plan": SnapshotPlanService(),
    "model_lineage": ModelLineageService(),
    "conform_dim": ConformDimService(),
    "data_vault": DataVaultService(),
    "cube_builder": CubeBuilderService(),
    "slow_change_log": SlowChangeLogService(),
    "date_dim_gen": DateDimGenService(),
    "model_validate": ModelValidateService(),
    "deploy_pipeline": DeployPipelineService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
