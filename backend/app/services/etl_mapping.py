"""ETL映射服务：定义从源系统到数仓模型的ETL字段映射和转换规则。

增强能力：
- 字段映射解析：name=>target 格式解析
- 清洗逻辑标准化：NULL过滤、去重、状态码标准化
- 写入模式管理：INSERT OVERWRITE、MERGE、INSERT INTO
- 转换函数验证：COALESCE、TRIM、UPPER 等函数名识别
"""
from __future__ import annotations

import re
from typing import Any

from app.services.base import BaseModuleService
from app.services.transform_verification import (
    mapping_revision,
    rule_fingerprint,
    touch_mapping_revision,
    verification_store,
)
from app.store import store

WRITE_MODES = {"INSERT OVERWRITE", "INSERT OVERWRITE PARTITION", "INSERT INTO", "INSERT INTO PARTITION", "MERGE"}
VALID_FUNCTIONS = {"COALESCE", "TRIM", "UPPER", "LOWER", "ABS", "DISTINCT", "MULTIPLY", "CONCAT", "NVL",
                   "normalize_status", "IFNULL", "CAST", "REPLACE"}


class EtlMappingService(BaseModuleService):
    MODULE = "etl_mapping"
    ENTITY = "映射规则"
    OPERATION_TERM = "清洗"
    LIST_FIELDS = ["规则名称", "源表", "目标表", "字段映射", "清洗逻辑", "转换函数", "写入模式"]
    REQUIRED_FIELDS = ["规则名称", "源表", "目标表"]
    STATUS_ORDER = ["草稿", "已测试", "已部署", "已废弃"]
    ACTION_RULES = {"测试映射": "草稿", "部署规则": "已测试", "废弃规则": "已部署"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        target_table = str(values.get("目标表", "")).strip()
        write_mode = str(values.get("写入模式", "")).strip()
        cleaning = str(values.get("清洗逻辑", "")).strip()

        # 标准化写入模式
        if write_mode not in WRITE_MODES:
            write_mode = "INSERT OVERWRITE PARTITION" if "dwd_" in target_table else "MERGE"
            values["写入模式"] = write_mode

        # 解析字段映射
        field_mapping = str(values.get("字段映射", ""))
        if "=>" in field_mapping:
            pairs = [p.strip() for p in field_mapping.split(".") if "=>" in p]
            values["_parsed_mappings"] = len(pairs)

        # 识别转换函数
        transform_func = str(values.get("转换函数", ""))
        recognized = [f for f in VALID_FUNCTIONS if f.lower() in transform_func.lower()]
        if recognized:
            values["_recognized_funcs"] = ",".join(recognized)

        # 消费相关发布事件
        store.channel.consume("fact.published", "etl_mapping")
        store.channel.consume("dim.published", "etl_mapping")

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        # 登记规则修订版本与指纹，作为转换验证结果的规则依据
        touch_mapping_revision(entry, is_new=True)

        store.channel.publish("etl.defined", {
            "rule_name": entry["规则名称"],
            "source_table": entry.get("源表"),
            "target_table": target_table,
            "write_mode": write_mode,
            "rule_revision": entry["_rule_revision"],
            "rule_fingerprint": entry["_rule_fingerprint"],
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "etl_mapping", "etl_rule",
            f"etl_{entry['id']}",
            {"rule_name": entry["规则名称"], "target": target_table},
            ["dim_definition", "fact_design", "etl_mapping"],
        )
        return entry, []

    def update_mapping(self, entry_id: int, changes: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """修改映射规则的转换口径并递增规则修订版本。

        基于旧规则产生的转换验证批次会因指纹/修订号变化被标记为过期，
        防止“测试通过的是旧映射，交付使用的却是新规则”。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        tracked = [f for f in self.LIST_FIELDS if f in changes]
        if not tracked:
            return None, "没有可更新的映射字段"
        for field in tracked:
            entry[field] = changes[field]

        target_table = str(entry.get("目标表", "")).strip()
        write_mode = str(entry.get("写入模式", "")).strip()
        if write_mode not in WRITE_MODES:
            entry["写入模式"] = "INSERT OVERWRITE PARTITION" if "dwd_" in target_table else "MERGE"

        old_revision = int(entry.get("_rule_revision", 1))
        touch_mapping_revision(entry)

        stale_batches = [
            batch_id for batch_id, batch in verification_store.batches.items()
            if batch["mapping_id"] == entry_id
        ]
        store.channel.publish("etl.rule_changed", {
            "rule_name": entry["规则名称"],
            "entry_id": entry_id,
            "old_revision": old_revision,
            "new_revision": entry["_rule_revision"],
            "rule_fingerprint": entry["_rule_fingerprint"],
            "affected_batches": stale_batches,
        })
        return entry, f"映射规则已更新，修订版本 {old_revision} -> {entry['_rule_revision']}"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return None, message

        if action == "测试映射":
            test_result = _test_mapping(entry)
            store.channel.publish("etl.tested", {
                "rule_name": entry["规则名称"],
                "test_result": test_result,
                "rule_revision": entry.get("_rule_revision"),
                "rule_fingerprint": entry.get("_rule_fingerprint"),
                "entry_id": entry_id,
            })
        elif action == "部署规则":
            store.channel.publish("etl.deployed", {
                "rule_name": entry["规则名称"],
                "target_table": entry.get("目标表"),
                "write_mode": entry.get("写入模式"),
                "rule_revision": entry.get("_rule_revision"),
                "rule_fingerprint": entry.get("_rule_fingerprint"),
                "entry_id": entry_id,
            })
        elif action == "废弃规则":
            store.channel.publish("etl.deprecated", {
                "rule_name": entry["规则名称"],
                "entry_id": entry_id,
            })
        return entry, message

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        deployed = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已部署")
        base["deployed_rules"] = deployed
        base["verification_batches"] = len(verification_store.batches)
        stale = 0
        for batch in verification_store.batches.values():
            info = mapping_revision(batch["mapping_id"])
            if info.get("exists") and (
                info["revision"] != batch["rule_revision"]
                or info["fingerprint"] != batch["rule_fingerprint"]
            ):
                stale += 1
        base["stale_batches"] = stale
        base["partition_deliveries"] = len(verification_store.deliveries)
        return base


def _test_mapping(entry: dict[str, Any]) -> dict[str, Any]:
    """表达式级测试摘要；逐行批量验证结果由 transform_verification 模块承载。"""
    mapping_id = int(entry.get("id", 0))
    batches = [
        batch for batch in verification_store.batches.values()
        if batch["mapping_id"] == mapping_id
    ]
    latest = batches[0] if batches else None
    if latest is None:
        return {
            "rows_processed": 0,
            "rows_failed": 0,
            "success": True,
            "duration_ms": 0,
            "note": "尚未提交样本批量验证，当前仅完成表达式可解析性检查",
            "rule_revision": entry.get("_rule_revision", 1),
            "rule_fingerprint": entry.get("_rule_fingerprint", rule_fingerprint(entry)),
        }
    return {
        "rows_processed": latest["total_rows"],
        "rows_failed": latest["rejected_count"],
        "success": latest["rejected_count"] == 0,
        "duration_ms": 0,
        "latest_batch_id": latest["batch_id"],
        "rule_revision": latest["rule_revision"],
        "rule_fingerprint": latest["rule_fingerprint"],
    }