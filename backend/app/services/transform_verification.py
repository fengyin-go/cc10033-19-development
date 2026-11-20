"""转换验证结果服务：ETL映射样本分区的逐行转换验证与分区交付。

业务链路：
  ETL映射（规则版本） -> 转换验证（样本逐行校验） -> 部署流水线（按分区消费已验证输出）

设计要点：
- 每条样本逐行执行三类规则：类型转换、必填字段、目标主键；
  无效目标主键的记录一律计为拒收，不能进入成功行。
- 验证批次记录使用的映射规则修订版本与规则指纹、样本数据范围，
  映射规则后续被修改后，历史批次会被标记为规则已过期，避免
  “测试通过的是旧映射、交付使用的却是新规则”。
- 只有全部通过的行才形成可交付分区输出；部署流水线通过样本验证
  接口消费时登记实际参与发布的数据范围与被拒收的行，拒收行保留
  源记录定位信息，便于修正后重新提交比对。
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

MODULE = "transform_verification"

# 校验规则编码，逐行结果与批次摘要都使用同一套编码，方便交付侧核对检查范围。
CHECK_TYPE_CONVERSION = "TYPE_CONVERSION"
CHECK_REQUIRED_FIELD = "REQUIRED_FIELD"
CHECK_TARGET_PK = "TARGET_PK"
CHECK_RULES = [CHECK_TYPE_CONVERSION, CHECK_REQUIRED_FIELD, CHECK_TARGET_PK]

CHECK_LABELS = {
    CHECK_TYPE_CONVERSION: "类型转换",
    CHECK_REQUIRED_FIELD: "必填字段",
    CHECK_TARGET_PK: "目标主键",
}

ROW_PASSED = "通过"
ROW_REJECTED = "拒收"

BATCH_PASSED = "全部通过"
BATCH_PARTIAL = "部分通过"
BATCH_FAILED = "全部拒收"

DELIVERY_DELIVERED = "已交付"
DELIVERY_ROLLED_BACK = "已回滚"

# 数值/日期类型的判定口径，交付侧与重测侧共用同一份口径。
NUMERIC_TYPES = {"INT", "BIGINT", "DECIMAL", "DOUBLE", "FLOAT", "NUMBER", "LONG", "SMALLINT", "TINYINT"}
DATE_TYPES = {"DATE", "DATETIME", "TIMESTAMP"}
BOOL_TYPES = {"BOOLEAN", "BOOL"}
TEXT_TYPES = {"VARCHAR", "STRING", "CHAR", "TEXT"}
DATE_PATTERNS = (
    re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$"),
    re.compile(r"^\d{8}$"),
)
BOOL_VALUES = {"TRUE", "FALSE", "1", "0", "Y", "N", "是", "否", "真", "假"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def rule_fingerprint(mapping: dict[str, Any]) -> str:
    """根据映射规则中参与转换的字段计算规则指纹。

    字段映射、清洗逻辑、转换函数、写入模式任一变化都会改变指纹，
    从而让基于旧规则产生的验证结果可被识别为过期。
    """
    material = "|".join(
        str(mapping.get(field, "") or "").strip()
        for field in ("字段映射", "清洗逻辑", "转换函数", "写入模式")
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def parse_field_mapping(field_mapping: str) -> list[tuple[str, str]]:
    """解析 `src=>tgt.src2=>tgt2` 形式的字段映射。"""
    pairs: list[tuple[str, str]] = []
    for chunk in str(field_mapping or "").split("."):
        chunk = chunk.strip()
        if "=>" in chunk:
            src, tgt = (part.strip() for part in chunk.split("=>", 1))
            if src and tgt:
                pairs.append((src, tgt))
    return pairs


def is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _check_type(value: Any, declared_type: str) -> bool:
    """按目标字段声明类型判断原始样本值能否完成类型转换。"""
    upper = (declared_type or "").upper()
    base = upper.split("(", 1)[0].strip()

    if base in NUMERIC_TYPES:
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            return True
        try:
            float(str(value).replace(",", "").strip())
            return True
        except (ValueError, TypeError):
            return False
    if base in DATE_TYPES:
        text = str(value).strip()
        return any(pattern.match(text) for pattern in DATE_PATTERNS)
    if base in BOOL_TYPES:
        return str(value).strip().upper() in BOOL_VALUES
    if base in TEXT_TYPES or not base:
        # 文本类型及未声明类型不拦截，空值由必填规则负责。
        return True
    # 未识别的类型按文本处理，避免误拒收。
    return True


class TransformVerificationService(BaseModuleService):
    """转换验证结果模块：承载批次结果的登记/查询骨架，领域方法在下方。"""

    MODULE = MODULE
    ENTITY = "转换验证结果"
    OPERATION_TERM = "验证"
    LIST_FIELDS = ["批次编号", "映射规则", "目标表", "样本范围", "通过行数", "拒收行数", "验证状态"]
    REQUIRED_FIELDS = ["批次编号", "映射规则", "目标表"]
    STATUS_ORDER = ["待验证", "部分通过", "全部通过", "全部拒收", "已交付", "已过期"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS = []

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """通用资源接口：把验证批次投影为模块表行，支持按目标表/状态检索。"""
        projected = []
        for seq, batch in enumerate(list_verification_batches(), start=1):
            scope = batch["sample_scope"]
            row_status = "已过期" if batch["rule_stale"] else (
                "已交付" if batch["delivered"] else batch["status"]
            )
            projected.append({
                "id": seq,
                "批次编号": batch["batch_id"],
                "映射规则": batch["rule_name"],
                "目标表": batch["target_table"],
                "样本范围": (
                    f"{scope['sample_count']}行/分区{','.join(scope['partitions'])}"
                ),
                "通过行数": batch["passed_count"],
                "拒收行数": batch["rejected_count"],
                "验证状态": batch["status"],
                "status": row_status,
                "pending": not batch["delivered"],
                "abnormal": batch["rejected_count"] > 0 or batch["rule_stale"],
            })
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                projected = [
                    row for row in projected
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword:
            projected = [
                row for row in projected
                if keyword.casefold() in str(row.get("批次编号", "")).casefold()
            ]
        if status:
            projected = [row for row in projected if row["status"] == status]
        total = len(projected)
        start = (page - 1) * size
        return projected[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        items, _ = self.list_entries(page=1, size=1_000_000)
        return next((row for row in items if row["id"] == entry_id), None)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["verification_batches"] = len(verification_store.batches)
        base["partition_deliveries"] = len(verification_store.deliveries)
        base["stale_batches"] = sum(1 for b in verification_store.batches.values() if _is_batch_stale(b))
        return base


# ---------------------------------------------------------------------------
# 转换验证状态仓库（进程内），与 store 的模块表并列，由本服务独占访问
# ---------------------------------------------------------------------------


class _VerificationStore:
    def __init__(self) -> None:
        self.batches: dict[str, dict[str, Any]] = {}
        self.rows: dict[str, list[dict[str, Any]]] = {}
        self.deliveries: dict[str, dict[str, Any]] = {}

    def reset(self) -> None:
        self.batches.clear()
        self.rows.clear()
        self.deliveries.clear()


verification_store = _VerificationStore()


# ---------------------------------------------------------------------------
# 映射规则版本：规则每次被登记/修改都递增修订号并刷新指纹
# ---------------------------------------------------------------------------


def mapping_revision(mapping_id: int) -> dict[str, Any]:
    entry = store.find("etl_mapping", mapping_id)
    if entry is None:
        return {"exists": False}
    revision = int(entry.get("_rule_revision", 1))
    fingerprint = str(entry.get("_rule_fingerprint") or rule_fingerprint(entry))
    return {
        "exists": True,
        "mapping_id": mapping_id,
        "rule_name": entry.get("规则名称"),
        "source_table": entry.get("源表"),
        "target_table": entry.get("目标表"),
        "revision": revision,
        "fingerprint": fingerprint,
        "status": entry.get("status"),
        "field_mapping": entry.get("字段映射", ""),
        "cleaning_logic": entry.get("清洗逻辑", ""),
        "transform_functions": entry.get("转换函数", ""),
        "write_mode": entry.get("写入模式", ""),
    }


def touch_mapping_revision(entry: dict[str, Any], *, is_new: bool = False) -> None:
    """登记或修改映射规则时调用：修订号 +1 并重算指纹。

    新建规则首版为修订 1；种子数据没有 _rule_revision 字段时，
    首次更新也以当前有效修订号 1 为基准递增到 2。
    """
    baseline = 0 if is_new else int(entry.get("_rule_revision", 1))
    entry["_rule_revision"] = baseline + 1
    entry["_rule_fingerprint"] = rule_fingerprint(entry)
    entry["_rule_updated_at"] = _now()


def _is_batch_stale(batch: dict[str, Any]) -> bool:
    info = mapping_revision(batch["mapping_id"])
    if not info.get("exists"):
        return True
    return (
        info["revision"] != batch["rule_revision"]
        or info["fingerprint"] != batch["rule_fingerprint"]
    )


def _refresh_stale_flag(batch: dict[str, Any]) -> bool:
    stale = _is_batch_stale(batch)
    batch["rule_stale"] = stale
    return stale


# ---------------------------------------------------------------------------
# 目标主键注册表：样本提交可显式给出有效主键，否则按目标表类型推导
# ---------------------------------------------------------------------------


def _default_pk_field(target_table: str, pairs: list[tuple[str, str]]) -> str:
    if pairs:
        return pairs[0][1]
    return "id"


def _registry_pk_values(target_table: str) -> set[str]:
    """从已建模的维度/事实数据推导目标表当前可接受的主键值。"""
    values: set[str] = set()
    if target_table.startswith("dim_"):
        for dim in store.rows("dim_definition"):
            code = str(dim.get("维度编码", "")).strip()
            if code:
                values.add(code)
    else:
        # 事实表主键以已登记事实/历史验证通过的业务键为准。
        for fact in store.rows("fact_design"):
            for key_field in ("事实名称",):
                code = str(fact.get(key_field, "")).strip()
                if code:
                    values.add(code)
    return values


def _build_pk_registry(payload: dict[str, Any], target_table: str) -> tuple[set[str], str]:
    explicit = payload.get("valid_target_pks")
    if isinstance(explicit, list) and explicit:
        return {str(v).strip() for v in explicit if str(v).strip() != ""}, "explicit"
    derived = _registry_pk_values(target_table)
    return derived, "registry" if derived else "open"


# ---------------------------------------------------------------------------
# 逐行校验
# ---------------------------------------------------------------------------


def _validate_row(
    index: int,
    sample: dict[str, Any],
    pairs: list[tuple[str, str]],
    field_types: dict[str, str],
    required_targets: set[str],
    pk_field: str,
    pk_registry: set[str],
    registry_mode: str,
) -> dict[str, Any]:
    source_ref = str(
        sample.get("_source_ref")
        or sample.get("source_ref")
        or sample.get("源记录标识")
        or f"row-{index}"
    )
    checks: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    transformed: dict[str, Any] = {}

    for src, tgt in pairs:
        raw = sample.get(src)
        transformed[tgt] = raw

        # 规则1：必填字段（空字符串、None、纯空白都视为空）
        required_passed = not (tgt in required_targets and is_blank(raw))
        checks.append({
            "rule": CHECK_REQUIRED_FIELD,
            "rule_label": CHECK_LABELS[CHECK_REQUIRED_FIELD],
            "field": tgt,
            "passed": required_passed,
        })
        if not required_passed:
            failures.append({
                "rule": CHECK_REQUIRED_FIELD,
                "field": tgt,
                "value": raw,
                "reason": f"必填目标字段 {tgt} 为空值",
            })

        # 规则2：类型转换（空值留给必填规则，不重复报类型错误）
        declared = field_types.get(tgt) or field_types.get(src) or ""
        type_passed = True
        if declared and not is_blank(raw):
            type_passed = _check_type(raw, declared)
        checks.append({
            "rule": CHECK_TYPE_CONVERSION,
            "rule_label": CHECK_LABELS[CHECK_TYPE_CONVERSION],
            "field": tgt,
            "declared_type": declared,
            "passed": type_passed,
        })
        if not type_passed:
            failures.append({
                "rule": CHECK_TYPE_CONVERSION,
                "field": tgt,
                "value": raw,
                "reason": f"值 {raw!r} 无法转换为目标类型 {declared}",
            })

    # 规则3：目标主键。未在映射中的主键字段直接从样本取同名字段。
    pk_value = transformed.get(pk_field)
    if pk_value is None:
        pk_value = sample.get(pk_field)
    pk_missing = is_blank(pk_value)
    if pk_missing:
        pk_passed = False
        pk_reason = f"目标主键 {pk_field} 缺失"
    elif registry_mode == "open":
        # 没有可参照的注册表时，非空即视为格式有效（仍记录校验口径）。
        pk_passed = True
        pk_reason = ""
    else:
        pk_passed = str(pk_value).strip() in pk_registry
        pk_reason = "" if pk_passed else f"无效目标主键：{pk_field}={pk_value} 不在目标主键注册表"
    checks.append({
        "rule": CHECK_TARGET_PK,
        "rule_label": CHECK_LABELS[CHECK_TARGET_PK],
        "field": pk_field,
        "passed": pk_passed,
        "registry_mode": registry_mode,
    })
    if not pk_passed:
        failures.append({
            "rule": CHECK_TARGET_PK,
            "field": pk_field,
            "value": pk_value,
            "reason": pk_reason,
        })

    rejected = bool(failures)
    return {
        "row_no": index,
        "source_ref": source_ref,
        "status": ROW_REJECTED if rejected else ROW_PASSED,
        "transformed": transformed,
        "raw_sample": dict(sample),
        "checks": checks,
        "checked_rules": [rule for rule in CHECK_RULES if any(c["rule"] == rule for c in checks)],
        "failures": failures,
        "failure_reasons": [f"{f['rule_label'] if 'rule_label' in f else CHECK_LABELS.get(f['rule'], f['rule'])}：{f['reason']}" for f in failures],
        "partition_value": None,  # 由批次按分区字段回填
    }


# ---------------------------------------------------------------------------
# 批次提交与查询
# ---------------------------------------------------------------------------


def submit_verification_batch(mapping_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    """提交一批样本并执行逐行转换验证，返回验证批次结果。"""
    entry = store.find("etl_mapping", mapping_id)
    if entry is None:
        raise KeyError(f"映射规则 {mapping_id} 不存在")

    samples = payload.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ValueError("samples 不能为空，至少提交一条样本")

    pairs = parse_field_mapping(str(entry.get("字段映射", "")))
    if not pairs:
        raise ValueError("映射规则缺少可解析的字段映射（src=>tgt）")

    target_table = str(entry.get("目标表", "")).strip()
    partition_field = str(payload.get("partition_field") or "dt").strip()
    field_types = {str(k).strip(): str(v).strip() for k, v in (payload.get("field_types") or {}).items()}
    required_fields = payload.get("required_fields")
    if isinstance(required_fields, list) and required_fields:
        required_targets = {str(v).strip() for v in required_fields}
    else:
        # 默认口径：第一个映射目标字段即业务主键，必填。
        required_targets = {pairs[0][1]}
    pk_field = str(payload.get("target_pk_field") or _default_pk_field(target_table, pairs)).strip()
    pk_registry, registry_mode = _build_pk_registry(payload, target_table)

    revision_info = mapping_revision(mapping_id)
    batch_id = f"TVB-{mapping_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{len(verification_store.batches) + 1:03d}"

    rows: list[dict[str, Any]] = []
    partitions: dict[str, dict[str, Any]] = {}
    for index, sample in enumerate(samples, start=1):
        if not isinstance(sample, dict):
            raise ValueError(f"第 {index} 条样本不是对象")
        row = _validate_row(
            index, sample, pairs, field_types, required_targets,
            pk_field, pk_registry, registry_mode,
        )
        partition_value = str(sample.get(partition_field, "default"))
        row["partition_value"] = partition_value
        rows.append(row)

        slot = partitions.setdefault(partition_value, {
            "partition_value": partition_value,
            "total": 0, "passed": 0, "rejected": 0,
            "passed_source_refs": [], "rejected_source_refs": [],
        })
        slot["total"] += 1
        if row["status"] == ROW_PASSED:
            slot["passed"] += 1
            slot["passed_source_refs"].append(row["source_ref"])
        else:
            slot["rejected"] += 1
            slot["rejected_source_refs"].append(row["source_ref"])

    passed_rows = [r for r in rows if r["status"] == ROW_PASSED]
    rejected_rows = [r for r in rows if r["status"] == ROW_REJECTED]
    if not rejected_rows:
        batch_status = BATCH_PASSED
    elif not passed_rows:
        batch_status = BATCH_FAILED
    else:
        batch_status = BATCH_PARTIAL

    sample_refs = [r["source_ref"] for r in rows]
    batch = {
        "batch_id": batch_id,
        "mapping_id": mapping_id,
        "rule_name": entry.get("规则名称"),
        "source_table": entry.get("源表"),
        "target_table": target_table,
        "rule_revision": revision_info["revision"],
        "rule_fingerprint": revision_info["fingerprint"],
        "rule_stale": False,
        "checked_rules": list(CHECK_RULES),
        "check_rule_labels": [CHECK_LABELS[r] for r in CHECK_RULES],
        "sample_scope": {
            "partition_field": partition_field,
            "sample_count": len(rows),
            "source_refs": sample_refs,
            "partitions": sorted(partitions),
        },
        "field_types": field_types,
        "required_fields": sorted(required_targets),
        "target_pk_field": pk_field,
        "pk_registry_mode": registry_mode,
        "pk_registry_size": len(pk_registry),
        "status": batch_status,
        "total_rows": len(rows),
        "passed_count": len(passed_rows),
        "rejected_count": len(rejected_rows),
        "partitions": list(partitions.values()),
        "rejected_source_refs": [r["source_ref"] for r in rejected_rows],
        "created_at": _now(),
        "delivered": False,
        "delivery_id": None,
    }
    verification_store.batches[batch_id] = batch
    verification_store.rows[batch_id] = rows

    store.artifacts.register(
        MODULE, "verification_batch", batch_id,
        {
            "mapping_id": mapping_id,
            "target_table": target_table,
            "rule_revision": batch["rule_revision"],
            "rule_fingerprint": batch["rule_fingerprint"],
            "passed": batch["passed_count"],
            "rejected": batch["rejected_count"],
        },
        source_modules=["etl_mapping", MODULE],
    )
    store.channel.publish("etl.verified", {
        "batch_id": batch_id,
        "mapping_id": mapping_id,
        "target_table": target_table,
        "status": batch_status,
        "passed": batch["passed_count"],
        "rejected": batch["rejected_count"],
        "rule_revision": batch["rule_revision"],
        "rule_fingerprint": batch["rule_fingerprint"],
        "entry_id": mapping_id,
    })
    return get_verification_batch(batch_id)


def get_verification_batch(batch_id: str) -> dict[str, Any] | None:
    batch = verification_store.batches.get(batch_id)
    if batch is None:
        return None
    _refresh_stale_flag(batch)
    # 不把逐行明细塞进批次摘要，明细走独立接口。
    return dict(batch)


def list_verification_batches(mapping_id: int | None = None) -> list[dict[str, Any]]:
    batches = []
    for batch in verification_store.batches.values():
        _refresh_stale_flag(batch)
        if mapping_id is not None and batch["mapping_id"] != mapping_id:
            continue
        batches.append(dict(batch))
    batches.sort(key=lambda b: b["created_at"], reverse=True)
    return batches


def get_verification_rows(batch_id: str, status: str | None = None) -> list[dict[str, Any]]:
    rows = verification_store.rows.get(batch_id)
    if rows is None:
        raise KeyError(f"验证批次 {batch_id} 不存在")
    if status:
        return [dict(r) for r in rows if r["status"] == status]
    return [dict(r) for r in rows]


def get_row_detail(batch_id: str, row_no: int) -> dict[str, Any] | None:
    for row in verification_store.rows.get(batch_id, []):
        if row["row_no"] == row_no:
            return dict(row)
    return None


# ---------------------------------------------------------------------------
# 可交付分区：只暴露实际通过验证的输出，且能查回映射规则
# ---------------------------------------------------------------------------


def list_deliverable_partitions(target_table: str | None = None) -> list[dict[str, Any]]:
    """部署流水线消费的样本验证接口：返回可交付分区及验证依据。"""
    result = []
    for batch in verification_store.batches.values():
        stale = _refresh_stale_flag(batch)
        if target_table and batch["target_table"] != target_table:
            continue
        for slot in batch["partitions"]:
            if slot["passed"] <= 0:
                continue
            result.append({
                "batch_id": batch["batch_id"],
                "target_table": batch["target_table"],
                "mapping_id": batch["mapping_id"],
                "rule_name": batch["rule_name"],
                "rule_revision": batch["rule_revision"],
                "rule_fingerprint": batch["rule_fingerprint"],
                "rule_stale": stale,
                "deliverable": not stale,
                "partition_value": slot["partition_value"],
                "passed_count": slot["passed"],
                "rejected_count": slot["rejected"],
                "passed_source_refs": list(slot["passed_source_refs"]),
                "rejected_source_refs": list(slot["rejected_source_refs"]),
                "sample_scope": dict(batch["sample_scope"]),
                "checked_rules": list(batch["checked_rules"]),
                "already_delivered": batch["delivered"],
            })
    result.sort(key=lambda item: (item["target_table"], item["partition_value"], item["batch_id"]))
    return result


def consume_partition_output(batch_id: str, partition_value: str) -> dict[str, Any]:
    """取回某分区实际通过验证的逐行输出（供部署流水线拉取数据）。"""
    batch = verification_store.batches.get(batch_id)
    if batch is None:
        raise KeyError(f"验证批次 {batch_id} 不存在")
    rows = verification_store.rows.get(batch_id, [])
    outputs = [
        {
            "row_no": r["row_no"],
            "source_ref": r["source_ref"],
            "target_record": r["transformed"],
            "partition_value": r["partition_value"],
        }
        for r in rows
        if r["status"] == ROW_PASSED and r["partition_value"] == partition_value
    ]
    if not outputs:
        raise KeyError(f"批次 {batch_id} 分区 {partition_value} 没有通过验证的输出")
    return {
        "batch_id": batch_id,
        "target_table": batch["target_table"],
        "partition_value": partition_value,
        "mapping_id": batch["mapping_id"],
        "rule_revision": batch["rule_revision"],
        "rule_fingerprint": batch["rule_fingerprint"],
        "outputs": outputs,
    }


# ---------------------------------------------------------------------------
# 分区交付登记：部署流水线确认哪个目标分区可交付
# ---------------------------------------------------------------------------


def deliver_partition(
    batch_id: str,
    partition_value: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = payload or {}
    batch = verification_store.batches.get(batch_id)
    if batch is None:
        raise KeyError(f"验证批次 {batch_id} 不存在")
    if _refresh_stale_flag(batch):
        current = mapping_revision(batch["mapping_id"])
        raise ValueError(
            f"批次 {batch_id} 依据的映射规则已变更"
            f"（修订 {batch['rule_revision']} -> {current['revision']}，"
            f"指纹 {batch['rule_fingerprint']} -> {current['fingerprint']}），"
            "请基于新规则重新验证后再交付"
        )

    target_slot = next((s for s in batch["partitions"] if s["partition_value"] == partition_value), None)
    if target_slot is None:
        raise KeyError(f"批次 {batch_id} 中不存在分区 {partition_value}")
    if target_slot["passed"] <= 0:
        raise ValueError(f"分区 {partition_value} 无通过验证的记录，不可交付")

    mapping_info = mapping_revision(batch["mapping_id"])
    delivery_id = f"DLV-{len(verification_store.deliveries) + 1:04d}"
    output = consume_partition_output(batch_id, partition_value)
    delivery = {
        "delivery_id": delivery_id,
        "batch_id": batch_id,
        "mapping_id": batch["mapping_id"],
        "rule_name": batch["rule_name"],
        "rule_revision": batch["rule_revision"],
        "rule_fingerprint": batch["rule_fingerprint"],
        "target_table": batch["target_table"],
        "partition_value": partition_value,
        "environment": str(payload.get("environment") or "预发"),
        "operator": str(payload.get("operator") or "deploy_pipeline"),
        # 实际参与发布的数据范围：只含通过行；拒收行单独登记用于回溯源记录。
        "published_scope": {
            "row_count": len(output["outputs"]),
            "source_refs": [o["source_ref"] for o in output["outputs"]],
        },
        "rejected_scope": {
            "row_count": target_slot["rejected"],
            "source_refs": list(target_slot["rejected_source_refs"]),
        },
        "checked_rules": list(batch["checked_rules"]),
        "status": DELIVERY_DELIVERED,
        "delivered_at": _now(),
        "rolled_back_at": None,
        "deploy_task_id": payload.get("deploy_task_id"),
    }
    verification_store.deliveries[delivery_id] = delivery
    batch["delivered"] = True
    batch["delivery_id"] = delivery_id
    if batch["status"] not in (BATCH_PASSED,):
        # 部分通过的批次交付后仍保留原验证状态，只是其中合格分区已发布。
        pass
    batch["delivery_partition"] = partition_value

    store.channel.publish("etl.partition_delivered", {
        "delivery_id": delivery_id,
        "batch_id": batch_id,
        "mapping_id": batch["mapping_id"],
        "target_table": batch["target_table"],
        "partition_value": partition_value,
        "published_refs": delivery["published_scope"]["source_refs"],
        "rejected_refs": delivery["rejected_scope"]["source_refs"],
        "rule_revision": batch["rule_revision"],
        "entry_id": batch["mapping_id"],
    })
    store.artifacts.mark_consumed(
        f"{MODULE}-verification_batch-{batch_id}", "deploy_pipeline",
    )
    return dict(delivery)


def list_deliveries(target_table: str | None = None) -> list[dict[str, Any]]:
    deliveries = [dict(d) for d in verification_store.deliveries.values()]
    if target_table:
        deliveries = [d for d in deliveries if d["target_table"] == target_table]
    deliveries.sort(key=lambda d: d["delivered_at"], reverse=True)
    return deliveries


def rollback_delivery(delivery_id: str, reason: str = "") -> dict[str, Any]:
    """回滚分区交付，释放批次占用并登记回滚原因，便于重测后重新交付。"""
    delivery = verification_store.deliveries.get(delivery_id)
    if delivery is None:
        raise KeyError(f"交付记录 {delivery_id} 不存在")
    delivery["status"] = DELIVERY_ROLLED_BACK
    delivery["rolled_back_at"] = _now()
    delivery["rollback_reason"] = reason or "部署流水线手动回滚"

    batch = verification_store.batches.get(delivery["batch_id"])
    if batch and batch.get("delivery_id") == delivery_id:
        batch["delivered"] = False
        batch["delivery_id"] = None
        batch.pop("delivery_partition", None)

    store.channel.publish("etl.delivery_rolled_back", {
        "delivery_id": delivery_id,
        "batch_id": delivery["batch_id"],
        "target_table": delivery["target_table"],
        "partition_value": delivery["partition_value"],
        "reason": delivery["rollback_reason"],
        "entry_id": delivery["mapping_id"],
    })
    return dict(delivery)


def compare_batches(batch_ids: list[str]) -> dict[str, Any]:
    """比较多个验证批次（修正样本前后重提）的逐行实际结果。"""
    comparisons = []
    for batch_id in batch_ids:
        batch = verification_store.batches.get(batch_id)
        if batch is None:
            raise KeyError(f"验证批次 {batch_id} 不存在")
        _refresh_stale_flag(batch)
        comparisons.append({
            "batch_id": batch_id,
            "created_at": batch["created_at"],
            "rule_revision": batch["rule_revision"],
            "rule_fingerprint": batch["rule_fingerprint"],
            "rule_stale": batch["rule_stale"],
            "status": batch["status"],
            "total_rows": batch["total_rows"],
            "passed_count": batch["passed_count"],
            "rejected_count": batch["rejected_count"],
            "delivered": batch["delivered"],
            "delivery_id": batch["delivery_id"],
            "rows": [
                {
                    "row_no": r["row_no"],
                    "source_ref": r["source_ref"],
                    "partition_value": r["partition_value"],
                    "status": r["status"],
                    "failure_reasons": r["failure_reasons"],
                }
                for r in verification_store.rows.get(batch_id, [])
            ],
        })

    # 以源记录标识为主键，横向比较同一源记录在不同批次的状态变化。
    ref_index: dict[str, dict[str, str]] = {}
    for item in comparisons:
        for row in item["rows"]:
            ref_index.setdefault(row["source_ref"], {})[item["batch_id"]] = row["status"]
    return {
        "batches": comparisons,
        "row_progress": [
            {"source_ref": ref, "status_by_batch": statuses}
            for ref, statuses in sorted(ref_index.items())
        ],
    }
