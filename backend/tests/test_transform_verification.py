"""转换验证结果领域测试：覆盖逐行类型/必填/主键校验、规则版本过期防护、
可交付分区消费、交付登记与拒收留档、修正样本后的批次对比。

直接调用服务层函数，使用独立 Store 与验证仓库，不依赖 HTTP 服务。
"""
from __future__ import annotations

import pytest

from app.services import etl_mapping as etl_module
from app.services.transform_verification import (
    CHECK_REQUIRED_FIELD,
    CHECK_TARGET_PK,
    CHECK_TYPE_CONVERSION,
    ROW_PASSED,
    ROW_REJECTED,
    compare_batches,
    deliver_partition,
    get_verification_rows,
    list_deliverable_partitions,
    list_verification_batches,
    rule_fingerprint,
    submit_verification_batch,
    verification_store,
)
from app.store import Store


@pytest.fixture
def store() -> Store:
    """每个测试清空转换验证仓库；模块表使用全局内存 store。"""
    verification_store.reset()
    from app.store import store as global_store
    return global_store


def _mapping_payload() -> dict:
    return {
        "规则名称": "测试销售映射",
        "源表": "pos_sales_raw",
        "目标表": "dwd_sales_detail",
        "字段映射": "order_id=>order_id.amt=>sales_amt",
        "清洗逻辑": "过滤NULL金额记录",
        "转换函数": "COALESCE",
        "写入模式": "INSERT OVERWRITE PARTITION",
    }


def _batch_payload(samples: list[dict]) -> dict:
    return {
        "partition_field": "dt",
        "field_types": {"order_id": "VARCHAR(32)", "sales_amt": "DECIMAL(18,2)"},
        "required_fields": ["order_id", "sales_amt"],
        "target_pk_field": "order_id",
        "valid_target_pks": ["ORD-001", "ORD-002", "ORD-003"],
        "samples": samples,
    }


@pytest.fixture
def mapping_id(store: Store) -> int:
    service = etl_module.EtlMappingService()
    entry, missing = service.create_entry(_mapping_payload())
    assert not missing
    return int(entry["id"])


# ======================================================================
# Section A: 逐行三类规则校验（数字/空值/文本混合样本）
# ======================================================================


class TestRowLevelValidation:
    """逐行类型转换、必填字段、目标主键校验。"""

    def test_mixed_samples_partial_pass(self, store: Store, mapping_id: int) -> None:
        """数字通过、空值触发必填、文本触发类型、无效主键触发主键校验。"""
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S1", "order_id": "ORD-001", "amt": 100, "dt": "p1"},
            {"_source_ref": "S2", "order_id": "ORD-002", "amt": None, "dt": "p1"},
            {"_source_ref": "S3", "order_id": "ORD-003", "amt": "abc文本", "dt": "p1"},
            {"_source_ref": "S4", "order_id": "ORD-999", "amt": 88, "dt": "p1"},
        ]))
        assert batch["status"] == "部分通过"
        assert batch["passed_count"] == 1
        assert batch["rejected_count"] == 3
        assert set(batch["checked_rules"]) == {
            CHECK_TYPE_CONVERSION, CHECK_REQUIRED_FIELD, CHECK_TARGET_PK,
        }

    def test_null_value_rejected_by_required_rule(self, store: Store, mapping_id: int) -> None:
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S2", "order_id": "ORD-002", "amt": None, "dt": "p1"},
        ]))
        row = get_verification_rows(batch["batch_id"])[0]
        assert row["status"] == ROW_REJECTED
        reasons = row["failure_reasons"]
        assert any("必填" in r and "sales_amt" in r for r in reasons)

    def test_text_into_decimal_rejected_by_type_rule(self, store: Store, mapping_id: int) -> None:
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S3", "order_id": "ORD-003", "amt": "abc文本", "dt": "p1"},
        ]))
        row = get_verification_rows(batch["batch_id"])[0]
        type_failures = [f for f in row["failures"] if f["rule"] == CHECK_TYPE_CONVERSION]
        assert len(type_failures) == 1
        assert "DECIMAL" in type_failures[0]["reason"]

    def test_invalid_target_pk_is_never_success(self, store: Store, mapping_id: int) -> None:
        """无效目标主键的记录即使类型正确也必须拒收，不能计入成功行。"""
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S4", "order_id": "ORD-999", "amt": 88, "dt": "p1"},
        ]))
        assert batch["passed_count"] == 0
        assert batch["rejected_count"] == 1
        row = get_verification_rows(batch["batch_id"])[0]
        pk_failures = [f for f in row["failures"] if f["rule"] == CHECK_TARGET_PK]
        assert pk_failures and "ORD-999" in pk_failures[0]["reason"]

    def test_missing_pk_rejected(self, store: Store, mapping_id: int) -> None:
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S5", "order_id": "", "amt": 10, "dt": "p1"},
        ]))
        row = get_verification_rows(batch["batch_id"])[0]
        assert row["status"] == ROW_REJECTED
        assert any(f["rule"] == CHECK_TARGET_PK for f in row["failures"])

    def test_all_pass_batch_status(self, store: Store, mapping_id: int) -> None:
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S1", "order_id": "ORD-001", "amt": 100, "dt": "p1"},
            {"_source_ref": "S2", "order_id": "ORD-002", "amt": 0, "dt": "p2"},
        ]))
        assert batch["status"] == "全部通过"
        assert batch["rejected_count"] == 0
        assert {p["partition_value"]: p["passed"] for p in batch["partitions"]} == {"p1": 1, "p2": 1}

    def test_all_rejected_batch_status(self, store: Store, mapping_id: int) -> None:
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S9", "order_id": "ORD-X", "amt": None, "dt": "p1"},
        ]))
        assert batch["status"] == "全部拒收"
        assert batch["passed_count"] == 0


# ======================================================================
# Section B: 检查规则与样本范围留痕，规则版本过期防护
# ======================================================================


class TestRuleVersioning:
    """规则修订版本/指纹与样本范围记录，防止旧规则结果被新规则交付。"""

    def test_batch_records_scope_and_rule_revision(self, store: Store, mapping_id: int) -> None:
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S1", "order_id": "ORD-001", "amt": 1, "dt": "p1"},
        ]))
        assert batch["rule_revision"] == 1
        assert len(batch["rule_fingerprint"]) == 16
        assert batch["sample_scope"]["sample_count"] == 1
        assert batch["sample_scope"]["source_refs"] == ["S1"]
        assert batch["sample_scope"]["partitions"] == ["p1"]
        assert batch["rule_stale"] is False

    def test_rule_change_marks_old_batch_stale(self, store: Store, mapping_id: int) -> None:
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S1", "order_id": "ORD-001", "amt": 1, "dt": "p1"},
        ]))
        service = etl_module.EtlMappingService()
        entry, message = service.update_mapping(mapping_id, {"转换函数": "COALESCE.UPPER"})
        assert entry["_rule_revision"] == 2
        assert entry["_rule_fingerprint"] != batch["rule_fingerprint"]

        refreshed = list_verification_batches(mapping_id)[0]
        assert refreshed["rule_stale"] is True

    def test_stale_batch_cannot_be_delivered(self, store: Store, mapping_id: int) -> None:
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S1", "order_id": "ORD-001", "amt": 1, "dt": "p1"},
        ]))
        etl_module.EtlMappingService().update_mapping(mapping_id, {"清洗逻辑": "新增去重"})
        with pytest.raises(ValueError, match="映射规则已变更"):
            deliver_partition(batch["batch_id"], "p1")

    def test_deliverable_partitions_exclude_stale(self, store: Store, mapping_id: int) -> None:
        submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S1", "order_id": "ORD-001", "amt": 1, "dt": "p1"},
        ]))
        assert len(list_deliverable_partitions("dwd_sales_detail")) == 1
        etl_module.EtlMappingService().update_mapping(mapping_id, {"转换函数": "UPPER"})
        items = list_deliverable_partitions("dwd_sales_detail")
        assert all(item["deliverable"] is False for item in items)

    def test_fingerprint_covers_material_fields(self) -> None:
        base = _mapping_payload()
        fp1 = rule_fingerprint(base)
        changed = dict(base, 字段映射="order_id=>order_id.amt=>amount")
        assert rule_fingerprint(changed) != fp1


# ======================================================================
# Section C: 可交付分区只含通过输出 + 交付登记发布/拒收范围
# ======================================================================


class TestPartitionDelivery:
    """部署流水线只消费实际通过验证的输出。"""

    def test_deliverable_only_contains_passed_partitions(
        self, store: Store, mapping_id: int,
    ) -> None:
        submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S1", "order_id": "ORD-001", "amt": 1, "dt": "p1"},
            {"_source_ref": "S2", "order_id": "ORD-002", "amt": None, "dt": "p1"},
            {"_source_ref": "S3", "order_id": "ORD-003", "amt": 2, "dt": "p2"},
        ]))
        items = list_deliverable_partitions("dwd_sales_detail")
        by_partition = {i["partition_value"]: i for i in items}
        # 两个分区都有通过行，但 p1 同时登记了拒收行
        assert by_partition["p1"]["passed_count"] == 1
        assert by_partition["p1"]["rejected_source_refs"] == ["S2"]
        assert by_partition["p2"]["passed_count"] == 1

    def test_delivery_records_published_and_rejected_scope(
        self, store: Store, mapping_id: int,
    ) -> None:
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S1", "order_id": "ORD-001", "amt": 1, "dt": "p1"},
            {"_source_ref": "S2", "order_id": "ORD-999", "amt": 2, "dt": "p1"},
        ]))
        delivery = deliver_partition(batch["batch_id"], "p1", {"environment": "预发"})
        assert delivery["published_scope"] == {"row_count": 1, "source_refs": ["S1"]}
        assert delivery["rejected_scope"]["source_refs"] == ["S2"]
        assert delivery["rule_revision"] == batch["rule_revision"]
        assert delivery["rule_fingerprint"] == batch["rule_fingerprint"]

    def test_partition_without_passed_row_not_deliverable(
        self, store: Store, mapping_id: int,
    ) -> None:
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S9", "order_id": "ORD-BAD", "amt": None, "dt": "pbad"},
        ]))
        with pytest.raises(ValueError, match="无通过验证的记录"):
            deliver_partition(batch["batch_id"], "pbad")

    def test_rollback_releases_batch(self, store: Store, mapping_id: int) -> None:
        from app.services.transform_verification import list_deliveries, rollback_delivery
        batch = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S1", "order_id": "ORD-001", "amt": 1, "dt": "p1"},
        ]))
        delivery = deliver_partition(batch["batch_id"], "p1")
        rolled = rollback_delivery(delivery["delivery_id"], "重测")
        assert rolled["status"] == "已回滚"
        refreshed = list_verification_batches(mapping_id)[0]
        assert refreshed["delivered"] is False
        assert list_deliveries()[0]["status"] == "已回滚"


# ======================================================================
# Section D: 修正样本后重新提交，批次逐行对比
# ======================================================================


class TestBatchComparison:
    """修正样本后重提，比较多条记录在交付前后的实际结果。"""

    def test_compare_tracks_source_ref_progress(self, store: Store, mapping_id: int) -> None:
        first = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S1", "order_id": "ORD-001", "amt": 1, "dt": "p1"},
            {"_source_ref": "S2", "order_id": "ORD-002", "amt": None, "dt": "p1"},
        ]))
        second = submit_verification_batch(mapping_id, _batch_payload([
            {"_source_ref": "S1", "order_id": "ORD-001", "amt": 1, "dt": "p1"},
            {"_source_ref": "S2", "order_id": "ORD-002", "amt": 0, "dt": "p1"},
        ]))
        result = compare_batches([first["batch_id"], second["batch_id"]])
        progress = {item["source_ref"]: item["status_by_batch"] for item in result["row_progress"]}
        assert progress["S1"][first["batch_id"]] == ROW_PASSED
        assert progress["S2"][first["batch_id"]] == ROW_REJECTED
        assert progress["S2"][second["batch_id"]] == ROW_PASSED
        assert result["batches"][0]["rejected_count"] == 1
        assert result["batches"][1]["passed_count"] == 2

    def test_compare_unknown_batch_raises(self, store: Store) -> None:
        with pytest.raises(KeyError):
            compare_batches(["TVB-NOPE-000"])


# ======================================================================
# Section E: 输入校验
# ======================================================================


class TestPayloadValidation:
    def test_empty_samples_rejected(self, store: Store, mapping_id: int) -> None:
        with pytest.raises(ValueError, match="samples 不能为空"):
            submit_verification_batch(mapping_id, {"samples": []})

    def test_unknown_mapping_rejected(self, store: Store) -> None:
        with pytest.raises(KeyError):
            submit_verification_batch(999, _batch_payload([
                {"_source_ref": "S1", "order_id": "ORD-001", "amt": 1, "dt": "p1"},
            ]))

    def test_mapping_without_field_mapping_rejected(self, store: Store) -> None:
        service = etl_module.EtlMappingService()
        payload = _mapping_payload()
        payload["字段映射"] = "没有映射符号"
        entry, missing = service.create_entry(payload)
        with pytest.raises(ValueError, match="字段映射"):
            submit_verification_batch(entry["id"], _batch_payload([
                {"_source_ref": "S1", "order_id": "ORD-001", "amt": 1, "dt": "p1"},
            ]))
