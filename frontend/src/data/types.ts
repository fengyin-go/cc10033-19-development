/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

// 转换验证结果相关类型，与后端 /api/etl-mappings 接口契约保持一致。

export type VerificationCheck = {
  rule: string
  rule_label: string
  field: string
  passed: boolean
  declared_type?: string
  registry_mode?: string
}

export type VerificationFailure = {
  rule: string
  field: string
  value: string | number | null
  reason: string
}

export type VerificationRow = {
  row_no: number
  source_ref: string
  status: string
  transformed: Record<string, string | number | null>
  raw_sample: Record<string, string | number | null>
  checks: VerificationCheck[]
  checked_rules: string[]
  failures: VerificationFailure[]
  failure_reasons: string[]
  partition_value: string
}

export type VerificationBatch = {
  batch_id: string
  mapping_id: number
  rule_name: string
  source_table: string
  target_table: string
  rule_revision: number
  rule_fingerprint: string
  rule_stale: boolean
  checked_rules: string[]
  check_rule_labels: string[]
  sample_scope: {
    partition_field: string
    sample_count: number
    source_refs: string[]
    partitions: string[]
  }
  field_types: Record<string, string>
  required_fields: string[]
  target_pk_field: string
  pk_registry_mode: string
  status: string
  total_rows: number
  passed_count: number
  rejected_count: number
  partitions: {
    partition_value: string
    total: number
    passed: number
    rejected: number
    passed_source_refs: string[]
    rejected_source_refs: string[]
  }[]
  rejected_source_refs: string[]
  created_at: string
  delivered: boolean
  delivery_id: string | null
}

export type DeliverablePartition = {
  batch_id: string
  target_table: string
  mapping_id: number
  rule_name: string
  rule_revision: number
  rule_fingerprint: string
  rule_stale: boolean
  deliverable: boolean
  partition_value: string
  passed_count: number
  rejected_count: number
  passed_source_refs: string[]
  rejected_source_refs: string[]
  sample_scope: VerificationBatch['sample_scope']
  checked_rules: string[]
  already_delivered: boolean
}

export type DeliveryRecord = {
  delivery_id: string
  batch_id: string
  mapping_id: number
  rule_name: string
  rule_revision: number
  rule_fingerprint: string
  target_table: string
  partition_value: string
  environment: string
  operator: string
  published_scope: { row_count: number; source_refs: string[] }
  rejected_scope: { row_count: number; source_refs: string[] }
  checked_rules: string[]
  status: string
  delivered_at: string
  rolled_back_at: string | null
  rollback_reason?: string
}
