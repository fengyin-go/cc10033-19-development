<template>
  <section class="page" data-module="transform_verification">
    <header class="page-head">
      <div>
        <h2>转换验证结果</h2>
        <p class="page-desc">
          基于 ETL 映射规则对样本分区逐行校验类型转换、必填字段与目标主键，
          只有实际通过验证的输出才能被部署流水线按分区交付。
        </p>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">验证批次</span>
        <strong class="stat-value">{{ batches.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">累计通过行</span>
        <strong class="stat-value">{{ totalPassed }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">累计拒收行</span>
        <strong class="stat-value">{{ totalRejected }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">可交付分区</span>
        <strong class="stat-value">{{ deliverable.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已登记交付</span>
        <strong class="stat-value">{{ deliveries.length }}</strong>
      </article>
    </div>

    <div class="verification-panel">
      <h3>提交样本批量验证</h3>
      <form class="analysis-form" @submit.prevent="submitBatch">
        <label class="filter-item">
          <span>ETL映射规则</span>
          <select v-model.number="form.mappingId" data-testid="select-mapping" required>
            <option v-for="m in mappings" :key="m.id" :value="Number(m.id)">
              {{ m['规则名称'] }}（{{ m['目标表'] }} / 修订{{ ruleRevOf(Number(m.id)) }}）
            </option>
          </select>
        </label>
        <label class="filter-item">
          <span>分区字段</span>
          <input v-model="form.partitionField" placeholder="dt" data-testid="input-partition-field" />
        </label>
        <label class="filter-item">
          <span>目标主键字段</span>
          <input v-model="form.pkField" placeholder="order_id" data-testid="input-pk-field" />
        </label>
        <label class="filter-item">
          <span>有效目标主键（逗号分隔）</span>
          <input v-model="form.validPks" placeholder="ORD-001,ORD-002" data-testid="input-valid-pks" />
        </label>
        <label class="filter-item">
          <span>必填目标字段（逗号分隔）</span>
          <input v-model="form.requiredFields" placeholder="order_id,sales_amt" data-testid="input-required" />
        </label>
        <label class="filter-item">
          <span>目标字段类型（字段:类型，逗号分隔）</span>
          <input
            v-model="form.fieldTypes"
            placeholder="order_id:VARCHAR(32),sales_amt:DECIMAL(18,2)"
            data-testid="input-field-types"
          />
        </label>
        <label class="sample-editor">
          <span>样本数据（JSON 数组，支持数字、空值、文本混合）</span>
          <textarea
            v-model="form.samplesText"
            rows="9"
            data-testid="input-samples"
            spellcheck="false"
          ></textarea>
        </label>
        <div class="page-actions">
          <button class="btn primary" type="submit" data-testid="submit-batch" :disabled="submitting">
            {{ submitting ? '验证中…' : '提交验证' }}
          </button>
          <button class="btn ghost" type="button" data-testid="fill-sample" @click="fillDemoSamples">
            填入混合样本示例
          </button>
        </div>
        <p v-if="formError" class="error-text" data-testid="form-error">{{ formError }}</p>
      </form>
    </div>

    <div class="verification-panel">
      <h3>验证批次（含规则修订版本与样本范围）</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>批次编号</th>
            <th>映射规则</th>
            <th>目标表</th>
            <th>规则修订/指纹</th>
            <th>样本范围</th>
            <th>通过/拒收</th>
            <th>验证状态</th>
            <th>规则时效</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="batch in batches" :key="batch.batch_id" :data-testid="`batch-${batch.batch_id}`">
            <td>{{ batch.batch_id }}</td>
            <td>{{ batch.rule_name }}</td>
            <td>{{ batch.target_table }}</td>
            <td>
              rev{{ batch.rule_revision }}
              <code>{{ batch.rule_fingerprint.slice(0, 8) }}</code>
            </td>
            <td>
              {{ batch.sample_scope.sample_count }}行 /
              分区 {{ batch.sample_scope.partitions.join(',') }}
            </td>
            <td>{{ batch.passed_count }} / {{ batch.rejected_count }}</td>
            <td>{{ batch.status }}</td>
            <td>
              <span :class="batch.rule_stale ? 'tag tag-stale' : 'tag tag-fresh'">
                {{ batch.rule_stale ? '规则已过期' : '规则一致' }}
              </span>
            </td>
            <td class="row-actions">
              <button class="link" type="button" @click="openRows(batch)">逐行结果</button>
              <button
                class="link"
                type="button"
                :disabled="!batch.delivered"
                :title="batch.delivered ? '' : '该批次尚未交付'"
                @click="batch.delivery_id && rollback(batch.delivery_id)"
              >
                回滚交付
              </button>
            </td>
          </tr>
          <tr v-if="!batches.length">
            <td colspan="9" class="empty-state">暂无验证批次，先提交一批样本</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="selectedBatch" class="verification-panel" data-testid="row-panel">
      <h3>逐行结果 · {{ selectedBatch.batch_id }}</h3>
      <p class="page-desc">
        检查规则：{{ selectedBatch.check_rule_labels.join('、') }}；
        拒收行保留源记录标识，便于修正后重新提交定位。
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>行号</th>
            <th>源记录标识</th>
            <th>目标分区</th>
            <th>转换输出</th>
            <th>结果</th>
            <th>失败原因</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in selectedRows" :key="row.row_no" :data-testid="`row-result-${row.row_no}`">
            <td>{{ row.row_no }}</td>
            <td>{{ row.source_ref }}</td>
            <td>{{ row.partition_value }}</td>
            <td><code>{{ JSON.stringify(row.transformed) }}</code></td>
            <td>
              <span :class="row.status === '通过' ? 'tag tag-fresh' : 'tag tag-stale'">{{ row.status }}</span>
            </td>
            <td>
              <ul v-if="row.failure_reasons.length" class="reason-list">
                <li v-for="(reason, idx) in row.failure_reasons" :key="idx">{{ reason }}</li>
              </ul>
              <span v-else>—</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="verification-panel">
      <h3>可交付分区（部署流水线消费入口，只含实际通过验证的输出）</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>目标表</th>
            <th>分区</th>
            <th>来源批次</th>
            <th>规则依据</th>
            <th>通过行</th>
            <th>拒收行（留档）</th>
            <th>可交付</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in deliverable" :key="`${item.batch_id}-${item.partition_value}`">
            <td>{{ item.target_table }}</td>
            <td>{{ item.partition_value }}</td>
            <td>{{ item.batch_id }}</td>
            <td>rev{{ item.rule_revision }} <code>{{ item.rule_fingerprint.slice(0, 8) }}</code></td>
            <td>{{ item.passed_count }}（{{ item.passed_source_refs.join(',') }}）</td>
            <td>{{ item.rejected_count }}（{{ item.rejected_source_refs.join(',') || '无' }}）</td>
            <td>
              <span :class="item.deliverable ? 'tag tag-fresh' : 'tag tag-stale'">
                {{ item.deliverable ? '可交付' : '规则已过期' }}
              </span>
            </td>
            <td class="row-actions">
              <button
                class="link"
                type="button"
                :disabled="!item.deliverable || item.already_delivered"
                data-testid="deliver-partition"
                @click="deliver(item)"
              >
                {{ item.already_delivered ? '已交付' : '登记交付' }}
              </button>
            </td>
          </tr>
          <tr v-if="!deliverable.length">
            <td colspan="8" class="empty-state">暂无可交付分区，提交全部或部分通过的验证批次后出现</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="verification-panel">
      <h3>交付登记（实际参与发布的数据范围与拒收行）</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>交付单号</th>
            <th>目标表/分区</th>
            <th>规则依据</th>
            <th>发布范围</th>
            <th>拒收留档</th>
            <th>环境</th>
            <th>状态</th>
            <th>时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="delivery in deliveries" :key="delivery.delivery_id" :data-testid="`delivery-${delivery.delivery_id}`">
            <td>{{ delivery.delivery_id }}</td>
            <td>{{ delivery.target_table }} / {{ delivery.partition_value }}</td>
            <td>rev{{ delivery.rule_revision }} <code>{{ delivery.rule_fingerprint.slice(0, 8) }}</code></td>
            <td>{{ delivery.published_scope.row_count }}行：{{ delivery.published_scope.source_refs.join(',') }}</td>
            <td>{{ delivery.rejected_scope.row_count }}行：{{ delivery.rejected_scope.source_refs.join(',') || '无' }}</td>
            <td>{{ delivery.environment }}</td>
            <td>{{ delivery.status }}</td>
            <td>{{ delivery.delivered_at.slice(0, 19).replace('T', ' ') }}</td>
          </tr>
          <tr v-if="!deliveries.length">
            <td colspan="8" class="empty-state">暂无交付记录</td>
          </tr>
        </tbody>
      </table>
    </div>

    <footer class="page-foot">
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { listEntries } from '@/api/backend-service'
import {
  compareBatches,
  deliverPartition,
  listDeliverablePartitions,
  listDeliveries,
  listVerificationBatches,
  listVerificationRows,
  rollbackDelivery,
  submitVerificationBatch,
  type SampleBatchInput,
} from '@/api/verification-service'
import type {
  DeliverablePartition,
  DeliveryRecord,
  EntryRow,
  VerificationBatch,
  VerificationRow,
} from '@/data/types'

const mappings = ref<EntryRow[]>([])
const batches = ref<VerificationBatch[]>([])
const deliverable = ref<DeliverablePartition[]>([])
const deliveries = ref<DeliveryRecord[]>([])
const selectedBatch = ref<VerificationBatch | null>(null)
const selectedRows = ref<VerificationRow[]>([])
const submitting = ref(false)
const errorMessage = ref('')
const formError = ref('')

const form = ref({
  mappingId: 0,
  partitionField: 'dt',
  pkField: 'order_id',
  validPks: 'ORD-001,ORD-002,ORD-003',
  requiredFields: 'order_id,sales_amt',
  fieldTypes: 'order_id:VARCHAR(32),sales_amt:DECIMAL(18,2)',
  samplesText: '',
})

const totalPassed = computed(() => batches.value.reduce((sum, b) => sum + b.passed_count, 0))
const totalRejected = computed(() => batches.value.reduce((sum, b) => sum + b.rejected_count, 0))
const revisionMap = computed(() => {
  const map = new Map<number, number>()
  for (const batch of batches.value) map.set(batch.mapping_id, batch.rule_revision)
  return map
})

function ruleRevOf(mappingId: number): string {
  const rev = revisionMap.value.get(mappingId)
  return rev ? String(rev) : '1'
}

function splitList(text: string): string[] {
  return text
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

function parseFieldTypes(text: string): Record<string, string> {
  const result: Record<string, string> = {}
  for (const pair of splitList(text)) {
    const [field, type] = pair.split(':').map((part) => part?.trim())
    if (field && type) result[field] = type
  }
  return result
}

function fillDemoSamples() {
  form.value.samplesText = JSON.stringify(
    [
      { _source_ref: 'SRC-1001', order_id: 'ORD-001', amt: 100, dt: '2026-11-01' },
      { _source_ref: 'SRC-1002', order_id: 'ORD-002', amt: null, dt: '2026-11-01' },
      { _source_ref: 'SRC-1003', order_id: 'ORD-003', amt: 'abc文本', dt: '2026-11-01' },
      { _source_ref: 'SRC-1004', order_id: 'ORD-999', amt: 88, dt: '2026-11-01' },
    ],
    null,
    2,
  )
}

async function submitBatch() {
  formError.value = ''
  errorMessage.value = ''
  let samples: SampleBatchInput['samples']
  try {
    const parsed = JSON.parse(form.value.samplesText)
    if (!Array.isArray(parsed) || !parsed.length) throw new Error('样本必须是非空 JSON 数组')
    samples = parsed
  } catch (error) {
    formError.value = `样本 JSON 解析失败：${error instanceof Error ? error.message : String(error)}`
    return
  }
  submitting.value = true
  try {
    const payload: SampleBatchInput = {
      samples,
      partition_field: form.value.partitionField || 'dt',
      target_pk_field: form.value.pkField,
      valid_target_pks: splitList(form.value.validPks),
      required_fields: splitList(form.value.requiredFields),
      field_types: parseFieldTypes(form.value.fieldTypes),
    }
    const batch = await submitVerificationBatch(form.value.mappingId, payload)
    await refreshAll()
    await openRows(batch)
    // 若修正后重提形成多个批次，自动产出对比结果供排查（仅控制台留痕）。
    const sameMapping = batches.value.filter((b) => b.mapping_id === form.value.mappingId)
    if (sameMapping.length >= 2) {
      await compareBatches(sameMapping.slice(0, 2).map((b) => b.batch_id))
    }
  } catch (error) {
    formError.value = error instanceof Error ? error.message : '验证提交失败'
  } finally {
    submitting.value = false
  }
}

async function openRows(batch: VerificationBatch) {
  selectedBatch.value = batch
  selectedRows.value = await listVerificationRows(batch.batch_id)
}

async function deliver(item: DeliverablePartition) {
  errorMessage.value = ''
  try {
    await deliverPartition(item.batch_id, item.partition_value, '预发')
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交付登记失败'
  }
}

async function rollback(deliveryId: string) {
  errorMessage.value = ''
  try {
    await rollbackDelivery(deliveryId, '前端工作台触发回滚')
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回滚失败'
  }
}

async function refreshAll() {
  const [batchList, partitionList, deliveryList] = await Promise.all([
    listVerificationBatches(),
    listDeliverablePartitions(),
    listDeliveries(),
  ])
  batches.value = batchList
  deliverable.value = partitionList
  deliveries.value = deliveryList
  if (selectedBatch.value) {
    const refreshed = batchList.find((b) => b.batch_id === selectedBatch.value?.batch_id)
    if (refreshed) await openRows(refreshed)
  }
}

onMounted(async () => {
  const page = await listEntries('etl_mapping')
  mappings.value = page.items
  if (mappings.value.length) form.value.mappingId = Number(mappings.value[0].id)
  fillDemoSamples()
  await refreshAll()
})
</script>

<style scoped>
.verification-panel {
  background: var(--color-surface, #fff);
  border: 1px solid var(--color-border, #e3e6ee);
  border-radius: 10px;
  padding: 16px 18px;
  margin-bottom: 18px;
}

.verification-panel h3 {
  margin: 0 0 12px;
  font-size: 15px;
}

.sample-editor {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
}

.sample-editor textarea {
  width: 100%;
  font-family: 'SFMono-Regular', Consolas, monospace;
  font-size: 12px;
  resize: vertical;
}

.tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 12px;
  white-space: nowrap;
}

.tag-fresh {
  background: #e6f7ec;
  color: #1a7f37;
}

.tag-stale {
  background: #fdecea;
  color: #b42318;
}

.reason-list {
  margin: 0;
  padding-left: 16px;
  color: #b42318;
  font-size: 12px;
}

code {
  font-size: 11px;
  color: #55607a;
}
</style>
