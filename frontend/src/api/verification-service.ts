import { request } from './client'
import type {
  DeliverablePartition,
  DeliveryRecord,
  VerificationBatch,
  VerificationRow,
} from '@/data/types'

const BASE = '/api/etl-mappings'

export type SampleBatchInput = {
  samples: Record<string, string | number | null>[]
  partition_field?: string
  field_types?: Record<string, string>
  required_fields?: string[]
  target_pk_field?: string
  valid_target_pks?: string[]
}

async function unwrap<T>(response: Response): Promise<T> {
  const body = await response.json()
  if (!response.ok) {
    throw new Error(typeof body?.detail === 'string' ? body.detail : '转换验证接口调用失败')
  }
  return body as T
}

/** 提交一批样本，执行逐行类型/必填/目标主键校验。 */
export async function submitVerificationBatch(
  mappingId: number,
  payload: SampleBatchInput,
): Promise<VerificationBatch> {
  const response = await request(`${BASE}/${mappingId}/verification-batches`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return unwrap<VerificationBatch>(response)
}

export async function listVerificationBatches(mappingId?: number): Promise<VerificationBatch[]> {
  const query = mappingId ? `?mapping_id=${mappingId}` : ''
  const response = await request(`${BASE}/verification-batches${query}`)
  const body = await unwrap<{ items: VerificationBatch[] }>(response)
  return body.items
}

export async function getVerificationBatch(batchId: string): Promise<VerificationBatch> {
  const response = await request(`${BASE}/verification-batches/${batchId}`)
  return unwrap<VerificationBatch>(response)
}

export async function listVerificationRows(
  batchId: string,
  status?: string,
): Promise<VerificationRow[]> {
  const query = status ? `?status=${encodeURIComponent(status)}` : ''
  const response = await request(`${BASE}/verification-batches/${batchId}/rows${query}`)
  const body = await unwrap<{ items: VerificationRow[] }>(response)
  return body.items
}

export async function listDeliverablePartitions(
  targetTable?: string,
): Promise<DeliverablePartition[]> {
  const query = targetTable ? `?target_table=${encodeURIComponent(targetTable)}` : ''
  const response = await request(`${BASE}/deliverable-partitions${query}`)
  const body = await unwrap<{ items: DeliverablePartition[] }>(response)
  return body.items
}

export async function deliverPartition(
  batchId: string,
  partitionValue: string,
  environment: string,
): Promise<DeliveryRecord> {
  const response = await request(
    `${BASE}/verification-batches/${batchId}/partitions/${encodeURIComponent(partitionValue)}/deliver`,
    {
      method: 'POST',
      body: JSON.stringify({ environment }),
    },
  )
  return unwrap<DeliveryRecord>(response)
}

export async function listDeliveries(targetTable?: string): Promise<DeliveryRecord[]> {
  const query = targetTable ? `?target_table=${encodeURIComponent(targetTable)}` : ''
  const response = await request(`${BASE}/deliveries${query}`)
  const body = await unwrap<{ items: DeliveryRecord[] }>(response)
  return body.items
}

export async function rollbackDelivery(deliveryId: string, reason: string): Promise<DeliveryRecord> {
  const response = await request(`${BASE}/deliveries/${deliveryId}/rollback`, {
    method: 'POST',
    body: JSON.stringify({ reason }),
  })
  return unwrap<DeliveryRecord>(response)
}

export async function compareBatches(
  batchIds: string[],
): Promise<{
  batches: VerificationBatch[]
  row_progress: { source_ref: string; status_by_batch: Record<string, string> }[]
}> {
  const response = await request(`${BASE}/verification-batches/compare`, {
    method: 'POST',
    body: JSON.stringify({ batch_ids: batchIds }),
  })
  return unwrap(response)
}
