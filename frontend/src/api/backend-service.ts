import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// -- 稀疏分窗指标：窗口统计、整期报表、发布与确认清单 --

function queryOf(params: Record<string, string | number | boolean | undefined>): string {
  const query = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== '') query.set(key, String(value))
  }
  return query.toString()
}

export type WindowMetricState = '已观测' | '空窗' | '缺证据'
export type WindowValueKind = '实测值' | '实测零值' | '无观测'

export type WindowMetric = {
  id: number
  窗口起点: string
  窗口终点: string
  观测状态: WindowMetricState
  值类型: WindowValueKind
  观测值: number | null
  事件数: number
  缺值原因: string | null
  [field: string]: unknown
}

export type SparseWindowSummary = {
  [field: string]: string | number | boolean | null | unknown[]
}

export type PeriodReport = {  ok: boolean
  已观测均值: number | null
  整期均值: number | null
  完整: boolean
  空窗数: number
  缺证据数: number
  实测零值窗口数: number
  [field: string]: unknown
}

export type PublicationResult = {
  ok: boolean
  发布编号?: string
  状态?: string
  确认清单?: Record<string, unknown>[]
  [field: string]: unknown
}

export async function ingestWindowEvent(
  params: Record<string, string | number | undefined>,
): Promise<Record<string, unknown>> {
  const response = await request(`/api/window-metrics/events?${queryOf(params)}`, { method: 'POST' })
  if (!response.ok) throw new Error('事件登记失败')
  return await response.json()
}

export async function computeSparseWindows(
  params: Record<string, string | number | boolean | undefined>,
): Promise<Record<string, unknown>> {
  const response = await request(`/api/window-metrics/compute?${queryOf(params)}`, { method: 'POST' })
  if (!response.ok) throw new Error('分窗计算失败')
  return await response.json()
}

export async function loadSparseWindows(
  params: Record<string, string | undefined> = {},
): Promise<{ items: WindowMetric[]; total: number }> {
  const response = await request(`/api/window-metrics?${queryOf(params)}`)
  if (!response.ok) throw new Error('分窗指标读取失败')
  return await response.json()
}

export async function loadPeriodReport(
  params: Record<string, string | number | boolean | undefined>,
): Promise<PeriodReport> {
  const response = await request(`/api/window-metrics/report?${queryOf(params)}`)
  if (!response.ok) throw new Error('整期报表读取失败')
  return await response.json()
}

export async function publishSparsePeriod(
  params: Record<string, string | number | undefined>,
): Promise<PublicationResult> {
  const response = await request(`/api/window-metrics/publish?${queryOf(params)}`, { method: 'POST' })
  if (!response.ok) throw new Error('指标发布失败')
  return await response.json()
}

export async function retryPublication(publicationId: string | number): Promise<PublicationResult> {
  const response = await request(`/api/window-metrics/publications/${publicationId}/retry`, { method: 'POST' })
  if (!response.ok) throw new Error('发布重试失败')
  return await response.json()
}

export async function loadWindowConfirmations(
  params: Record<string, string | undefined> = {},
): Promise<{ items: Record<string, unknown>[]; total: number }> {
  const response = await request(`/api/window-metrics/confirmations?${queryOf(params)}`)
  if (!response.ok) throw new Error('窗口确认清单读取失败')
  return await response.json()
}

export async function loadPublicationEvents(
  publicationId: string | number,
): Promise<Record<string, unknown>> {
  const response = await request(`/api/window-metrics/publications/${publicationId}`)
  if (!response.ok) throw new Error('发布事件关联读取失败')
  return await response.json()
}
