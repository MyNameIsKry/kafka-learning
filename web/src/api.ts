export type Summary = {
  totalTransactions: number
  totalPredictions: number
  totalAnomalies: number
  anomalyRate: number
  latestStep: number
  latestModelVersion?: string
  generatedAt: string
}

export type Transaction = {
  transactionId: string
  sourceRowIndex: number
  step: number
  type: string
  amount: number
  nameOrig: string
  nameDest: string
  oldbalanceOrg: number
  newbalanceOrig: number
  oldbalanceDest: number
  newbalanceDest: number
  anomalyScore?: number
  isAnomaly: boolean
  status: 'pending' | 'normal' | 'anomaly'
  modelVersion?: string
  processedAt?: string
}

export type Alert = {
  transactionId: string
  step: number
  anomalyScore: number
  modelVersion: string
  processedAt: string
  reasonCodes: string[]
}

export type Page<T> = {
  items: T[]
  limit: number
  offset: number
  hasMore: boolean
}

// Use Vite's same-origin proxy during local development; set this for a deployed UI.
const apiBase = (import.meta.env.VITE_API_BASE_URL ?? '').replace(/\/$/, '')

async function request<T>(path: string): Promise<T> {
  const response = await fetch(`${apiBase}${path}`)
  if (!response.ok) {
    throw new Error(`API request failed (${response.status})`)
  }
  return response.json() as Promise<T>
}

export function getSummary(): Promise<Summary> {
  return request<Summary>('/api/v1/summary')
}

export async function getTransactions(limit = 12, offset = 0, status = ''): Promise<Page<Transaction>> {
  const query = new URLSearchParams({ limit: String(limit), offset: String(offset) })
  if (status) query.set('status', status)
  return request<Page<Transaction>>(`/api/v1/transactions?${query.toString()}`)
}

export function getAlerts(limit = 8, offset = 0): Promise<Page<Alert>> {
  const query = new URLSearchParams({ limit: String(limit), offset: String(offset) })
  return request<Page<Alert>>(`/api/v1/alerts?${query.toString()}`)
}
