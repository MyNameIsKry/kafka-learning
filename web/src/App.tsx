import { useEffect, useState } from 'react'
import { getAlerts, getSummary, getTransactions, type Alert, type Page, type Summary, type Transaction } from './api'

type DashboardData = {
  summary: Summary
  transactions: Page<Transaction>
  alerts: Page<Alert>
}

type TransactionFilter = 'all' | 'pending' | 'normal' | 'anomaly'

const numberFormat = new Intl.NumberFormat('en-US')
const moneyFormat = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 })
const transactionPageSize = 12
const alertPageSize = 8

function formatDate(value?: string) {
  if (!value) return '—'
  return new Intl.DateTimeFormat('en-US', { dateStyle: 'medium', timeStyle: 'short' }).format(new Date(value))
}

function scoreLabel(score?: number) {
  return score === undefined ? '—' : score.toFixed(3)
}

function App() {
  const [data, setData] = useState<DashboardData | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [filter, setFilter] = useState<TransactionFilter>('all')
  const [transactionPage, setTransactionPage] = useState(0)
  const [alertPage, setAlertPage] = useState(0)

  useEffect(() => {
    let cancelled = false

    const load = async () => {
      try {
          const [summary, transactions, alerts] = await Promise.all([
            getSummary(),
            getTransactions(transactionPageSize, transactionPage * transactionPageSize, filter === 'all' ? '' : filter),
            getAlerts(alertPageSize, alertPage * alertPageSize),
        ])
        if (!cancelled) {
          setData({ summary, transactions, alerts })
          setError(null)
          setLastUpdated(new Date())
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : 'Unable to load dashboard data')
        }
      }
    }

    void load()
    const refreshTimer = window.setInterval(() => void load(), 10_000)
    return () => {
      cancelled = true
      window.clearInterval(refreshTimer)
    }
  }, [alertPage, filter, transactionPage])

  const summary = data?.summary
  const transactions = data?.transactions.items ?? []
  const alerts = data?.alerts.items ?? []

  return (
    <main className="app-shell">
      <header className="hero">
        <div>
            <p className="eyebrow">KURRENTDB · KAFKA · ONNX MOCK</p>
            <h1>Financial anomaly console</h1>
            <p className="subtitle">
              PaySim transaction flow, model scores and anomaly alerts from the live persistence pipeline.
            </p>
        </div>
        <div className={`connection-pill ${error ? 'is-error' : ''}`}>
          <span className="status-dot" />
          {error ? 'API unavailable' : data ? 'API connected' : 'Connecting…'}
        </div>
      </header>

      {error && (
        <section className="error-banner" role="alert">
          <strong>Dashboard data unavailable.</strong> {error}. Start the Go API and MongoDB, then refresh.
        </section>
      )}

      <section className="metrics-grid" aria-label="Pipeline summary">
        <MetricCard label="Transactions" value={summary ? numberFormat.format(summary.totalTransactions) : '—'} detail={`Latest step ${summary?.latestStep ?? '—'}`} />
        <MetricCard label="Predictions" value={summary ? numberFormat.format(summary.totalPredictions) : '—'} detail={summary?.latestModelVersion ?? 'No model reported'} />
        <MetricCard label="Anomalies" value={summary ? numberFormat.format(summary.totalAnomalies) : '—'} detail={summary ? `${(summary.anomalyRate * 100).toFixed(2)}% of predictions` : 'Awaiting data'} accent />
        <MetricCard label="Refresh" value={lastUpdated ? formatDate(lastUpdated.toISOString()) : '—'} detail="Automatic refresh every 10s" />
      </section>

      <div className="content-grid">
        <section className="panel panel-wide">
          <div className="panel-heading">
            <div>
              <p className="section-kicker">STREAM OVERVIEW</p>
              <h2>Recent transactions</h2>
            </div>
            <div className="panel-actions">
              <label className="filter-control">
                <span>Show</span>
                <select value={filter} onChange={(event) => { setFilter(event.target.value as TransactionFilter); setTransactionPage(0) }}>
                  <option value="all">All transactions</option>
                  <option value="pending">Pending</option>
                  <option value="normal">Normal</option>
                  <option value="anomaly">Anomaly</option>
                </select>
              </label>
                <span className="panel-count">{transactions.length} shown</span>
            </div>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Transaction</th>
                    <th>Sender</th>
                    <th>Receiver</th>
                    <th>Step</th>
                  <th>Type</th>
                  <th>Amount</th>
                  <th>Score</th>
                  <th>State</th>
                </tr>
              </thead>
              <tbody>
                {transactions.map((transaction) => (
                  <tr key={transaction.transactionId} className={transaction.isAnomaly ? 'row-anomaly' : ''}>
                    <td><span className="mono">{transaction.transactionId}</span></td>
                    <td><span className="account-tag sender-tag">{transaction.nameOrig}</span></td>
                    <td><span className="account-tag receiver-tag">{transaction.nameDest}</span></td>
                    <td>{transaction.step}</td>
                    <td><span className="type-tag">{transaction.type}</span></td>
                    <td className={transaction.isAnomaly ? 'amount-high' : ''}>{moneyFormat.format(transaction.amount)}</td>
                    <td className={transaction.isAnomaly ? 'score-high' : ''}>{scoreLabel(transaction.anomalyScore)}</td>
                    <td><span className={`state-badge is-${transaction.status}`}>{transaction.status === 'pending' ? 'Pending' : transaction.status === 'anomaly' ? 'Anomaly' : 'Normal'}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
            {data && transactions.length === 0 && <EmptyState text="No transactions match this filter." />}
          </div>
          <Pagination page={transactionPage} hasMore={data?.transactions.hasMore ?? false} onPrevious={() => setTransactionPage((page) => Math.max(0, page - 1))} onNext={() => setTransactionPage((page) => page + 1)} />
        </section>

        <section className="panel alert-panel">
          <div className="panel-heading">
            <div>
              <p className="section-kicker">REVIEW QUEUE</p>
              <h2>Latest alerts</h2>
            </div>
            <span className="alert-indicator">{summary?.totalAnomalies ?? 0}</span>
          </div>
          <div className="alert-list">
            {alerts.map((alert) => (
              <article className="alert-item" key={`${alert.transactionId}-${alert.modelVersion}`}>
                <div className="alert-topline"><span className="state-badge is-anomaly">Anomaly</span><span className="muted">step {alert.step}</span></div>
                <strong className="mono">{alert.transactionId}</strong>
                <div className="alert-meta"><span>score {alert.anomalyScore.toFixed(3)}</span><span>{formatDate(alert.processedAt)}</span></div>
                {alert.reasonCodes.length > 0 && <p className="reason">{alert.reasonCodes.join(' · ')}</p>}
              </article>
            ))}
            {data && alerts.length === 0 && <EmptyState text="No anomaly alerts on this page." />}
          </div>
          <Pagination page={alertPage} hasMore={data?.alerts.hasMore ?? false} onPrevious={() => setAlertPage((page) => Math.max(0, page - 1))} onNext={() => setAlertPage((page) => page + 1)} />
        </section>
      </div>
    </main>
  )
}

function MetricCard({ label, value, detail, accent = false }: { label: string; value: string; detail: string; accent?: boolean }) {
  return <article className={`metric-card ${accent ? 'is-accent' : ''}`}><p>{label}</p><strong>{value}</strong><span>{detail}</span></article>
}

function EmptyState({ text }: { text: string }) {
  return <div className="empty-state">{text}</div>
}

function Pagination({ page, hasMore, onPrevious, onNext }: { page: number; hasMore: boolean; onPrevious: () => void; onNext: () => void }) {
  return <div className="pagination"><button type="button" onClick={onPrevious} disabled={page === 0}>Previous</button><span>Page {page + 1}</span><button type="button" onClick={onNext} disabled={!hasMore}>Next</button></div>
}

export default App
