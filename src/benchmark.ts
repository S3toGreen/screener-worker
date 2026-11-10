/**
 * The TAIEX benchmark series — the market context the screener header shows.
 *
 * The index is not in any per-stock table, so it lives in its own R2 object
 * (`benchmark.json`, one array pair over the newest 260 sessions) that the ETL
 * appends to exactly like a symbol's history. The daily close comes from TWSE's
 * `FMTQIK` (成交統計), a ~15-row monthly table — one small request per run
 * instead of re-downloading `MI_INDEX`'s 5 MB of per-stock rows.
 *
 * Horizons are TRADING SESSIONS (5 ≈ a week, 20 ≈ a month, 60 ≈ a quarter,
 * 240 ≈ a year), the same index math the row columns use.
 */
import {
  returnOver,
  type BenchmarkSeries,
  type ScreenerBenchmark
} from '@odf/screener-worker/screener'
import { dayStampOf } from '@odf/screener-worker/session'
import type { FetchLike } from '@odf/screener-worker/httpJson'
import { getJson, num, rocToMs } from '@odf/screener-worker/httpJson'

const TWSE_RWD = 'https://www.twse.com.tw/rwd/zh'
/** The bucket key (the ETL's private state, never served as an API route). */
export const BENCHMARK_KEY = 'benchmark.json'
/** Sessions retained — the same 52-week window as a symbol's history. */
const SESSIONS = 260

export interface IndexRow {
  /** `YYYYMMDD` Taipei session stamp. */
  stamp: string
  close: number
}

/** One month of TAIEX closes (`FMTQIK?date=YYYYMM01`). */
export async function twseMonthIndex(dateMs: number, fetchImpl?: FetchLike): Promise<IndexRow[]> {
  const stamp = dayStampOf(dateMs)
  const url = `${TWSE_RWD}/afterTrading/FMTQIK?date=${stamp.slice(0, 6)}01&response=json`
  const payload = (await getJson(url, fetchImpl)) as {
    stat?: string
    fields?: string[]
    data?: unknown[][]
  } | null
  if (payload?.stat !== 'OK' || !Array.isArray(payload.data)) return []
  const at = (payload.fields ?? []).indexOf('發行量加權股價指數')
  if (at < 0) return []
  const out: IndexRow[] = []
  for (const row of payload.data) {
    const ms = rocToMs(String(row[0] ?? ''))
    const session = ms > 0 ? dayStampOf(ms) : ''
    const close = num(String(row[at] ?? ''))
    if (!session || !Number.isFinite(close) || close <= 0) continue
    out.push({ stamp: session, close })
  }
  return out
}

/** The `count` month-start epochs (Taipei) ending with `sessionDate`'s month,
 *  ascending. Used to rebuild a missing/short benchmark series from FMTQIK. */
export function benchmarkMonthStarts(sessionDate: number, count: number): number[] {
  if (!(sessionDate > 0) || count <= 0) return []
  const d = new Date(sessionDate + 8 * 3_600_000)
  let year = d.getUTCFullYear()
  let month = d.getUTCMonth()
  const out: number[] = []
  for (let i = 0; i < count; i++) {
    out.push(Date.UTC(year, month, 1) - 8 * 3_600_000)
    month -= 1
    if (month < 0) {
      month = 11
      year -= 1
    }
  }
  return out.reverse()
}

/** Upserts the month's rows into the stored series (a re-run REPLACES the same
 *  session), then keeps the newest 260. */
export function mergeBenchmark(prior: BenchmarkSeries | null, rows: IndexRow[]): BenchmarkSeries {
  const byStamp = new Map<string, number>()
  if (prior) {
    for (let i = 0; i < prior.dates.length; i++) {
      const close = prior.close[i]
      if (typeof close === 'number' && close > 0) byStamp.set(prior.dates[i], close)
    }
  }
  for (const row of rows) byStamp.set(row.stamp, row.close)
  const dates = [...byStamp.keys()].sort()
  const trimmed = dates.slice(Math.max(0, dates.length - SESSIONS))
  return { dates: trimmed, close: trimmed.map((d) => byStamp.get(d) as number) }
}

/** `shape` guard for the stored object (a truncated payload is dropped). */
export function shapedBenchmark(raw: unknown): raw is BenchmarkSeries {
  if (!raw || typeof raw !== 'object') return false
  const s = raw as BenchmarkSeries
  return (
    Array.isArray(s.dates) &&
    Array.isArray(s.close) &&
    s.close.length === s.dates.length &&
    s.dates.length > 0
  )
}

/** The five returns over the series' tail — shared formula with the rows. */
export function benchmarkReturns(series: BenchmarkSeries): ScreenerBenchmark {
  const last = series.close[series.close.length - 1]
  return {
    close: typeof last === 'number' && last > 0 ? last : undefined,
    ret1d: returnOver(series.close, 1),
    ret5d: returnOver(series.close, 5),
    ret20d: returnOver(series.close, 20),
    ret60d: returnOver(series.close, 60),
    ret240d: returnOver(series.close, 240)
  }
}
