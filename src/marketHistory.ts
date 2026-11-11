/**
 * Historical market-context builder — reconstructs one `MarketHistoryPoint`
 * from the exchanges' per-date RWD tables. This is the credit half of the
 * market-overview backfill: `seedMarketHistory.ts --backfill N` walks the
 * recent weekdays, calls this module, and uploads the merged series.
 *
 * Sources per session:
 * - TWSE `MI_MARGN?selectType=ALL` — the market-wide credit summary AND the
 *   per-stock margin balances (one request).
 * - TPEx `margin/balance?date=` — per-stock balances + the authoritative
 *   `summary` (合計(張) + 融資金(仟元)); historical, not dashboard-only.
 * - TWSE `MI_INDEX?type=ALLBUT0999` — bars, category totals (WITH TDR/ETF
 *   1,2,4,6,14,15; stock-only 1,6,14,15) and breadth, all in ONE request.
 * - TWSE `MI_QFIIS` — issued shares for the derived stock market cap; already
 *   requested for foreign holdings.
 * - TPEx `afterTrading/otc?type=EW` — bars; `marketStats` — category totals
 *   (WITH 1,2,3,5,6; stock-only 1,2,3); `highlight` — whole-market cap.
 * - TWSE `BFIJ3U_TREND` — official 全市場 keepRate + `crdAmt` (credit amount),
 *   prefetched once for the whole window.
 * - TPEx `dashboardOtc/margin` — ONLY `creditAmount` (09/10 onward; no other
 *   market-level endpoint exists, so earlier points are null).
 *
 * The row construction is deliberately the same positional `ScreenerRow`
 * shape the ETL uses; `buildMarketContext` is then the ONE credit definition,
 * so a backfilled session is indistinguishable from a live one. Missing
 * sub-sources stay null — never zero-filled. A required read that fails, or a
 * session whose financed join is empty, is reported as `incomplete` so the
 * caller retries/fails instead of storing a silent hole.
 */
import type { FetchLike } from '@odf/screener-worker/httpJson'
import { stampToMs } from '@odf/screener-worker/session'
import {
  SC,
  SCREENER_FIELDS,
  mergeMarketContext,
  mergeMarketHistory,
  type MarketContext,
  type MarketHistoryPoint,
  type ScreenerRow
} from './screener'
import {
  buildMarketContext,
  marginTotalsOf,
  TPEX_DAILY_MIN_ROWS,
  TWSE_DAILY_MIN_ROWS,
  type MarketAggregates
} from './etl'
import {
  tpexDayBars,
  tpexDayHighlight,
  tpexDayMarginFull,
  tpexDayMarketStats,
  twseDayForeign,
  twseStockMarketCap,
  twseDayMarket,
  twseMarginDay
} from './history'
import { tpexCreditSummary } from './sources/tpex'
import type { DayBar, MaintenancePoint, MarginSrc, TpexCreditSummary } from './sources/types'

export type MarketHistoryResult =
  | { ok: true; point: MarketHistoryPoint }
  | {
      ok: false
      reason: 'no-session' | 'incomplete'
      detail: string
      /** Only a THROWN/rejected read is worth retrying. An empty payload means
       *  a holiday or a not-yet-published session; retrying it cannot help. */
      retryable?: boolean
      /** A clean empty/missing source (the session is not published yet), as
       *  opposed to a truncated table or a failed join. Only the caller's
       *  newest session may be skipped for this; a middle hole never is. */
      notPublished?: boolean
    }

interface ReadOutcome<T> {
  value: T | null
  failed: boolean
  error?: string
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

/** The OTC dashboard's market-level credit series starts on 2026-09-10; there
 *  is no other historical endpoint for TPEx `creditAmount`. Do not spend one
 *  request per older session on an endpoint that cannot answer it. */
const TPEX_DASHBOARD_START_MS = stampToMs('20260910')

/** Retry a REQUIRED read only on a THROWN/rejected request. An empty payload is
 *  returned immediately: it means a holiday or a session that has not been
 *  published yet, and a retry cannot change it. */
async function readRequired<T>(read: () => Promise<T | null>): Promise<ReadOutcome<T>> {
  let lastError = ''
  for (let attempt = 0; ; attempt++) {
    try {
      return { value: await read(), failed: false }
    } catch (err) {
      lastError = String(err)
      if (attempt >= 2) return { value: null, failed: true, error: lastError }
    }
    await sleep(1_000 * 2 ** attempt)
  }
}

/** An OPTIONAL read: its absence/null is a normal data gap, never a session
 *  failure. Retried once if the request THREW. */
async function readOptional<T>(read: () => Promise<T | null>): Promise<ReadOutcome<T>> {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      return { value: await read(), failed: false }
    } catch {
      if (attempt === 1) return { value: null, failed: true }
      await sleep(500)
    }
  }
  return { value: null, failed: true }
}

/** Rows for the per-stock margin + close join. Only symbols present in BOTH
 *  the session's quote table and its margin table enter the financed sum. */
export function creditRows(
  market: 'TWSE' | 'TPEX',
  bars: Map<string, DayBar> | null,
  margin: Map<string, MarginSrc> | null
): ScreenerRow[] {
  if (!bars || !margin) return []
  const rows: ScreenerRow[] = []
  for (const [symbol, bar] of bars) {
    const m = margin.get(symbol)
    if (!m) continue
    const row = SCREENER_FIELDS.map(() => null) as ScreenerRow
    row[SC.symbol] = symbol
    row[SC.market] = market
    row[SC.close] = bar.close
    row[SC.marginBal] = Number.isFinite(m.marginBal) ? m.marginBal : null
    row[SC.shortBal] = Number.isFinite(m.shortBal) ? m.shortBal : null
    rows.push(row)
  }
  return rows
}

function tpexSummaryOf(
  dateMs: number,
  margin: Awaited<ReturnType<typeof tpexDayMarginFull>>,
  stats: Awaited<ReturnType<typeof tpexDayMarketStats>>,
  highlight: Awaited<ReturnType<typeof tpexDayHighlight>>,
  dashboard: TpexCreditSummary | null
): TpexCreditSummary {
  return {
    dateMs,
    marginBalanceNtd: margin?.summary?.marginBalanceNtd ?? null,
    shortBalanceLots: margin?.summary?.shortBalanceLots ?? null,
    creditAmountNtd: dashboard?.creditAmountNtd ?? null,
    marketCapNtd: highlight?.marketCapNtd ?? null,
    marginPrevNtd: margin?.summary?.marginPrevNtd ?? null,
    marginBuyNtd: margin?.summary?.marginBuyNtd ?? null,
    marginSellNtd: margin?.summary?.marginSellNtd ?? null,
    marginCashRepayNtd: margin?.summary?.marginCashRepayNtd ?? null,
    totalAmountNtd: stats?.totalAmountNtd ?? null
  }
}

/**
 * One session's market history point. `no-session` is a holiday/empty
 * exchange day and is safe to skip. `incomplete` means a required primitive
 * (TWSE credit/quotes/shares, or a TPEx market summary) is missing on a day
 * that otherwise has a quote table — the caller must retry or fail, never
 * silently store a hole.
 */
export async function buildMarketHistorySession(
  dateMs: number,
  fetchImpl?: FetchLike,
  official: MaintenancePoint | null = null
): Promise<MarketHistoryResult> {
  const tpexDashRead: Promise<ReadOutcome<TpexCreditSummary>> =
    dateMs >= TPEX_DASHBOARD_START_MS
      ? readOptional(() => tpexCreditSummary(dateMs, fetchImpl))
      : Promise.resolve({ value: null, failed: false })
  const [
    twse,
    twseMarket,
    twseForeign,
    tpexMargin,
    tpexBars,
    tpexStats,
    tpexHighlightRead,
    tpexDash
  ] = await Promise.all([
    readRequired(() => twseMarginDay(dateMs, fetchImpl)),
    readRequired(() => twseDayMarket(dateMs, fetchImpl)),
    readRequired(() => twseDayForeign(dateMs, fetchImpl)),
    readRequired(() => tpexDayMarginFull(dateMs, fetchImpl)),
    readRequired(() => tpexDayBars(dateMs, fetchImpl)),
    readRequired(() => tpexDayMarketStats(dateMs, fetchImpl)),
    readRequired(() => tpexDayHighlight(dateMs, fetchImpl)),
    tpexDashRead
  ])
  const required = [
    ['TWSE MI_MARGN', twse],
    ['TWSE MI_INDEX', twseMarket],
    ['TWSE MI_QFIIS', twseForeign],
    ['TPEx margin/balance', tpexMargin],
    ['TPEx otc', tpexBars],
    ['TPEx marketStats', tpexStats],
    ['TPEx highlight', tpexHighlightRead]
  ] as const
  for (const [name, read] of required) {
    if (read.failed) {
      return {
        ok: false,
        reason: 'incomplete',
        detail: `${name} read failed${read.error ? ` (${read.error})` : ''}`,
        retryable: true
      }
    }
  }

  const summary = twse.value?.summary ?? null
  const stocks = twse.value?.stocks ?? null
  const twseDay = twseMarket.value
  const bars = twseDay?.bars ?? null
  const margin = tpexMargin.value
  const tpexDay = tpexBars.value
  const stats = tpexStats.value
  const highlight = tpexHighlightRead.value

  // A holiday answers nothing on BOTH exchanges. One market answering while
  // the other is empty is a partial session, never a holiday to skip.
  const twseHasSession = Boolean(summary || (bars && bars.size > 0))
  // A not-yet-published/empty TPEx `marketStats` can still be an object with
  // null totals; only REAL rows/money make it a session answer.
  const tpexHasSession = Boolean(
    margin?.summary ||
    (tpexDay && tpexDay.size > 0) ||
    (stats && (stats.totalAmountNtd ?? 0) > 0) ||
    (highlight && (highlight.marketCapNtd ?? 0) > 0)
  )
  if (!twseHasSession && !tpexHasSession && !official) {
    return { ok: false, reason: 'no-session', detail: 'no TWSE or TPEx session tables' }
  }
  if (!twseHasSession || !tpexHasSession) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'only one market answered the session (TWSE or TPEx missing)',
      notPublished: true
    }
  }
  const twseTruncated =
    (bars !== null && bars.size > 0 && bars.size < TWSE_DAILY_MIN_ROWS) ||
    (stocks !== null && stocks.size > 0 && stocks.size < TWSE_DAILY_MIN_ROWS)
  if (
    !summary ||
    !stocks ||
    stocks.size < TWSE_DAILY_MIN_ROWS ||
    !bars ||
    bars.size < TWSE_DAILY_MIN_ROWS
  ) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'TWSE summary, per-stock margin or quote table missing/truncated',
      notPublished: !twseTruncated
    }
  }
  if (!official) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'BFIJ3U official keepRate missing',
      notPublished: true
    }
  }
  if (official.creditAmountNtd == null) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'BFIJ3U official creditAmount missing',
      notPublished: true
    }
  }
  const tpexTruncated =
    (margin?.stocks !== undefined &&
      margin.stocks !== null &&
      margin.stocks.size > 0 &&
      margin.stocks.size < TPEX_DAILY_MIN_ROWS) ||
    (tpexDay !== null && tpexDay.size > 0 && tpexDay.size < TPEX_DAILY_MIN_ROWS)
  if (
    !margin?.summary ||
    !margin.stocks ||
    margin.stocks.size < TPEX_DAILY_MIN_ROWS ||
    !tpexDay ||
    tpexDay.size < TPEX_DAILY_MIN_ROWS
  ) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'TPEx margin/market summary missing/truncated',
      notPublished: !tpexTruncated
    }
  }
  if (margin.summary.marginBalanceNtd == null || margin.summary.shortBalanceLots == null) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'TPEx margin summary money/short balance missing',
      notPublished: true
    }
  }
  if (!stats || !highlight) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'TPEx marketStats/highlight missing',
      notPublished: true
    }
  }
  if (twseDay?.totalsIncomplete || stats.totalsIncomplete) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'marketStats category set truncated',
      notPublished: false
    }
  }
  if (
    twseDay?.totalAmountNtd == null ||
    !(twseDay.totalAmountNtd > 0) ||
    twseDay.stockTotalAmountNtd == null ||
    !(twseDay.stockTotalAmountNtd > 0)
  ) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'TWSE marketStats totals missing/zero',
      notPublished: true
    }
  }
  if (
    stats.totalAmountNtd == null ||
    !(stats.totalAmountNtd > 0) ||
    stats.stockTotalAmountNtd == null ||
    !(stats.stockTotalAmountNtd > 0)
  ) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'TPEx marketStats totals missing/zero',
      notPublished: true
    }
  }
  if (highlight.marketCapNtd == null || !(highlight.marketCapNtd > 0)) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'TPEx whole-market cap missing',
      notPublished: true
    }
  }

  // The stock-only cap join must cover the session's stock universe. A
  // truncated MI_QFIIS page would otherwise produce a plausible but tiny cap
  // (and an inflated margin/cap ratio).
  const twseStockBars = [...bars.keys()].filter((s) => /^[1-9][0-9]{3}$/.test(s)).length
  let capPairs = 0
  for (const [symbol, bar] of bars) {
    if (!/^[1-9][0-9]{3}$/.test(symbol)) continue
    const hold = twseForeign.value?.get(symbol)
    if (
      hold &&
      Number.isFinite(hold.issuedShares) &&
      Number.isFinite(bar.close) &&
      (hold.issuedShares as number) > 0
    ) {
      capPairs++
    }
  }
  if (twseStockBars < TWSE_DAILY_MIN_ROWS || capPairs < Math.ceil(twseStockBars * 0.8)) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: `TWSE cap join covers ${capPairs}/${twseStockBars} stocks`,
      // An empty MI_QFIIS is the not-yet-published case; a non-empty map that
      // fails the join is a truncated/mis-shaped table and must abort.
      notPublished: !twseForeign.value || twseForeign.value.size === 0
    }
  }
  const twseCap = twseStockMarketCap(twseForeign.value, bars)
  if (twseCap === null || !(twseCap > 0)) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'TWSE market cap join is empty/zero',
      notPublished: false
    }
  }

  const twseRows = creditRows('TWSE', bars, stocks)
  const tpexRows = creditRows('TPEX', tpexDay, margin.stocks)
  if (
    twseRows.length < Math.ceil(TWSE_DAILY_MIN_ROWS * 0.8) ||
    tpexRows.length < Math.ceil(TPEX_DAILY_MIN_ROWS * 0.8)
  ) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: `financed join truncated (TWSE ${twseRows.length}, TPEx ${tpexRows.length})`
    }
  }
  const rows = [...twseRows, ...tpexRows]
  const aggregates: MarketAggregates = {
    twseMarketCapNtd: twseCap,
    twseTotalAmountNtd: twseDay.totalAmountNtd,
    twseStockTotalAmountNtd: twseDay.stockTotalAmountNtd,
    twseCreditAmountNtd: official.creditAmountNtd ?? null,
    tpexMarketCapNtd: highlight.marketCapNtd,
    tpexTotalAmountNtd: stats.totalAmountNtd,
    tpexStockTotalAmountNtd: stats.stockTotalAmountNtd,
    tpexCreditAmountNtd: tpexDash.value?.creditAmountNtd ?? null
  }
  const tpexSummary = tpexSummaryOf(dateMs, margin, stats, highlight, tpexDash.value)
  const context = buildMarketContext(
    summary,
    tpexSummary,
    official,
    null,
    rows,
    dateMs,
    marginTotalsOf(margin.stocks),
    [],
    aggregates
  )
  if (!context?.credit || context.credit.scope !== 'TWSE+TPEX') {
    return {
      ok: false,
      reason: 'incomplete',
      detail: `credit scope is ${context?.credit?.scope ?? 'absent'}, expected TWSE+TPEX`,
      notPublished: true
    }
  }
  if (context.credit.financedValueNtd === null) {
    return {
      ok: false,
      reason: 'incomplete',
      detail: 'financed value join is empty'
    }
  }
  return {
    ok: true,
    point: { sessionDate: dateMs, credit: context.credit, derivatives: null }
  }
}

/** Field-wise same-date merge of points. A TAIFEX-only point and an RWD
 *  credit point for one session become ONE observation with both halves; a
 *  later full point wins non-null cells, never erasing the other half. */
export function mergeHistoryPoints(points: readonly MarketHistoryPoint[]): MarketHistoryPoint[] {
  const byDate = new Map<number, MarketHistoryPoint>()
  for (const point of points) {
    if (!point || point.sessionDate <= 0 || (!point.credit && !point.derivatives)) continue
    const prior = byDate.get(point.sessionDate)
    if (!prior) {
      byDate.set(point.sessionDate, point)
      continue
    }
    const merged = mergeMarketContext(
      { asOf: prior.sessionDate, credit: prior.credit, derivatives: prior.derivatives },
      { asOf: point.sessionDate, credit: point.credit, derivatives: point.derivatives }
    )
    byDate.set(point.sessionDate, {
      sessionDate: point.sessionDate,
      credit: merged.credit,
      derivatives: merged.derivatives
    })
  }
  return [...byDate.values()].sort((a, b) => a.sessionDate - b.sessionDate)
}

/** Merge the point sources and the current observation into the capped rolling
 *  series. Filtering by the current anchor BEFORE the cap is essential: a
 *  derivatives-only point newer than the credit anchor (TAIFEX can publish
 *  first) must not consume one of the 60 slots only to be dropped after. */
export function mergeCappedMarketHistory(
  points: readonly MarketHistoryPoint[],
  current: MarketContext
): MarketHistoryPoint[] {
  const bounded = mergeHistoryPoints(points).filter((point) => point.sessionDate <= current.asOf)
  return mergeMarketHistory(bounded, current)
}

/** The newest point that actually carries a credit observation. The
 *  derivatives-only TAIFEX half must never become the object's top-level
 *  credit anchor. */
export function newestCreditPoint(
  points: readonly MarketHistoryPoint[]
): MarketHistoryPoint | null {
  let newest: MarketHistoryPoint | null = null
  for (const point of points) {
    if (!point.credit || point.sessionDate <= 0) continue
    if (!newest || point.sessionDate > newest.sessionDate) newest = point
  }
  return newest
}
