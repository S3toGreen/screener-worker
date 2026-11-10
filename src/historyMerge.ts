/**
 * Merge a repaired PREPENDED series onto the stored one. Missing dates are
 * contiguous, but the merge is date-keyed so a same-date row can never
 * duplicate: the repaired value wins, the stored history keeps everything
 * older, and the result is sorted and capped.
 */
import {
  SCREENER_HISTORY_FIELDS,
  SCREENER_HISTORY_SESSIONS,
  type ScreenerHistory,
  type ScreenerHistoryField
} from '@odf/screener-worker/screener'

export function mergeHistorySeries(
  prior: ScreenerHistory | null,
  next: ScreenerHistory | null,
  cap: number = SCREENER_HISTORY_SESSIONS
): ScreenerHistory | null {
  if (!prior) return next
  if (!next) return prior
  const rows = new Map<string, Array<number | null>>()
  const read = (history: ScreenerHistory, overwrite: boolean): void => {
    for (let i = 0; i < history.dates.length; i++) {
      const stamp = history.dates[i]
      if (!stamp) continue
      if (!overwrite && rows.has(stamp)) continue
      rows.set(
        stamp,
        SCREENER_HISTORY_FIELDS.map((field) => {
          const values = history[field as ScreenerHistoryField] as Array<number | null> | undefined
          const value = values?.[i]
          return typeof value === 'number' && Number.isFinite(value) ? value : null
        })
      )
    }
  }
  read(prior, false)
  read(next, true)
  const dates = [...rows.keys()].sort()
  const kept = dates.slice(Math.max(0, dates.length - cap))
  const out: ScreenerHistory = {
    symbol: next.symbol || prior.symbol,
    dates: kept,
    open: [],
    high: [],
    low: [],
    close: [],
    volume: [],
    tickets: [],
    tradeValue: [],
    turnoverRate: [],
    foreign: [],
    trust: [],
    dealer: [],
    per: [],
    pbr: [],
    divYield: [],
    marginBal: [],
    marginQuota: [],
    shortBal: [],
    sblBal: [],
    riskFlags: [],
    grossMargin: [],
    opMargin: [],
    pretaxMargin: [],
    netMargin: [],
    revenueMom: [],
    revenueYoy: [],
    revenueCumYoy: [],
    revenueMonth: [],
    revenueYm: [],
    foreignShares: [],
    foreignRatio: [],
    dayTradeShares: []
  }
  for (const stamp of kept) {
    const row = rows.get(stamp) as Array<number | null>
    SCREENER_HISTORY_FIELDS.forEach((field, i) => {
      ;(out[field as ScreenerHistoryField] as Array<number | null>).push(row[i] ?? null)
    })
  }
  // The reverse gap walk intentionally does NOT fetch MOPS monthly/quarterly
  // reports. Carry the last PUBLISHED value forward across new dates so a
  // repaired gap cannot erase a known margin/revenue reading.
  const carry: ScreenerHistoryField[] = [
    'grossMargin',
    'opMargin',
    'pretaxMargin',
    'netMargin',
    'revenueMom',
    'revenueYoy',
    'revenueCumYoy',
    'revenueMonth',
    'revenueYm'
  ]
  for (const field of carry) {
    const list = out[field] as Array<number | null>
    let last: number | null = null
    for (let i = 0; i < list.length; i++) {
      const value = list[i]
      if (typeof value === 'number' && Number.isFinite(value)) last = value
      else if (last !== null) list[i] = last
    }
  }
  return out
}
