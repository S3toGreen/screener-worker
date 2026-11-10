/**
 * The ex-dividend CALENDAR — TWSE's own 3-week 除權除息預告表 (`TWT48U`),
 * accumulated into one small object so the daily ETL can put an event column
 * (`exDivInDays`) on the grid:
 *
 *   { updatedAt, events: [ { symbol, exDate, cash, stockRatio, kind } ] }
 *
 * The exchange only ever serves the window STARTING today, and it ignores
 * `?date=` — so the object is maintained by ACCUMULATING every run's window
 * (a same-symbol+date event is replaced, past events are pruned), which makes
 * the calendar stable from the day an event first appears to its ex-date.
 *
 * `cash` is 現金股利 per share (NT$; null when the issuer has not announced —
 * ETFs often show "待公告"), `stockRatio` the 無償配股率 (shares per share).
 * Nothing derives from it at capture; the ETL only joins `exDivInDays`.
 */
import {
  isDividendsObject,
  type DividendEvent,
  type DividendsObject
} from '@odf/screener-worker/screener'
import { DAY_MS } from '@odf/screener-worker/session'
import type { ScreenerEnv } from './env'
import { DIVIDENDS_KEY } from './keys'

/** How long an event is kept AFTER its ex-date (a small grace for late runs). */
const KEEP_DAYS = 7
/** The exchange's window is ~3 weeks; anything later is a stale leftover. */
const MAX_DAYS = 120

export async function readDividends(env: ScreenerEnv): Promise<DividendsObject | null> {
  const obj = await env.R2_BUCKET.get(DIVIDENDS_KEY)
  if (!obj) return null
  try {
    const parsed = await obj.json()
    return isDividendsObject(parsed) ? parsed : null
  } catch {
    return null
  }
}

/** Folds one run's window into the stored calendar and prunes it to
 *  `[session − KEEP_DAYS, session + MAX_DAYS]`. */
export function mergeDividends(
  prior: DividendsObject | null,
  fresh: readonly DividendEvent[],
  sessionDate: number
): DividendsObject {
  const key = (e: DividendEvent): string => `${e.symbol}|${e.exDate}`
  const byKey = new Map<string, DividendEvent>()
  for (const e of prior?.events ?? []) byKey.set(key(e), e)
  for (const e of fresh) byKey.set(key(e), e)
  const from = sessionDate - KEEP_DAYS * DAY_MS
  const to = sessionDate + MAX_DAYS * DAY_MS
  const events = [...byKey.values()]
    .filter((e) => {
      const ms = stampMs(e.exDate)
      return ms > 0 && ms >= from && ms <= to
    })
    .sort((a, b) => a.exDate.localeCompare(b.exDate) || a.symbol.localeCompare(b.symbol))
  return { updatedAt: Date.now(), events }
}

/** `'YYYYMMDD'` → ms (Taipei midnight); 0 when malformed. */
function stampMs(stamp: string): number {
  const m = /^(\d{4})(\d{2})(\d{2})$/.exec(stamp)
  if (!m) return 0
  return Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])) - 8 * 3_600_000
}

/** Days from the session to the symbol's next ex-date (0 = today), or null. */
export function exDivInDays(
  calendar: DividendsObject | null,
  symbol: string,
  sessionDate: number
): number | null {
  let best: number | null = null
  for (const e of calendar?.events ?? []) {
    if (e.symbol !== symbol) continue
    const ms = stampMs(e.exDate)
    if (ms < sessionDate) continue
    const days = Math.round((ms - sessionDate) / DAY_MS)
    if (best === null || days < best) best = days
  }
  return best
}
