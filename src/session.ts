/**
 * Taiwan calendar / session constants — ONE definition.
 *
 * The app stores UTC epoch ms; Taiwan is a FIXED UTC+8 (no DST), so a
 * time-of-day is plain arithmetic and every module that needs "what time is it
 * in Taipei" (the session windows, the demo venue's RTH force-close, the day
 * cache's freshness gate, the replay's session opens) reads it here instead of
 * re-deriving `+ 8h % 86400000`.
 *
 * `*_TOD_MS` values are milliseconds AFTER TAIPEI MIDNIGHT (0..86_399_999) —
 * the unit `todOf` answers.
 */
export const TAIPEI_OFFSET_MS = 8 * 3_600_000
export const DAY_MS = 86_400_000

/** RTH open (futures AND stocks): 08:45. */
export const RTH_OPEN_TOD_MS = 31_500_000
/** Futures RTH close: 13:45 (the index futures + the letter-coded stock
 *  futures). */
export const FUTURES_CLOSE_TOD_MS = 49_500_000
/** Stocks / `00`-prefixed ETFs close: 13:30. */
export const STOCK_CLOSE_TOD_MS = 48_600_000
/** Night session (ETH) open: 15:00. */
export const NIGHT_OPEN_TOD_MS = 54_000_000
/** Night session close: 05:00 the NEXT day. */
export const NIGHT_CLOSE_TOD_MS = 18_000_000

/** Taiwan time-of-day (ms since local midnight, 0..86_399_999). */
export function todOf(tsMs: number): number {
  return (((tsMs + TAIPEI_OFFSET_MS) % DAY_MS) + DAY_MS) % DAY_MS
}

/** The integer Taipei day index (days since the epoch in Taipei terms) —
 *  equal values mean the same Taipei calendar day. */
export function dayIndexOf(tsMs: number): number {
  return Math.floor((tsMs + TAIPEI_OFFSET_MS) / DAY_MS)
}

/** The Taipei calendar day as `YYYYMMDD` (the form the day cache's freshness
 *  gate compares and the exchanges' rwd endpoints take). */
export function dayStampOf(tsMs: number): string {
  const d = new Date(tsMs + TAIPEI_OFFSET_MS)
  const m = String(d.getUTCMonth() + 1).padStart(2, '0')
  const day = String(d.getUTCDate()).padStart(2, '0')
  return `${d.getUTCFullYear()}${m}${day}`
}

/** A `YYYYMMDD` Taipei session stamp → ms of that day's Taipei midnight (the
 *  inverse of `dayStampOf`; 0 when malformed). */
export function stampToMs(stamp: string): number {
  const m = /^(\d{4})(\d{2})(\d{2})$/.exec(stamp)
  if (!m) return 0
  const year = Number(m[1])
  const month = Number(m[2])
  const day = Number(m[3])
  const ms = Date.UTC(year, month - 1, day)
  const back = new Date(ms)
  if (back.getUTCFullYear() !== year || back.getUTCMonth() !== month - 1) return 0
  if (back.getUTCDate() !== day) return 0
  return ms - TAIPEI_OFFSET_MS
}

/** The instant of a Taipei wall-clock time on `tsMs`'s own Taipei day
 *  (`undoTod(tsMs, RTH_OPEN_TOD_MS)` = that day's 08:45). */
export function instantOfTod(tsMs: number, tod: number): number {
  return dayIndexOf(tsMs) * DAY_MS + tod - TAIPEI_OFFSET_MS
}
