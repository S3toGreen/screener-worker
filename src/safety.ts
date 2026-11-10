/**
 * Missing-history detection — the pure half of the CI safety guard.
 *
 * Missing dates are guaranteed CONTIGUOUS. A stored history is healthy when
 * its newest date IS the latest completed session; an absent one or a series
 * whose newest date is older has a recent gap to repair. A short history that
 * is already current is NOT a repair candidate (the reverse walk would stop on
 * its first existing date anyway).
 *
 * A NEW listing with no history is exempt: the daily ETL starts it.
 *
 * A STALE history is only a repair candidate when the snapshot row actually
 * TRADED that session (`volume > 0`). Suspended / delisted / no-trade rows
 * legitimately have no row for the session; requiring one made the guard
 * repair the same eight symbols on every run forever. An ABSENT history is
 * still repaired regardless of volume: a missing object is a real loss.
 */
import { DAY_MS, dayStampOf, stampToMs } from '@odf/screener-worker/session'
import { SC, type ScreenerRow } from '@odf/screener-worker/screener'

const NEW_LISTING_DAYS = 45

export interface MissingHistory {
  symbol: string
  reason: string
}

/** The session calendar from the exchanges' OWN bar responses: both empty =
 *  a holiday/typhoon closure (safe to skip); one empty while the other traded
 *  is that MARKET's resource skip — the other market still writes its rows. */
export type BarDay = 'closed' | 'session' | 'twse-missing' | 'tpex-missing'

export function classifyBarDay(hasTwse: boolean, hasTpex: boolean): BarDay {
  if (!hasTwse && !hasTpex) return 'closed'
  if (hasTwse && hasTpex) return 'session'
  return hasTwse ? 'tpex-missing' : 'twse-missing'
}

/** Symbols to rebuild; `lastDates` maps symbol → its newest stored session ms
 *  (absent/0 = no readable history). */
export function findMissingHistories(
  rows: readonly ScreenerRow[],
  lastDates: ReadonlyMap<string, number>,
  sessionDate: number,
  newListingDays: number = NEW_LISTING_DAYS
): MissingHistory[] {
  const out: MissingHistory[] = []
  const newListingMs = Math.max(0, newListingDays) * DAY_MS
  for (const row of rows) {
    const symbol = String(row[SC.symbol] ?? '')
    if (!symbol) continue
    const listed = row[SC.listedDate]
    const listedMs = typeof listed === 'string' ? stampToMs(listed) : 0
    const age = sessionDate - listedMs
    const newListing = listedMs > 0 && age >= 0 && age <= newListingMs
    const last = lastDates.get(symbol) ?? 0
    if (last === 0) {
      // The daily ETL starts a genuinely new listing; not a deletion.
      if (!newListing) out.push({ symbol, reason: 'absent' })
      continue
    }
    // Contiguous history: once the latest completed session is present there
    // is nothing behind it to repair (a short but current series is valid).
    // A no-trade row cannot have a session in its history — do not chase it.
    const volume = row[SC.volume]
    const traded = typeof volume === 'number' && volume > 0
    if (traded && last < sessionDate) out.push({ symbol, reason: `stale ${dayStampOf(last)}` })
  }
  return out
}
