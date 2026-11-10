/**
 * The reverse backfill window — pure calendar/early-stop logic shared by the
 * CI safety guard and the backfill script.
 *
 * The walk ALWAYS starts at the most recent closed session and moves backward
 * one weekday at a time. Missing dates are guaranteed CONTIGUOUS: as soon as a
 * date exists in the stored history, the gap has bridged and scanning stops.
 */
import {
  DAY_MS,
  TAIPEI_OFFSET_MS,
  dayStampOf,
  stampToMs,
  todOf
} from '@odf/screener-worker/session'

/** Today is a usable session only after the stock close + EOD publish lag
 *  (14:00 Taipei); before that the walk starts at the previous weekday. */
export const SESSION_FINAL_TOD_MS = 48_600_000 + 30 * 60_000 // 13:30 + 30 min

const isWeekday = (ms: number): boolean => {
  const wd = new Date(ms + TAIPEI_OFFSET_MS).getUTCDay()
  return wd !== 0 && wd !== 6
}

/** The most recent closed weekday at/before `nowMs` (Taipei midnight ms). */
export function latestClosedSessionMs(nowMs: number): number {
  const today = stampToMs(dayStampOf(nowMs))
  if (today > 0 && isWeekday(today) && todOf(nowMs) >= SESSION_FINAL_TOD_MS) return today
  let ms = today > 0 ? today : stampToMs(dayStampOf(Date.now()))
  do {
    ms -= DAY_MS
  } while (!isWeekday(ms))
  return ms
}

/** Weekdays from `fromMs` down to `floorMs`, newest first. */
export function reverseWeekdays(fromMs: number, floorMs: number): number[] {
  const out: number[] = []
  for (let ms = fromMs; ms >= floorMs; ms -= DAY_MS) {
    if (isWeekday(ms)) out.push(ms)
  }
  return out
}

export type GapStop = 'bridged' | 'before-listing' | 'quota' | 'continue'

export interface GapState {
  /** Stamps already present in the stored series. */
  existingDates: ReadonlySet<string>
  /** The symbol's listing day (Taipei midnight), 0 when unknown. */
  listedMs: number
  /** Market sessions collected so far for an absent series. */
  collected: number
  /** Per-symbol session quota (3 years); 0 disables the quota. */
  quota: number
}

/** Decide whether a symbol still needs this date. */
export function gapStop(stamp: string, dateMs: number, state: GapState): GapStop {
  if (state.existingDates.has(stamp)) return 'bridged'
  if (state.listedMs > 0 && dateMs < state.listedMs) return 'before-listing'
  if (state.quota > 0 && state.collected >= state.quota) return 'quota'
  return 'continue'
}
