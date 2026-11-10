import { describe, expect, test } from 'bun:test'
import { DAY_MS, stampToMs } from '@odf/screener-worker/session'
import {
  SESSION_FINAL_TOD_MS,
  gapStop,
  latestClosedSessionMs,
  reverseWeekdays
} from './backfillWindow'

const taipei = (y: number, m: number, d: number, tod = 0): number =>
  Date.UTC(y, m - 1, d) - 8 * 3_600_000 + tod
const stamp = (ms: number): string => {
  const d = new Date(ms + 8 * 3_600_000)
  return `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, '0')}${String(d.getUTCDate()).padStart(2, '0')}`
}

describe('latestClosedSessionMs', () => {
  test('before the final publish it starts at the previous weekday', () => {
    // Tuesday 2026-11-10 13:00 Taipei.
    expect(latestClosedSessionMs(taipei(2026, 11, 10, 13 * 3_600_000))).toBe(taipei(2026, 11, 9))
  })

  test('after the final publish today is the starting day', () => {
    expect(latestClosedSessionMs(taipei(2026, 11, 10, SESSION_FINAL_TOD_MS + 1))).toBe(
      taipei(2026, 11, 10)
    )
  })

  test('a weekend starts at Friday', () => {
    // Saturday 2026-11-14 10:00.
    expect(latestClosedSessionMs(taipei(2026, 11, 14, 10 * 3_600_000))).toBe(taipei(2026, 11, 13))
  })
})

describe('reverseWeekdays', () => {
  test('walks backward and skips weekends', () => {
    const days = reverseWeekdays(taipei(2026, 11, 9), taipei(2026, 11, 2))
    expect(days.map(stamp)).toEqual([
      '20261109',
      '20261106',
      '20261105',
      '20261104',
      '20261103',
      '20261102'
    ])
  })

  test('stops at the floor', () => {
    const days = reverseWeekdays(taipei(2026, 11, 9), taipei(2026, 11, 9))
    expect(days.map(stamp)).toEqual(['20261109'])
    expect(reverseWeekdays(taipei(2026, 11, 9), taipei(2026, 11, 10))).toEqual([])
  })
})

describe('gapStop', () => {
  const existing = new Set(['20261103'])

  test('bridged wins first', () => {
    expect(
      gapStop('20261103', taipei(2026, 11, 3), {
        existingDates: existing,
        listedMs: 0,
        collected: 0,
        quota: 780
      })
    ).toBe('bridged')
  })

  test('before the listing day stops without a fetch', () => {
    expect(
      gapStop('20261101', taipei(2026, 11, 1), {
        existingDates: existing,
        listedMs: taipei(2026, 11, 2),
        collected: 0,
        quota: 780
      })
    ).toBe('before-listing')
  })

  test('the listing day itself is included', () => {
    expect(
      gapStop('20261102', taipei(2026, 11, 2), {
        existingDates: existing,
        listedMs: taipei(2026, 11, 2),
        collected: 0,
        quota: 780
      })
    ).toBe('continue')
  })

  test('the session quota stops an absent series', () => {
    expect(
      gapStop('20261001', taipei(2026, 10, 1), {
        existingDates: existing,
        listedMs: 0,
        collected: 780,
        quota: 780
      })
    ).toBe('quota')
  })

  test('an unbridged date continues', () => {
    expect(
      gapStop('20261102', taipei(2026, 11, 2), {
        existingDates: existing,
        listedMs: 0,
        collected: 20,
        quota: 780
      })
    ).toBe('continue')
  })
})

describe('window arithmetic', () => {
  test('a floor at the 36-month horizon is inclusive', () => {
    const from = taipei(2026, 11, 9)
    const floor = from - 400 * DAY_MS
    expect(stampToMs(stamp(reverseWeekdays(from, floor).at(-1) as number))).toBeGreaterThanOrEqual(
      floor
    )
  })
})
