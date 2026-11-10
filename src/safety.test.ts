import { describe, expect, test } from 'bun:test'
import { SC, SCREENER_FIELDS, type ScreenerRow } from '@odf/screener-worker/screener'
import { DAY_MS, stampToMs } from '@odf/screener-worker/session'
import { classifyBarDay, findMissingHistories } from './safety'

const session = stampToMs('20261104')

function row(symbol: string, listed: string | null, volume = 1000): ScreenerRow {
  const out: ScreenerRow = SCREENER_FIELDS.map(() => null)
  out[SC.symbol] = symbol
  out[SC.listedDate] = listed
  out[SC.volume] = volume
  return out
}

describe('classifyBarDay', () => {
  test('both empty is a holiday/typhoon closure, not a zero session', () => {
    expect(classifyBarDay(false, false)).toBe('closed')
  })

  test('both traded is a session', () => {
    expect(classifyBarDay(true, true)).toBe('session')
  })

  test('one empty is classified per market, not as a whole-day hole', () => {
    expect(classifyBarDay(false, true)).toBe('twse-missing')
    expect(classifyBarDay(true, false)).toBe('tpex-missing')
  })
})

describe('findMissingHistories', () => {
  test('flags an absent history for an established listing', () => {
    const out = findMissingHistories([row('2330', '19940905')], new Map(), session)
    expect(out).toEqual([{ symbol: '2330', reason: 'absent' }])
  })

  test('flags a stale history whose newest date is behind the session', () => {
    const last = new Map([['2330', stampToMs('20261103')]])
    expect(findMissingHistories([row('2330', '19940905')], last, session)).toEqual([
      { symbol: '2330', reason: 'stale 20261103' }
    ])
  })

  test('a no-trade stale history is NOT chased: there is no session to append', () => {
    const last = new Map([['020001', stampToMs('20261103')]])
    expect(findMissingHistories([row('020001', '20040609', 0)], last, session)).toEqual([])
  })

  test('a no-trade row with an ABSENT history is still a real loss', () => {
    expect(findMissingHistories([row('020001', '20040609', 0)], new Map(), session)).toEqual([
      { symbol: '020001', reason: 'absent' }
    ])
  })

  test('a short but CURRENT history needs no repair', () => {
    const last = new Map([['00999', stampToMs('20261104')]])
    expect(findMissingHistories([row('00999', null)], last, session)).toEqual([])
  })

  test('exempts a new listing with no history', () => {
    const rows = [row('9999', '20261101')]
    expect(findMissingHistories(rows, new Map(), session)).toEqual([])
  })

  test('a new listing with a recent gap is still repaired', () => {
    const last = new Map([['9999', stampToMs('20261103')]])
    const rows = [row('9999', '20261101')]
    expect(findMissingHistories(rows, last, session)).toEqual([
      { symbol: '9999', reason: 'stale 20261103' }
    ])
  })

  test('a missing listedDate is treated as an established symbol', () => {
    const rows = [row('0050', null)]
    expect(findMissingHistories(rows, new Map(), session)).toEqual([
      { symbol: '0050', reason: 'absent' }
    ])
  })

  test('the 45-day listing window is boundary-inclusive', () => {
    const rows = [row('9999', '20260920')] // 45 days before 2026-11-04
    expect(findMissingHistories(rows, new Map(), session)).toEqual([])
    const older = [row('9999', '20260919')]
    expect(findMissingHistories(older, new Map(), session)).toEqual([
      { symbol: '9999', reason: 'absent' }
    ])
  })

  test('a future listing date is not treated as new', () => {
    const rows = [row('9999', '20261110')]
    expect(findMissingHistories(rows, new Map(), session + 0 * DAY_MS)).toHaveLength(1)
  })
})
