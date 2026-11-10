import { describe, expect, test } from 'bun:test'
import { SCREENER_FIELDS, type ScreenerHistory } from '@odf/screener-worker/screener'
import { mergeHistorySeries } from './historyMerge'

function history(dates: string[], close: number[]): ScreenerHistory {
  const h = {
    symbol: '2330',
    dates,
    open: [],
    high: [],
    low: [],
    close,
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
  } as unknown as ScreenerHistory
  return h
}

describe('mergeHistorySeries', () => {
  test('prepends repaired dates, keeps older stored dates, sorts', () => {
    const prior = history(['20261102', '20261103'], [10, 11])
    const next = history(['20261104', '20261105'], [12, 13])
    const merged = mergeHistorySeries(prior, next) as ScreenerHistory
    expect(merged.dates).toEqual(['20261102', '20261103', '20261104', '20261105'])
    expect(merged.close).toEqual([10, 11, 12, 13])
  })

  test('a same-date repaired row wins and never duplicates', () => {
    const prior = history(['20261103', '20261104'], [10, 11])
    const next = history(['20261104', '20261105'], [99, 12])
    const merged = mergeHistorySeries(prior, next) as ScreenerHistory
    expect(merged.dates).toEqual(['20261103', '20261104', '20261105'])
    expect(merged.close).toEqual([10, 99, 12])
    expect(new Set(merged.dates).size).toBe(merged.dates.length)
  })

  test('caps to the requested horizon', () => {
    const prior = history(['20261101', '20261102'], [1, 2])
    const next = history(['20261103', '20261104'], [3, 4])
    const merged = mergeHistorySeries(prior, next, 3) as ScreenerHistory
    expect(merged.dates).toEqual(['20261102', '20261103', '20261104'])
    expect(SCREENER_FIELDS.length).toBeGreaterThan(0)
  })

  test('carries published quarterly/monthly fields across a repaired gap', () => {
    const prior = history(['20261102', '20261103'], [10, 11])
    prior.grossMargin = [20, 21]
    prior.revenueYoy = [5, 6]
    const next = history(['20261104', '20261105'], [12, 13])
    const merged = mergeHistorySeries(prior, next) as ScreenerHistory
    expect(merged.grossMargin).toEqual([20, 21, 21, 21])
    expect(merged.revenueYoy).toEqual([5, 6, 6, 6])
  })

  test('null sides pass through', () => {
    const h = history(['20261104'], [12])
    expect(mergeHistorySeries(null, h)).toBe(h)
    expect(mergeHistorySeries(h, null)).toBe(h)
  })
})
