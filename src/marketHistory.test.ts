import { describe, expect, test } from 'bun:test'
import { buildMarketContext } from './etl'
import {
  buildMarketHistorySession,
  creditRows,
  mergeCappedMarketHistory,
  mergeHistoryPoints,
  newestCreditPoint
} from './marketHistory'
import { twseStockMarketCap } from './history'
import { SC, SCREENER_FIELDS, type ScreenerRow } from './screener'
import type { MarketContext, MarketDerivatives, MarketHistoryPoint } from './screener'
import type { CreditSummary, DayBar, MarginSrc, TpexCreditSummary } from './sources/types'

function bar(close: number): DayBar {
  return {
    name: 'test',
    open: close,
    high: close,
    low: close,
    close,
    change: 0,
    volume: 1,
    tickets: 1,
    tradeValue: 1,
    shares: 0
  }
}

function margin(marginBal: number, shortBal: number): MarginSrc {
  return {
    marginBal,
    shortBal,
    quota: 0,
    marginPrev: marginBal,
    marginBuy: 0,
    marginSell: 0,
    marginCashRepay: 0,
    shortQuota: 0,
    shortPrev: shortBal,
    shortSell: 0,
    shortCover: 0,
    shortCashRepay: 0,
    offset: 0,
    note: ''
  }
}

function summary(dateMs: number): CreditSummary {
  return {
    dateMs,
    margin: { buy: 1, sell: 1, repay: 0, prev: 2, bal: 3 },
    short: { buy: 1, sell: 1, repay: 0, prev: 2, bal: 3 },
    marginAmountNtd: 1_000_000,
    marginAmountBuyNtd: 1,
    marginAmountSellNtd: 1,
    marginAmountRepayNtd: 0,
    marginAmountPrevNtd: 1_000_000
  }
}

describe('historical session reads', () => {
  test('an empty payload is final and is not retried', async () => {
    let calls = 0
    const empty = async (): Promise<unknown> => {
      calls++
      return {
        ok: true,
        status: 200,
        json: async () => null,
        text: async () => 'null'
      }
    }
    const result = await buildMarketHistorySession(Date.UTC(2026, 10, 11), empty as never, null)
    // Seven required readers + the optional dashboard, each ONE request.
    expect(calls).toBe(8)
    expect(result.ok).toBe(false)
    if (!result.ok) expect(result.reason).toBe('no-session')
  })
})

describe('historical session request budget', () => {
  test('the TPEx dashboard is not requested before its 2026-09-10 start', async () => {
    let calls = 0
    const empty = async (): Promise<unknown> => {
      calls++
      return {
        ok: true,
        status: 200,
        json: async () => null,
        text: async () => 'null'
      }
    }
    const result = await buildMarketHistorySession(Date.UTC(2026, 8, 9), empty as never, null)
    expect(calls).toBe(7)
    expect(result.ok).toBe(false)
    if (!result.ok) expect(result.reason).toBe('no-session')
  })
})

describe('historical market credit rows', () => {
  test('joins only bars that also have a margin balance', () => {
    const bars = new Map([
      ['2330', bar(100_000)],
      ['2317', bar(20_000)]
    ])
    const margins = new Map([['2330', margin(2, 1)]])
    const rows = creditRows('TWSE', bars, margins)
    expect(rows).toHaveLength(1)
    const row = rows[0] as ScreenerRow
    expect(row).toHaveLength(SCREENER_FIELDS.length)
    expect(row[SC.market]).toBe('TWSE')
    expect(row[SC.symbol]).toBe('2330')
    expect(row[SC.close]).toBe(100_000)
    expect(row[SC.marginBal]).toBe(2)
    expect(row[SC.shortBal]).toBe(1)
  })

  test('the joined rows feed the same financed/maintenance definition', () => {
    const dateMs = 1_700_000_000_000
    const rows = creditRows(
      'TWSE',
      new Map([['2330', bar(100_000)]]),
      new Map([['2330', margin(2, 1)]])
    )
    const ctx = buildMarketContext(summary(dateMs), null, null, null, rows, dateMs)
    expect(ctx?.credit?.financedValueNtd).toBe(2 * 100_000 * 10)
    expect(ctx?.credit?.loanBalanceNtd).toBe(1_000_000)
    expect(ctx?.credit?.maintenanceRatio).toBeCloseTo(200, 6)
  })
})

describe('market history point merge', () => {
  const dateMs = 1_700_000_000_000
  const creditDateMs = dateMs - 86_400_000
  const rows = creditRows(
    'TWSE',
    new Map([['2330', bar(100_000)]]),
    new Map([['2330', margin(2, 1)]])
  )
  const credit = buildMarketContext(
    summary(creditDateMs),
    null,
    null,
    null,
    rows,
    creditDateMs
  )!.credit!
  const derivatives: MarketDerivatives = {
    asOf: dateMs,
    institutions: [
      {
        item: 'Foreign',
        futuresLong: 1,
        futuresShort: 0,
        futuresNet: 1,
        futuresOiLong: 1,
        futuresOiShort: 0,
        futuresOiNet: 1,
        optionsLong: 0,
        optionsShort: 0,
        optionsNet: 0,
        optionsOiLong: 0,
        optionsOiShort: 0,
        optionsOiNet: 0
      }
    ],
    futures: [],
    options: [],
    largeTraders: [],
    pcr: [],
    vix: null
  }
  test('a credit point and a derivatives-only point for one date become one observation', () => {
    const merged = mergeHistoryPoints([
      { sessionDate: dateMs, credit: null, derivatives },
      { sessionDate: dateMs, credit, derivatives: null }
    ])
    expect(merged).toHaveLength(1)
    expect(merged[0]!.credit).toBe(credit)
    expect(merged[0]!.derivatives).toBe(derivatives)
  })
  test('the top-level anchor is the newest CREDIT-bearing point', () => {
    const newest = newestCreditPoint([
      { sessionDate: dateMs, credit: null, derivatives },
      { sessionDate: creditDateMs, credit, derivatives: null }
    ])
    expect(newest?.sessionDate).toBe(creditDateMs)
  })
  test('a newer derivatives-only point does not consume a rolling slot', () => {
    const day = 86_400_000
    const points: MarketHistoryPoint[] = Array.from({ length: 60 }, (_, i) => ({
      sessionDate: dateMs - i * day,
      credit,
      derivatives: null
    }))
    const future: MarketHistoryPoint = {
      sessionDate: dateMs + day,
      credit: null,
      derivatives
    }
    const current: MarketContext = { asOf: dateMs, credit, derivatives: null }
    const history = mergeCappedMarketHistory([future, ...points], current)
    expect(history).toHaveLength(60)
    expect(history[0]!.sessionDate).toBe(dateMs - 59 * day)
    expect(history.at(-1)!.sessionDate).toBe(dateMs)
  })
})

describe('symmetric whole-market fields', () => {
  test('TWSE stock cap uses issued shares and excludes TDR/ETF', () => {
    const foreign = new Map([
      ['2330', { shares: 1, ratio: 50, issuedShares: 2_000 }],
      ['0050', { shares: 1, ratio: 50, issuedShares: 1_000 }],
      ['910322', { shares: 1, ratio: 50, issuedShares: 1_000 }]
    ])
    const bars = new Map([
      ['2330', bar(100_000)],
      ['0050', bar(20_000)],
      ['910322', bar(10_000)]
    ])
    expect(twseStockMarketCap(foreign, bars)).toBe(2_000_000)
  })

  test('a missing half of the aggregate cap falls back to BOTH row caps', () => {
    const dateMs = 1_700_000_000_000
    const rows = [
      ...creditRows('TWSE', new Map([['2330', bar(100_000)]]), new Map([['2330', margin(2, 1)]])),
      ...creditRows('TPEX', new Map([['6488', bar(50_000)]]), new Map([['6488', margin(1, 1)]]))
    ]
    const tpex: TpexCreditSummary = {
      dateMs,
      marginBalanceNtd: 100,
      shortBalanceLots: 10,
      creditAmountNtd: 20,
      marketCapNtd: 500
    }
    const ctx = buildMarketContext(
      summary(dateMs),
      tpex,
      { dateMs, keepRate: 180, creditAmountNtd: 30 },
      null,
      rows,
      dateMs,
      null,
      [],
      {
        twseMarketCapNtd: 1_000,
        tpexMarketCapNtd: null,
        twseTotalAmountNtd: 300,
        twseStockTotalAmountNtd: 200,
        twseCreditAmountNtd: 30,
        tpexTotalAmountNtd: 70,
        tpexStockTotalAmountNtd: 60,
        tpexCreditAmountNtd: 20
      }
    )
    // The rows carry no market-cap cell, so the symmetric fallback is empty;
    // the important part is that a TWSE-only 1,000 is never published as the
    // whole-market denominator.
    expect(ctx?.credit?.marketCapNtd).toBeNull()
  })

  test('buildMarketContext carries both with/without TDR-ETF totals', () => {
    const dateMs = 1_700_000_000_000
    const rows = creditRows(
      'TWSE',
      new Map([['2330', bar(100_000)]]),
      new Map([['2330', margin(2, 1)]])
    )
    const tpex: TpexCreditSummary = {
      dateMs,
      marginBalanceNtd: 100,
      shortBalanceLots: 10,
      creditAmountNtd: 20,
      marketCapNtd: 500
    }
    const ctx = buildMarketContext(
      summary(dateMs),
      tpex,
      { dateMs, keepRate: 180, creditAmountNtd: 30 },
      null,
      rows,
      dateMs,
      null,
      [],
      {
        twseMarketCapNtd: 1_000,
        twseTotalAmountNtd: 300,
        twseStockTotalAmountNtd: 200,
        twseCreditAmountNtd: 30,
        tpexMarketCapNtd: 500,
        tpexTotalAmountNtd: 70,
        tpexStockTotalAmountNtd: 60,
        tpexCreditAmountNtd: 20
      }
    )
    const c = ctx?.credit
    expect(c?.marketCapNtd).toBe(1_500)
    expect(c?.totalAmountNtd).toBe(370)
    expect(c?.stockTotalAmountNtd).toBe(260)
    expect(c?.creditAmountNtd).toBe(50)
    expect(c?.twseMarketCapNtd).toBe(1_000)
    expect(c?.tpexMarketCapNtd).toBe(500)
    expect(c?.twseStockTotalAmountNtd).toBe(200)
    expect(c?.tpexStockTotalAmountNtd).toBe(60)
  })
})
