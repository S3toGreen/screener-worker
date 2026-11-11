import { describe, expect, test } from 'bun:test'
import {
  SC,
  SCREENER_FIELDS,
  SCREENER_HISTORY_FIELDS,
  SCREENER_HISTORY_SESSIONS,
  type ScreenerHistory,
  type ScreenerRow
} from '@odf/screener-worker/screener'
import { stampToMs } from '@odf/screener-worker/session'
import {
  anchorDaily,
  buildMarketContext,
  buildRow,
  lateRepairWindow,
  mergeSession,
  perDateFirst,
  preserveRowCells,
  probeDays,
  readSafe,
  vwapOf,
  type HistoryValues,
  type Seed
} from './etl'
import {
  sourced,
  type CreditSummary,
  type DailyRow,
  type Sourced,
  type TpexCreditSummary,
  type ValuationRow
} from './sources/types'

function emptyHistory(symbol = '2330'): ScreenerHistory {
  const h = { symbol, dates: [] } as unknown as ScreenerHistory
  const write = h as unknown as Record<string, unknown[]>
  for (const field of SCREENER_HISTORY_FIELDS) write[field] = []
  return h
}

function values(patch: Partial<HistoryValues> = {}): HistoryValues {
  const out = {} as HistoryValues
  for (const field of SCREENER_HISTORY_FIELDS) out[field] = null
  out.close = 100_000
  return { ...out, ...patch }
}

describe('mergeSession — duplicate daily updates', () => {
  test('a new session appends every column', () => {
    const h = emptyHistory()
    mergeSession(h, '20261103', values(), { preserved: 0 })
    expect(h.dates).toEqual(['20261103'])
    expect(h.close).toEqual([100_000])
    expect(h.per).toEqual([null])
  })

  test('a same-session re-run REPLACES a finite cell', () => {
    const h = emptyHistory()
    mergeSession(h, '20261103', values({ close: 100_000, per: 12 }), { preserved: 0 })
    const stats = { preserved: 0 }
    mergeSession(h, '20261103', values({ close: 101_000, per: 13 }), stats)
    expect(h.dates.length).toBe(1)
    expect(h.close).toEqual([101_000])
    expect(h.per).toEqual([13])
    expect(stats.preserved).toBe(0)
  })

  test('a null re-read NEVER erases a published value (one-way carry)', () => {
    const h = emptyHistory()
    mergeSession(h, '20261103', values({ close: 100_000, per: 12, grossMargin: 55 }), {
      preserved: 0
    })
    const stats = { preserved: 0 }
    mergeSession(h, '20261103', values({ close: 100_000, per: null, grossMargin: null }), stats)
    expect(h.per).toEqual([12])
    expect(h.grossMargin).toEqual([55])
    expect(stats.preserved).toBe(2)
  })

  test('a null over a null stays null', () => {
    const h = emptyHistory()
    mergeSession(h, '20261103', values({ per: null }), { preserved: 0 })
    const stats = { preserved: 0 }
    mergeSession(h, '20261103', values({ per: null }), stats)
    expect(h.per).toEqual([null])
    expect(stats.preserved).toBe(0)
  })

  test('out-of-order back-dated repairs keep every column aligned', () => {
    const h = emptyHistory()
    mergeSession(h, '20261103', values({ close: 100 }), { preserved: 0 })
    mergeSession(h, '20261105', values({ close: 300 }), { preserved: 0 })
    // A repair for a session the file skipped.
    mergeSession(h, '20261104', values({ close: 200 }), { preserved: 0 })
    expect(h.dates).toEqual(['20261103', '20261104', '20261105'])
    expect(h.close).toEqual([100, 200, 300])
  })

  test('retention drops the oldest sessions and their cells', () => {
    const h = emptyHistory()
    // One over the cap: the append must trim to the retention.
    for (let i = 0; i < SCREENER_HISTORY_SESSIONS; i++) {
      const day = String(i + 1).padStart(8, '0')
      h.dates.push(day)
      for (const field of SCREENER_HISTORY_FIELDS) {
        ;(h as unknown as Record<string, unknown[]>)[field].push(field === 'close' ? i : null)
      }
    }
    mergeSession(h, '99999999', values({ close: 7 }), { preserved: 0 })
    expect(h.dates.length).toBe(SCREENER_HISTORY_SESSIONS)
    expect(h.dates[0]).toBe('00000002')
    expect(h.dates[h.dates.length - 1]).toBe('99999999')
    expect(h.close.length).toBe(SCREENER_HISTORY_SESSIONS)
    expect(h.close[h.close.length - 1]).toBe(7)
  })
})

describe('vwapOf — the cold-start margin-cost prior', () => {
  test('Σ成交金額 ÷ Σ成交股數 over the newest N traded sessions', () => {
    const h = emptyHistory()
    h.dates = ['20261029', '20261030', '20261102', '20261103']
    h.volume = [1_000, 0, 2_000, 3_000]
    h.tradeValue = [50_000, 0, 130_000, 210_000]
    // The newest 3 sessions: the halted (0-volume) day is skipped, not 0-weighted.
    expect(vwapOf(h, 3)).toBeCloseTo(340_000 / 5_000, 10)
    expect(vwapOf(h, 1)).toBeCloseTo(70, 10)
    expect(vwapOf(h, 0)).toBeNull()
    expect(vwapOf(emptyHistory(), 60)).toBeNull()
  })
})

describe('buildMarketContext — the separate market object', () => {
  const session = 1_793_635_200_000
  const summary = (dateMs = session): CreditSummary => ({
    dateMs,
    margin: { buy: 100, sell: 40, repay: 10, prev: 500, bal: 550 },
    short: { buy: 5, sell: 20, repay: 1, prev: 50, bal: 64 },
    // The announced loan balance (NT$50 M) — the estimate's denominator.
    marginAmountNtd: 50_000_000,
    // The same row's flows (kept, never dropped).
    marginAmountBuyNtd: 4_000_000,
    marginAmountSellNtd: 3_000_000,
    marginAmountRepayNtd: 1_000_000,
    marginAmountPrevNtd: 50_000_000
  })
  const tpexSummary = (patch: Partial<TpexCreditSummary> = {}): TpexCreditSummary => ({
    dateMs: session,
    marginBalanceNtd: 20_000_000,
    shortBalanceLots: 4_000,
    creditAmountNtd: 3_000_000,
    marketCapNtd: 5_000_000,
    ...patch
  })
  const row = (
    market: 'TWSE' | 'TPEX',
    patch: Partial<Record<ScreenerFieldKey, number>> = {}
  ): ScreenerRow => {
    const out: ScreenerRow = SCREENER_FIELDS.map(() => null)
    out[SC.symbol] = market === 'TWSE' ? '2330' : '2618'
    out[SC.market] = market
    out[SC.close] = market === 'TWSE' ? 100_000 : 50_000 // NT$1 000 / NT$500
    out[SC.marketCap] = market === 'TWSE' ? 10_000_000 : 1_000_000
    out[SC.tradeValue] = 2_000_000
    out[SC.marginBal] = 100 // lots
    out[SC.shortBal] = 10
    for (const [key, value] of Object.entries(patch)) out[SC[key as ScreenerFieldKey]] = value
    return out
  }
  const derivatives = {
    asOf: session,
    institutions: [],
    futures: [],
    options: [],
    largeTraders: [],
    pcr: [],
    vix: null
  }
  const official = { dateMs: session, keepRate: 165.53 }

  test('both halves absent is no object', () => {
    expect(buildMarketContext(null, null, null, null, [], session)).toBeNull()
  })

  test('a stale credit summary is never stamped as the session', () => {
    expect(
      buildMarketContext(summary(session - 86_400_000), null, null, null, [], session)
    ).toBeNull()
  })

  test("the exchange's own ratio is carried in its OWN slot", () => {
    const ctx = buildMarketContext(summary(), null, official, null, [row('TWSE')], session)
    expect(ctx?.asOf).toBe(session)
    expect(ctx?.credit?.officialMaintenanceRatio).toBe(165.53)
    // The 上市 balance in MONEY is its own cell (the lots row is 交易單位).
    expect(ctx?.credit?.twseMarginBalanceNtd).toBe(50_000_000)
    // Our estimate is a separate number: 100 lots × NT$1 000 × 1000 ÷ NT$50 M.
    expect(ctx?.credit?.maintenanceRatio).toBeCloseTo(200, 6)
    expect(ctx?.credit?.maintenanceRatioKind).toBe('estimate')
    expect(ctx?.credit?.scope).toBe('TWSE')
    // The note names the DEFINITION (融資維持率 estimate), never the official one.
    expect(ctx?.credit?.maintenanceRatioNote).toContain('融資維持率 (est.)')
  })

  test('the per-stock totals fill the OTC block and the TWSE money columns survive', () => {
    const totals = {
      marginBal: 2_250_000,
      marginPrev: 2_240_000,
      marginBuy: 1_000,
      marginSell: 900,
      marginCashRepay: 50,
      shortBal: 250_000,
      shortPrev: 249_000,
      shortSell: 500,
      shortCover: 400,
      shortCashRepay: 10
    }
    const ctx = buildMarketContext(
      summary(),
      tpexSummary(),
      official,
      null,
      [row('TWSE'), row('TPEX')],
      session,
      totals
    )
    expect(ctx?.credit?.tpexMarginBalanceLots).toBe(2_250_000)
    expect(ctx?.credit?.tpexMarginChangeLots).toBe(10_000)
    expect(ctx?.credit?.tpexMarginBuyLots).toBe(1_000)
    expect(ctx?.credit?.tpexShortCashRepayLots).toBe(10)
    // The 上市 融資金額 row's other columns (not just the balance).
    expect(ctx?.credit?.twseMarginBalanceNtd).toBe(50_000_000)
    expect(ctx?.credit?.twseMarginBuyNtd).toBe(4_000_000)
    expect(ctx?.credit?.twseMarginPrevNtd).toBe(50_000_000)
  })

  test('a stale official ratio is NULL, never the previous session', () => {
    const stale = { dateMs: session - 86_400_000, keepRate: 165.53 }
    const ctx = buildMarketContext(summary(), null, stale, null, [row('TWSE')], session)
    expect(ctx?.credit?.officialMaintenanceRatio).toBeNull()
  })

  test('a stale TPEx summary is never joined to the anchor', () => {
    const stale = tpexSummary({ dateMs: session - 86_400_000 })
    const ctx = buildMarketContext(
      summary(),
      stale,
      official,
      null,
      [row('TWSE'), row('TPEX')],
      session
    )
    expect(ctx?.credit?.scope).toBe('TWSE')
    // No OTC loan, credit amount or market cap from the prior session.
    expect(ctx?.credit?.tpexMarginBalanceNtd).toBeNull()
    expect(ctx?.credit?.tpexCreditAmountNtd).toBeNull()
    expect(ctx?.credit?.tpexMarketCapNtd).toBeNull()
    expect(ctx?.credit?.financedValueNtd).toBe(100_000_000)
  })

  test('a carried market never enters the anchored credit aggregate', () => {
    const ctx = buildMarketContext(
      summary(),
      tpexSummary(),
      official,
      null,
      [row('TWSE'), row('TPEX')],
      session,
      null,
      ['TPEX']
    )
    // The stale TPEx row's financed value/cap is excluded even though its
    // summary is current.
    expect(ctx?.credit?.scope).toBe('TWSE')
    expect(ctx?.credit?.financedValueNtd).toBe(100_000_000)
    expect(ctx?.credit?.marketCapNtd).toBe(10_000_000)
    expect(ctx?.credit?.maintenanceRatio).toBeCloseTo(200, 6)
  })

  test('the 上市+上櫃 ratio joins the announced loan balances', () => {
    const ctx = buildMarketContext(
      summary(),
      tpexSummary(),
      official,
      null,
      [row('TWSE'), row('TPEX')],
      session
    )
    expect(ctx?.credit?.scope).toBe('TWSE+TPEX')
    // financed 100 M (TWSE) + 50 M (TPEx); loans 50 M + 20 M.
    expect(ctx?.credit?.financedValueNtd).toBe(150_000_000)
    expect(ctx?.credit?.loanBalanceNtd).toBe(70_000_000)
    expect(ctx?.credit?.maintenanceRatio).toBeCloseTo((150 / 70) * 100, 6)
    expect(ctx?.credit?.tpexMarginBalanceLots).toBe(100)
    expect(ctx?.credit?.tpexShortBalanceLots).toBe(10)
    expect(ctx?.credit?.tpexMarginBalanceNtd).toBe(20_000_000)
    expect(ctx?.credit?.marketCapNtd).toBe(11_000_000)
    expect(ctx?.credit?.marginToMarketCap).toBeCloseTo((70 / 11) * 100, 6)
  })

  test('without the TPEx money summary the ratio stays TWSE-only', () => {
    const ctx = buildMarketContext(summary(), null, null, null, [row('TWSE'), row('TPEX')], session)
    expect(ctx?.credit?.scope).toBe('TWSE')
    // The TPEx row's financed value must NOT enter a TWSE-denominated ratio.
    expect(ctx?.credit?.maintenanceRatio).toBeCloseTo(200, 6)
    expect(ctx?.credit?.tpexMarginBalanceLots).toBe(100)
    expect(ctx?.credit?.tpexMarginBalanceNtd).toBeNull()
  })

  test('the flows are the published units', () => {
    const ctx = buildMarketContext(summary(), null, official, null, [row('TWSE')], session)
    expect(ctx?.credit?.twseMarginChangeLots).toBe(50)
    expect(ctx?.credit?.twseShortChangeLots).toBe(14)
    expect(ctx?.credit?.twseMarginBuyLots).toBe(100)
  })

  test('a missing loan amount leaves BOTH ratios null (no fake denominator)', () => {
    const noAmount = { ...summary(), marginAmountNtd: NaN }
    const ctx = buildMarketContext(noAmount, null, official, null, [row('TWSE')], session)
    expect(ctx?.credit?.maintenanceRatio).toBeNull()
    expect(ctx?.credit?.marginToMarketCap).toBeNull()
    expect(ctx?.credit?.officialMaintenanceRatio).toBe(165.53)
  })

  test('derivatives alone still produce an object (credit null)', () => {
    const withPcr = {
      ...derivatives,
      pcr: [
        {
          dateMs: session,
          putVolume: 1,
          callVolume: 1,
          volumeRatio: 100,
          putOi: null,
          callOi: null,
          oiRatio: null
        }
      ]
    }
    const ctx = buildMarketContext(null, null, null, withPcr, [], session)
    expect(ctx?.asOf).toBe(session)
    expect(ctx?.credit).toBeNull()
    expect(ctx?.derivatives?.pcr.length).toBe(1)
  })
})

type ScreenerFieldKey = (typeof SCREENER_FIELDS)[number]

describe('lateRepairWindow — the evening catch-up', () => {
  const session = stampToMs('20261104')

  test('same session at/after 20:00 Taipei is a repair pass', () => {
    // 22:00 Taipei.
    expect(lateRepairWindow(Date.UTC(2026, 10, 4, 14), session)).toBe(true)
  })

  test('earlier the same day, or the next day, is not', () => {
    // 18:00 Taipei (first run).
    expect(lateRepairWindow(Date.UTC(2026, 10, 4, 10), session)).toBe(false)
    // 03:45 Taipei the NEXT day IS the second repair slot.
    expect(lateRepairWindow(Date.UTC(2026, 10, 4, 19, 45), session)).toBe(true)
    // 03:45 two days later is outside the session's fast-path window.
    expect(lateRepairWindow(Date.UTC(2026, 10, 5, 19, 45), session)).toBe(false)
    expect(lateRepairWindow(0, session)).toBe(false)
  })
})

describe('preserveRowCells — same-session re-run safety', () => {
  test('fills only null/empty cells; finite/new values win', () => {
    const next = [null, '', 0, 5, 'new', null] as ScreenerRow
    const prior = ['old', 'prior', 3, null, 'old', 'kept'] as ScreenerRow
    expect(preserveRowCells(next, prior)).toEqual(['old', 'prior', 0, 5, 'new', 'kept'])
  })
})

describe('probeDays — the rwd anchor walk', () => {
  test('today first, weekends skipped, the newest five weekdays', () => {
    // 2026-11-05 02:00 Taipei (Thursday).
    const now = Date.UTC(2026, 10, 4, 18)
    expect(probeDays(now)).toEqual([
      stampToMs('20261105'),
      stampToMs('20261104'),
      stampToMs('20261103'),
      stampToMs('20261102'),
      stampToMs('20261030')
    ])
  })

  test('a Saturday starts at Friday', () => {
    // 2026-11-07 02:00 Taipei.
    const now = Date.UTC(2026, 10, 6, 18)
    expect(probeDays(now, 2)).toEqual([stampToMs('20261106'), stampToMs('20261105')])
  })

  test('no clock is no walk', () => {
    expect(probeDays(0)).toEqual([])
  })
})

describe('perDateFirst — rwd before the OpenAPI mirror', () => {
  const day = stampToMs('20261104')
  const mirror = (dateMs = day, rows = 1): Sourced<ValuationRow> => ({
    dateMs,
    rows,
    data: new Map([['2330', { per: 99, divYield: 9, pbr: 9 }]])
  })
  const rows = (n: number, per = 10): Map<string, ValuationRow> =>
    new Map(Array.from({ length: n }, (_, i) => [String(2300 + i), { per, divYield: 1, pbr: 2 }]))

  test('the exchange per-date table wins even when the mirror holds the anchor', async () => {
    const degraded = new Set<string>()
    const late = new Set<string>()
    let reads = 0
    let mirrorReads = 0
    const out = await perDateFirst(
      'twse.valuation',
      async () => {
        mirrorReads++
        return mirror()
      },
      day,
      async () => {
        reads++
        return rows(2)
      },
      degraded,
      late
    )
    expect(reads).toBe(1)
    // The mirror is a LAZY fallback: an answered per-date read costs ONE request.
    expect(mirrorReads).toBe(0)
    expect(out?.dateMs).toBe(day)
    expect(out?.rows).toBe(2)
    expect(out?.data.get('2300')?.per).toBe(10)
    expect(degraded.size).toBe(0)
    expect(late.size).toBe(0)
  })

  test('a missing per-date read falls back to a mirror dated to the anchor', async () => {
    const degraded = new Set<string>()
    const late = new Set<string>()
    const stale = { ...mirror(), rows: 1 }
    const out = await perDateFirst(
      'tpex.margin',
      async () => stale,
      day,
      async () => null,
      degraded,
      late
    )
    expect(out).toBe(stale)
    expect(degraded.size).toBe(0)
    expect(late.size).toBe(0)
  })

  test('a mirror from ANOTHER session is never stamped (moves to `late`)', async () => {
    const degraded = new Set<string>()
    const late = new Set<string>()
    const out = await perDateFirst(
      'twse.valuation',
      async () => mirror(day - 86_400_000),
      day,
      async () => null,
      degraded,
      late
    )
    expect(out).toBeNull()
    // A clean "not out yet" is LATE — the later cron repairs it, no retry.
    expect([...late]).toEqual(['twse.valuation'])
    expect(degraded.size).toBe(0)
  })

  test('an undated mirror is the last resort', async () => {
    const degraded = new Set<string>()
    const late = new Set<string>()
    const out = await perDateFirst(
      'twse.sbl',
      async () => mirror(0),
      day,
      async () => null,
      degraded,
      late
    )
    expect(out?.dateMs).toBe(0)
    expect(degraded.size).toBe(0)
    expect(late.size).toBe(0)
  })

  test('nothing at all is a LATE source', async () => {
    const degraded = new Set<string>()
    const late = new Set<string>()
    const out = await perDateFirst('twse.margin', null, day, async () => null, degraded, late)
    expect(out).toBeNull()
    expect([...late]).toEqual(['twse.margin'])
    expect(degraded.size).toBe(0)
  })

  test('a FAILED per-date read is retried once, then degrades', async () => {
    const degraded = new Set<string>()
    const late = new Set<string>()
    let reads = 0
    const out = await perDateFirst(
      'twse.margin',
      null,
      day,
      async (): Promise<Map<string, never> | null> => {
        reads++
        throw new Error('HTTP 500')
      },
      degraded,
      late
    )
    expect(out).toBeNull()
    expect(reads).toBe(2)
    expect([...degraded]).toEqual(['twse.margin'])
    expect(late.size).toBe(0)
  })
})

describe('anchorDaily — the per-date table is the session of record', () => {
  const anchor = stampToMs('20261104')
  const daily = (rowsCount: number, dateMs = anchor): Sourced<DailyRow> => {
    const out = sourced<DailyRow>(dateMs)
    for (let i = 0; i < rowsCount; i++) {
      out.data.set(String(2300 + i), {
        symbol: String(2300 + i),
        name: `S${i}`,
        open: 100,
        high: 110,
        low: 90,
        close: 100,
        changePct: 1,
        volume: 1000,
        tradeValue: 100_000,
        transactions: 10
      })
    }
    out.rows = rowsCount
    return out
  }

  test("the probe's payload is used without a second read", async () => {
    let reads = 0
    const probe = daily(1000)
    const out = await anchorDaily(
      'TWSE',
      daily(1000),
      anchor,
      900,
      probe,
      async () => {
        reads++
        return null
      },
      new Set(),
      new Set()
    )
    expect(out).toBe(probe)
    expect(reads).toBe(0)
  })

  test('a probe for an OLDER date triggers one read for the anchor', async () => {
    const fresh = daily(1000)
    const degraded = new Set<string>()
    const out = await anchorDaily(
      'TWSE',
      daily(1000, anchor - 86_400_000),
      anchor,
      900,
      daily(1000, anchor - 172_800_000),
      async () => fresh,
      degraded,
      new Set()
    )
    expect(out).toBe(fresh)
    expect(degraded.size).toBe(0)
  })

  test('a no-trade row the per-date table omits is restored from the anchor mirror', async () => {
    const probe = daily(1000)
    const mirror = daily(1000)
    // The mirror lists a suspended name the per-date table has no row for.
    mirror.data.set('9999', {
      symbol: '9999',
      name: 'SUSPENDED',
      open: NaN,
      high: NaN,
      low: NaN,
      close: NaN,
      changePct: NaN,
      volume: 0,
      tradeValue: 0,
      transactions: 0
    })
    mirror.rows++
    const out = await anchorDaily(
      'TPEX',
      mirror,
      anchor,
      700,
      probe,
      async () => null,
      new Set(),
      new Set()
    )
    expect(out?.rows).toBe(1001)
    expect(out?.data.get('9999')?.name).toBe('SUSPENDED')
    // The per-date value wins for a symbol both carry.
    expect(out?.data.get('2300')?.name).toBe('S0')
  })

  test('a per-date miss falls back to a mirror holding the anchor', async () => {
    const mirror = daily(1000)
    const degraded = new Set<string>()
    const out = await anchorDaily(
      'TPEX',
      mirror,
      anchor,
      700,
      null,
      async () => null,
      degraded,
      new Set()
    )
    expect(out).toBe(mirror)
    expect(degraded.size).toBe(0)
  })

  test('a truncated per-date table + a stale mirror degrades (token)', async () => {
    const degraded = new Set<string>()
    const staleMirror = daily(1000, anchor - 86_400_000)
    const out = await anchorDaily(
      'TWSE',
      staleMirror,
      anchor,
      900,
      null,
      async () => daily(12),
      degraded,
      new Set()
    )
    expect(out).toBe(staleMirror) // carried forward, never stamped on the anchor
    // A TRUNCATED table is a failure (not a publish-timing wait).
    expect([...degraded]).toEqual(['twse.daily'])
  })

  test('a clean "not out yet" daily is LATE, never a failure', async () => {
    const degraded = new Set<string>()
    const late = new Set<string>()
    const staleMirror = daily(1000, anchor - 86_400_000)
    const out = await anchorDaily(
      'TPEX',
      staleMirror,
      anchor,
      700,
      null,
      async () => null,
      degraded,
      late
    )
    expect(out).toBe(staleMirror)
    expect([...late]).toEqual(['tpex.daily'])
    expect(degraded.size).toBe(0)
  })
})

describe('buildRow — a same-stamp re-run keeps the KNOWN cells', () => {
  /** A seed whose live sources ALL failed this pass. */
  const seed = (): Seed => ({
    symbol: '2330',
    market: 'TWSE',
    name: '台積電',
    industry: null,
    par: 1000,
    paidIn: 2_593_000_000_000,
    daily: {
      symbol: '2330',
      name: '台積電',
      open: 229_500,
      high: 230_000,
      low: 225_000,
      close: 226_000,
      changePct: -1.52,
      volume: 53_879_371,
      tradeValue: 122_293_100_290,
      transactions: 100_000
    },
    valuation: null,
    ratios: null,
    revenue: null,
    margin: null,
    sbl: null,
    risk: null,
    inst: null,
    foreign: null,
    dayTrade: null,
    marginCost: null,
    bigHolder: 84.38,
    retail: 9.13,
    holders: 3_185_409,
    bigHolder4w: null,
    exDiv: null,
    listed: '19940905'
  })

  test('the row is exactly the contract width and identity order', () => {
    const h = emptyHistory()
    mergeSession(h, '20261104', values({ close: 100_000 }), { preserved: 0 })
    const row = buildRow(seed(), h, '20261104')
    expect(row.length).toBe(SCREENER_FIELDS.length)
    expect(row[SC.symbol]).toBe('2330')
    expect(row[SC.name]).toBe('台積電')
    expect(row[SC.market]).toBe('TWSE')
  })

  test('a missing trust-flow session does not reset the streak', () => {
    const h = emptyHistory()
    mergeSession(h, '20261101', values({ trust: 10 }), { preserved: 0 })
    mergeSession(h, '20261102', values({ trust: 20 }), { preserved: 0 })
    mergeSession(h, '20261103', values({ trust: null }), { preserved: 0 })
    const row = buildRow(seed(), h, '20261104')
    // The newest published trust flows are 20 and 10; the null session is a
    // gap, not a zero.
    expect(row[SC.instTrustStreak]).toBe(2)
  })

  test('the history cell fills a source that failed THIS pass', () => {
    const h = emptyHistory()
    mergeSession(
      h,
      '20261104',
      values({
        per: 26.2,
        pbr: 9.11,
        divYield: 0.97,
        marginBal: 35_144,
        marginQuota: 6_483_092,
        shortBal: 16,
        sblBal: 8_440_719,
        foreign: -48_824_612_860,
        trust: 8_396_270_640,
        dealer: 13_485_307_000,
        foreignRatio: 68.31,
        dayTradeShares: 11_008_000,
        riskFlags: 0
      }),
      { preserved: 0 }
    )
    const row = buildRow(seed(), h, '20261104')
    expect(row[SC.per]).toBe(26.2)
    expect(row[SC.pbr]).toBe(9.11)
    expect(row[SC.divYield]).toBe(0.97)
    expect(row[SC.marginBal]).toBe(35_144)
    expect(row[SC.marginQuota]).toBe(6_483_092)
    expect(row[SC.shortBal]).toBe(16)
    expect(row[SC.sblBal]).toBe(8_440_719)
    expect(row[SC.instForeign]).toBe(-48_824_612_860)
    expect(row[SC.foreignRatio]).toBe(68.31)
    expect(row[SC.dayTradeRatio]).toBeCloseTo((11_008_000 / 53_879_371) * 100, 6)
    expect(row[SC.riskFlags]).toBe(0)
  })

  test("a NEW session never borrows the previous session's cells", () => {
    const h = emptyHistory()
    mergeSession(
      h,
      '20261103',
      values({ per: 26.2, marginBal: 35_144, dayTradeShares: 11_008_000 }),
      { preserved: 0 }
    )
    const row = buildRow(seed(), h, '20261104')
    expect(row[SC.per]).toBeNull()
    expect(row[SC.marginBal]).toBeNull()
    expect(row[SC.dayTradeRatio]).toBeNull()
    // The price-derived values still come from THIS session's daily.
    expect(row[SC.close]).toBe(226_000)
    expect(row[SC.marketCap]).toBeCloseTo((2_593_000_000_000 / 1000) * 2260, 0)
  })

  test('a day-trade value above the session volume is ignored, never >100%', () => {
    const h = emptyHistory()
    const s = seed()
    s.dayTrade = 60_000_000
    const row = buildRow(s, h, '20261104')
    expect(row[SC.dayTradeRatio]).toBeNull()
  })
})

describe('readSafe — late vs failed', () => {
  test('a value passes through untouched', async () => {
    const degraded = new Set<string>()
    const late = new Set<string>()
    const out = await readSafe('twse.daily', degraded, late, async () => 42)
    expect(out).toBe(42)
    expect(degraded.size + late.size).toBe(0)
  })

  test('a CLEAN null is LATE — no retry at all', async () => {
    const degraded = new Set<string>()
    const late = new Set<string>()
    let reads = 0
    const out = await readSafe('tpex.margin', degraded, late, async () => {
      reads++
      return null
    })
    expect(out).toBeNull()
    expect(reads).toBe(1)
    expect([...late]).toEqual(['tpex.margin'])
    expect(degraded.size).toBe(0)
  })

  test('a THROWN read is retried once, then FAILED', async () => {
    const degraded = new Set<string>()
    const late = new Set<string>()
    let reads = 0
    const out = await readSafe('twse.inst', degraded, late, async () => {
      reads++
      throw new Error('HTML instead of JSON')
    })
    expect(out).toBeNull()
    expect(reads).toBe(2)
    expect([...degraded]).toEqual(['twse.inst'])
    expect(late.size).toBe(0)
  })
})
