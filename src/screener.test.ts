import { describe, expect, test } from 'bun:test'
import {
  MARKET_HISTORY_SESSIONS,
  SC,
  SCREENER_BASE_FIELDS,
  SCREENER_FIELDS,
  mergeMarketContext,
  mergeMarketDerivatives,
  mergeMarketHistory,
  readMarketContext,
  readSnapshotRows,
  screenNum,
  screenStr,
  type MarketContext,
  type MarketCredit,
  type MarketDerivatives,
  type ScreenerRow,
  type TaifexInstitution
} from './screener'

/** A full-contract row (every cell null except the identity columns). */
function row(symbol = '2330', close = 100_000): ScreenerRow {
  const out: ScreenerRow = SCREENER_FIELDS.map(() => null)
  out[SC.symbol] = symbol
  out[SC.name] = '台積電'
  out[SC.market] = 'TWSE'
  out[SC.close] = close
  return out
}

describe('the positional contract', () => {
  test('the base width is exactly the pre-credit contract', () => {
    expect(SCREENER_BASE_FIELDS).toBe(SC.marginBuy)
    expect(SCREENER_BASE_FIELDS).toBeLessThan(SCREENER_FIELDS.length)
    // Every appended column is the credit block, in order.
    expect(SCREENER_FIELDS.slice(SCREENER_BASE_FIELDS)).toEqual([
      'marginBuy',
      'marginSell',
      'marginCashRepay',
      'marginChange',
      'shortSell',
      'shortCover',
      'shortCashRepay',
      'shortChange',
      'avgMarginCost',
      'estMaintenanceRatio',
      'marginOffset',
      'marginNote'
    ])
  })
})

describe('readSnapshotRows', () => {
  test('a full-width grid is passed through WITHOUT copying rows', () => {
    const grid = [row(), row('2317')]
    const read = readSnapshotRows(grid)
    expect(read).not.toBeNull()
    expect(read?.[0]).toBe(grid[0])
    expect(read?.[1]).toBe(grid[1])
  })

  test('an older, narrower grid is padded — never rejected as corrupt', () => {
    // The production incident: a 61-column snapshot behind a 72-column client.
    const old = row().slice(0, SCREENER_BASE_FIELDS)
    const grid = [old]
    const read = readSnapshotRows(grid)
    expect(read).not.toBeNull()
    expect(read?.[0].length).toBe(SCREENER_FIELDS.length)
    // The old cells survive positionally; the appended cells are unknown.
    expect(read?.[0][SC.close]).toBe(100_000)
    expect(read?.[0][SC.marginBuy]).toBeNull()
    expect(read?.[0][SC.estMaintenanceRatio]).toBeNull()
    // The input is untouched (the payload is the cache writer's).
    expect(old.length).toBe(SCREENER_BASE_FIELDS)
  })

  test('a grid shorter than the base contract is not a snapshot', () => {
    expect(readSnapshotRows([row().slice(0, SCREENER_BASE_FIELDS - 1)])).toBeNull()
    expect(readSnapshotRows([['2330']])).toBeNull()
  })

  test('malformed payloads are rejected, not guessed at', () => {
    expect(readSnapshotRows(null)).toBeNull()
    expect(readSnapshotRows(undefined)).toBeNull()
    expect(readSnapshotRows([])).toBeNull()
    expect(readSnapshotRows({ rows: [row()] })).toBeNull()
    expect(readSnapshotRows([row(), 'oops'])).toBeNull()
    expect(readSnapshotRows([row(), [1, 2, 3]])).toBeNull()
  })

  test('a NEWER, wider grid is accepted as-is (forward compatible)', () => {
    const wide = [...row(), 'future-column']
    const read = readSnapshotRows([wide])
    expect(read?.[0]).toBe(wide)
  })
})

describe('market history', () => {
  const point = (session: number, margin: number | null): Record<string, unknown> => ({
    sessionDate: session,
    credit: { scope: 'TWSE', sessionDate: session, twseMarginBalanceLots: margin },
    derivatives: null
  })
  const credit = (session: number, patch: Partial<MarketCredit> = {}): MarketCredit =>
    ({
      scope: 'TWSE',
      sessionDate: session,
      maintenanceRatioKind: 'estimate',
      ...patch
    }) as MarketCredit
  const derivatives = (
    asOf: number,
    institutions: TaifexInstitution[] = []
  ): MarketDerivatives => ({
    asOf,
    institutions,
    futures: [],
    options: [],
    largeTraders: [],
    pcr: [],
    vix: null
  })

  test('points are parsed, deduped, future-bounded and ordered', () => {
    const ctx = readMarketContext({
      asOf: 3,
      credit: { scope: 'TWSE', sessionDate: 3, twseMarginBalanceLots: 30 },
      history: [
        point(2, 20),
        point(1, 10),
        point(2, 22),
        point(4, 40),
        point(0, 9),
        'bad',
        { sessionDate: 5 }
      ]
    })
    expect(ctx?.history?.map((p) => p.sessionDate)).toEqual([1, 2])
    expect(ctx?.history?.[1]?.credit?.twseMarginBalanceLots).toBe(22)
  })

  test('the rolling series keeps the newest MARKET_HISTORY_SESSIONS points', () => {
    const points = Array.from({ length: 70 }, (_, i) => point(i + 1, i + 1))
    const ctx = readMarketContext({
      asOf: 70,
      credit: { scope: 'TWSE', sessionDate: 70, twseMarginBalanceLots: 70 },
      history: points
    })
    expect(ctx?.history?.length).toBe(MARKET_HISTORY_SESSIONS)
    expect(ctx?.history?.[0]?.sessionDate).toBe(11)
    expect(ctx?.history?.at(-1)?.sessionDate).toBe(70)
  })

  test('a same-session market re-run keeps cells the failed half lost', () => {
    const prior: MarketContext = {
      asOf: 2,
      credit: credit(2, {
        twseMarginBalanceLots: 5,
        twseMarginBuyLots: 7,
        maintenanceRatio: 160
      }),
      derivatives: derivatives(2, [{ item: '外資', futuresOiNet: 10 } as TaifexInstitution])
    }
    const next: MarketContext = {
      asOf: 2,
      credit: credit(2, { twseMarginBalanceLots: null, twseMarginBuyLots: 8 }),
      derivatives: derivatives(2, [])
    }
    const merged = mergeMarketContext(prior, next)
    expect(merged.credit?.twseMarginBalanceLots).toBe(5)
    expect(merged.credit?.twseMarginBuyLots).toBe(8)
    expect(merged.credit?.maintenanceRatio).toBe(160)
    expect(merged.derivatives?.institutions).toHaveLength(1)
  })

  test('the daily writer seeds the current observation when the series is missing', () => {
    const current: MarketContext = {
      asOf: 3,
      credit: credit(3, { twseMarginBalanceLots: 30 }),
      derivatives: null
    }
    expect(mergeMarketHistory(undefined, current)).toEqual([
      { sessionDate: 3, credit: current.credit, derivatives: null }
    ])
    expect(mergeMarketHistory(undefined, current, null).map((p) => p.sessionDate)).toEqual([3])
  })

  test('the prior object seeds a second observation and replaces the current point', () => {
    const fallback: MarketContext = {
      asOf: 2,
      credit: credit(2, { twseMarginBalanceLots: 20 }),
      derivatives: null
    }
    const current: MarketContext = {
      asOf: 3,
      credit: credit(3, { twseMarginBalanceLots: 30 }),
      derivatives: null
    }
    const history = mergeMarketHistory(undefined, current, fallback)
    expect(history.map((p) => p.sessionDate)).toEqual([2, 3])
    expect(history.length).toBe(2)
    const replaced = mergeMarketHistory(history, current, null)
    expect(replaced).toEqual(history)
  })

  test('a scope downgrade keeps the prior self-consistent credit block', () => {
    const prior: MarketContext = {
      asOf: 2,
      credit: credit(2, {
        scope: 'TWSE+TPEX',
        twseMarginBalanceLots: 5,
        maintenanceRatio: 160,
        maintenanceRatioNote: 'both'
      }),
      derivatives: null
    }
    const next: MarketContext = {
      asOf: 2,
      credit: credit(2, {
        scope: 'TWSE',
        twseMarginBalanceLots: 9,
        maintenanceRatio: null,
        maintenanceRatioNote: 'TWSE only'
      }),
      derivatives: null
    }
    const merged = mergeMarketContext(prior, next)
    expect(merged.credit?.scope).toBe('TWSE+TPEX')
    expect(merged.credit?.twseMarginBalanceLots).toBe(5)
    expect(merged.credit?.maintenanceRatio).toBe(160)
    expect(merged.credit?.maintenanceRatioNote).toBe('both')
  })

  test('an older TAIFEX observation never overwrites a newer one', () => {
    const prior = derivatives(2, [{ item: '外資', futuresOiNet: 10 } as TaifexInstitution])
    prior.vix = { dateMs: 2, close: 40 }
    const next = derivatives(1, [])
    next.vix = { dateMs: 1, close: 30 }
    const merged = mergeMarketDerivatives(prior, next)
    expect(merged.asOf).toBe(2)
    expect(merged.institutions[0]?.futuresOiNet).toBe(10)
    expect(merged.vix?.close).toBe(40)
  })
})
describe('column readers', () => {
  test('missing and malformed cells are null / empty — never 0', () => {
    const r = row()
    expect(screenNum(r, 'per')).toBeNull()
    expect(screenStr(r, 'industry')).toBe('')
    r[SC.per] = Number.NaN
    expect(screenNum(r, 'per')).toBeNull()
    r[SC.per] = 12.5
    expect(screenNum(r, 'per')).toBe(12.5)
  })
})

describe('readMarketContext', () => {
  test('a null/empty object is no context', () => {
    expect(readMarketContext(null)).toBeNull()
    expect(readMarketContext({})).toBeNull()
    expect(readMarketContext({ credit: null, derivatives: null })).toBeNull()
    expect(readMarketContext({ credit: { scope: 'TPEX' } })).toBeNull()
  })

  test('the object date falls back to the halves, never fabricated', () => {
    const ctx = readMarketContext({
      credit: { scope: 'TWSE', sessionDate: 1_793_635_200_000, twseMarginBalanceLots: 5 },
      derivatives: null
    })
    expect(ctx?.asOf).toBe(1_793_635_200_000)
    expect(ctx?.credit?.twseMarginBalanceLots).toBe(5)
    // An explicit stamp wins.
    expect(readMarketContext({ asOf: 7, credit: { scope: 'TWSE', sessionDate: 1 } })?.asOf).toBe(7)
  })

  test('the two ratios keep their own slots', () => {
    const ctx = readMarketContext({
      asOf: 1,
      credit: {
        scope: 'TWSE+TPEX',
        sessionDate: 1,
        maintenanceRatio: 161.2,
        maintenanceRatioKind: 'estimate',
        maintenanceRatioNote: '上市櫃 estimate',
        officialMaintenanceRatio: 165.53,
        loanBalanceNtd: 796_340_000_000
      }
    })
    expect(ctx?.credit?.scope).toBe('TWSE+TPEX')
    expect(ctx?.credit?.maintenanceRatio).toBe(161.2)
    expect(ctx?.credit?.officialMaintenanceRatio).toBe(165.53)
    expect(ctx?.credit?.loanBalanceNtd).toBe(796_340_000_000)
  })

  test('the PRE-SPLIT object migrates: its official ratio is never re-labelled', () => {
    const ctx = readMarketContext({
      asOf: 1,
      credit: {
        scope: 'TWSE',
        sessionDate: 1,
        maintenanceRatio: 165.53,
        maintenanceRatioKind: 'official',
        maintenanceRatioNote: 'BFIJ3U_TREND'
      }
    })
    expect(ctx?.credit?.maintenanceRatio).toBeNull()
    expect(ctx?.credit?.officialMaintenanceRatio).toBe(165.53)
    // An undeclared 'estimated' kind is not our estimate either.
    const odd = readMarketContext({
      asOf: 1,
      credit: {
        scope: 'TWSE',
        sessionDate: 1,
        maintenanceRatio: 162.66,
        maintenanceRatioKind: 'estimated'
      }
    })
    expect(odd?.credit?.maintenanceRatio).toBeNull()
    expect(odd?.credit?.officialMaintenanceRatio).toBeNull()
  })

  test('a stringified number is unknown, not coerced', () => {
    const ctx = readMarketContext({
      credit: { scope: 'TWSE', sessionDate: 1, twseMarginBalanceLots: '599774' }
    })
    expect(ctx?.credit?.twseMarginBalanceLots).toBeNull()
  })

  test('derivatives keep their own date and drop dateless PCR rows', () => {
    const ctx = readMarketContext({
      asOf: 5,
      derivatives: {
        asOf: 4,
        institutions: [{ item: '自營商', futuresNet: 12 }],
        pcr: [
          { dateMs: 0, volumeRatio: 100 },
          { dateMs: 4, oiRatio: 150 }
        ],
        vix: { dateMs: 0, close: 36.8 }
      }
    })
    expect(ctx?.derivatives?.asOf).toBe(4)
    expect(ctx?.derivatives?.institutions[0]?.futuresNet).toBe(12)
    expect(ctx?.derivatives?.pcr.length).toBe(1)
    expect(ctx?.derivatives?.pcr[0]?.oiRatio).toBe(150)
    expect(ctx?.derivatives?.vix).toEqual({ dateMs: 0, close: 36.8 })
  })
})
