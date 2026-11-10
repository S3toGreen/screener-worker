import { describe, expect, test } from 'bun:test'
import {
  MARGIN_COST_MAX_AGE_DAYS,
  MARGIN_COST_STATE_VERSION,
  MARGIN_FINANCING_RATIO_DEFAULT,
  estimatedMaintenanceRatio,
  rollAverageMarginCost,
  sanitizeCreditState,
  type CreditFlows
} from './credit'

const flows = (patch: Partial<CreditFlows> = {}): CreditFlows => ({
  prevBal: 1_000,
  buy: 200,
  sell: 100,
  cashRepay: 100,
  bal: 1_000,
  close: 50,
  ...patch
})

describe('rollAverageMarginCost', () => {
  test('no prior state initializes at the close — and says it is not warm', () => {
    expect(rollAverageMarginCost(null, flows())).toEqual({ cost: 50, reset: true, warm: false })
  })

  test('a cold start seeds from the warm VWAP, not the close', () => {
    // The 60-session VWAP is the cost basis proxy; the ratio is no longer the
    // constant 100/0.6 that a close-seeded book produces.
    expect(rollAverageMarginCost(null, flows(), 44)).toEqual({
      cost: 44,
      reset: true,
      warm: true
    })
    // ...unless the price has moved corporate-action-sized away from it.
    expect(rollAverageMarginCost(null, flows({ close: 50 }), 20)).toEqual({
      cost: 50,
      reset: true,
      warm: false
    })
    // A zero/NaN seed is ignored.
    expect(rollAverageMarginCost(null, flows(), 0)?.warm).toBe(false)
    expect(rollAverageMarginCost(null, flows(), NaN)?.warm).toBe(false)
  })

  test('rolls (remaining × prior + buy × close) ÷ balance', () => {
    // remaining = 1000 − 100 − 100 = 800
    // cost = (800 × 40 + 200 × 50) / 1000 = 42
    expect(rollAverageMarginCost({ cost: 40, asOf: '20261103' }, flows())).toEqual({
      cost: 42,
      reset: false,
      warm: true
    })
  })

  test('a zero/negative balance has no estimate', () => {
    expect(rollAverageMarginCost(null, flows({ bal: 0 }))).toBeNull()
    expect(rollAverageMarginCost(null, flows({ bal: -5 }))).toBeNull()
  })

  test('an unusable price has no estimate', () => {
    expect(rollAverageMarginCost(null, flows({ close: 0 }))).toBeNull()
    expect(rollAverageMarginCost(null, flows({ close: NaN }))).toBeNull()
  })

  test('a corporate-action-sized price jump re-initializes', () => {
    expect(rollAverageMarginCost({ cost: 40, asOf: '20261103' }, flows({ close: 60 }))).toEqual({
      cost: 60,
      reset: true,
      warm: false
    })
    // Just under the 50 % threshold still rolls.
    const rolled = rollAverageMarginCost({ cost: 40, asOf: '20261103' }, flows({ close: 59 }))
    expect(rolled?.reset).toBe(false)
    expect(rolled?.cost).toBeCloseTo((800 * 40 + 200 * 59) / 1_000, 10)
  })

  test('a zero prior balance re-initializes instead of dividing history away', () => {
    expect(rollAverageMarginCost({ cost: 40, asOf: '20261103' }, flows({ prevBal: 0 }))).toEqual({
      cost: 50,
      reset: true,
      warm: false
    })
  })

  test('no churn (buy 0) keeps the prior cost', () => {
    // remaining 0, fresh 0 → balance must be 0 for that to be consistent, but
    // the rule is explicit: nothing traded → the prior estimate stands.
    const flat = flows({ buy: 0, sell: 50, cashRepay: 50, prevBal: 100, bal: 100, close: 21 })
    expect(rollAverageMarginCost({ cost: 20, asOf: '20261103' }, flat)).toEqual({
      cost: 20,
      reset: false,
      warm: true
    })
  })
})

describe('estimatedMaintenanceRatio', () => {
  test('close / (cost × ratio) × 100', () => {
    expect(estimatedMaintenanceRatio(80, 100, 60)).toBeCloseTo(133.333, 3)
    expect(estimatedMaintenanceRatio(80, 100)).toBeCloseTo(133.333, 3)
    expect(estimatedMaintenanceRatio(80, 100, 100)).toBe(80)
  })

  test('an unknown cost or price is null, never 0', () => {
    expect(estimatedMaintenanceRatio(80, null)).toBeNull()
    expect(estimatedMaintenanceRatio(80, 0)).toBeNull()
    expect(estimatedMaintenanceRatio(0, 100)).toBeNull()
    expect(estimatedMaintenanceRatio(80, 100, 0)).toBeNull()
  })

  test('the default ratio is the general 60 % maximum', () => {
    expect(MARGIN_FINANCING_RATIO_DEFAULT).toBe(60)
  })
})

describe('sanitizeCreditState', () => {
  const now = Date.UTC(2026, 10, 4) // 2026-11-04

  test('keeps valid entries and drops malformed ones', () => {
    const state = sanitizeCreditState(
      {
        version: MARGIN_COST_STATE_VERSION,
        asOf: '20261104',
        symbols: {
          '2330': { cost: 1_500, asOf: '20261103' },
          bad: { cost: 'x', asOf: '20261103' },
          zero: { cost: 0, asOf: '20261103' },
          noStamp: { cost: 10, asOf: 'yesterday' }
        }
      },
      now
    )
    expect(state.asOf).toBe('20261104')
    expect(Object.keys(state.symbols)).toEqual(['2330'])
    expect(state.symbols['2330']).toEqual({ cost: 1_500, asOf: '20261103', warm: true })
  })

  test('a stale entry is pruned (it re-initializes at its next session)', () => {
    const stale = '20240101'
    const state = sanitizeCreditState(
      {
        version: MARGIN_COST_STATE_VERSION,
        asOf: '20240101',
        symbols: { '2330': { cost: 100, asOf: stale } }
      },
      now
    )
    expect(state.symbols['2330']).toBeUndefined()
    expect(MARGIN_COST_MAX_AGE_DAYS).toBeGreaterThan(0)
  })

  test('a close-initialized (cold) entry keeps its flag through a round trip', () => {
    const state = sanitizeCreditState({
      version: MARGIN_COST_STATE_VERSION,
      asOf: '20261104',
      symbols: { '2330': { cost: 100, asOf: '20261104', warm: false } }
    })
    expect(state.symbols['2330']).toEqual({ cost: 100, asOf: '20261104', warm: false })
  })

  test('garbage input is an empty state, never a throw', () => {
    for (const raw of [null, undefined, 42, 'x', {}, { symbols: [] }]) {
      expect(sanitizeCreditState(raw)).toEqual({
        version: MARGIN_COST_STATE_VERSION,
        asOf: '',
        symbols: {}
      })
    }
    // A v1 (close-seeded) file is re-initialized from the warm priors.
    expect(
      sanitizeCreditState({ asOf: '20261104', symbols: { '2330': { cost: 50, asOf: '20261103' } } })
    ).toEqual({ version: MARGIN_COST_STATE_VERSION, asOf: '', symbols: {} })
  })

  test('without a clock no age prune happens', () => {
    const state = sanitizeCreditState({
      version: MARGIN_COST_STATE_VERSION,
      asOf: '20240101',
      symbols: { '2330': { cost: 100, asOf: '20240101' } }
    })
    expect(state.symbols['2330']).toEqual({ cost: 100, asOf: '20240101', warm: true })
  })
})
