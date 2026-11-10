import { describe, expect, test } from 'bun:test'
import { benchmarkMonthStarts, mergeBenchmark } from './benchmark'

const session = Date.UTC(2026, 10, 4) - 8 * 3_600_000 // 2026-11-04 Taipei

describe('benchmarkMonthStarts', () => {
  test('returns the last N Taipei month starts, ascending', () => {
    const starts = benchmarkMonthStarts(session, 13)
    expect(starts).toHaveLength(13)
    expect(starts[0]).toBe(Date.UTC(2025, 10, 1) - 8 * 3_600_000)
    expect(starts.at(-1)).toBe(Date.UTC(2026, 10, 1) - 8 * 3_600_000)
    for (let i = 1; i < starts.length; i++) expect(starts[i]).toBeGreaterThan(starts[i - 1])
  })

  test('degenerate inputs produce no months', () => {
    expect(benchmarkMonthStarts(0, 13)).toEqual([])
    expect(benchmarkMonthStarts(session, 0)).toEqual([])
  })

  test('merging a month upserts and keeps the series sorted', () => {
    const prior = { dates: ['20261103'], close: [100] }
    const series = mergeBenchmark(prior, [
      { stamp: '20261104', close: 101 },
      { stamp: '20261103', close: 99 }
    ])
    expect(series.dates).toEqual(['20261103', '20261104'])
    expect(series.close).toEqual([99, 101])
  })
})
