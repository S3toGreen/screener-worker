import { describe, expect, test } from 'bun:test'
import {
  HISTORY_MANIFEST_KEY,
  emptyHistoryManifest,
  manifestLastDates,
  mergeHistoryManifest,
  readHistoryManifest
} from './manifest'
import { stampToMs } from './session'

describe('history manifest', () => {
  test('the key is ONE private bucket object', () => {
    expect(HISTORY_MANIFEST_KEY).toBe('history-manifest.json')
  })

  test('round-trips valid entries and drops malformed ones', () => {
    const m = readHistoryManifest({
      version: 1,
      updatedAt: 1_700_000_000_000,
      symbols: { 2330: '20261109', 2317: '20261108', bad: '2026-11-09', empty: '' }
    })
    expect(m?.symbols).toEqual({ 2317: '20261108', 2330: '20261109' })
    expect(manifestLastDates(m!).get('2330')).toBe(stampToMs('20261109'))
  })

  test('rejects corrupt shapes rather than trusting them', () => {
    expect(readHistoryManifest(null)).toBeNull()
    expect(readHistoryManifest({ symbols: {} })).toBeNull()
    expect(readHistoryManifest({ version: 2, updatedAt: 1, symbols: {} })).toBeNull()
    expect(readHistoryManifest({ version: 1, updatedAt: 0, symbols: {} })).toBeNull()
  })

  test('merges without dropping symbols and never invalidates an old date', () => {
    const base = mergeHistoryManifest(emptyHistoryManifest(1), { 2330: '20261109' }, 2)
    const next = mergeHistoryManifest(
      base,
      new Map([
        ['6488', '20261109'],
        ['2330', 'bad']
      ]),
      3
    )
    expect(next.symbols).toEqual({ 2330: '20261109', 6488: '20261109' })
    expect(next.updatedAt).toBe(3)
  })
})
