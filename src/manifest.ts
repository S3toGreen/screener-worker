/**
 * ONE bucket manifest for the per-symbol histories — the safety guard's index.
 *
 * Each entry is the symbol's newest stored `YYYYMMDD` session. Writers:
 *   - the daily ETL records every history it wrote or found unchanged;
 *   - the backfill writes the repaired targets (the uploader merges it into R2);
 *   - the safety guard repairs a missing/unknown entry by reading that one
 *     history, then writes the merged manifest.
 *
 * The manifest is an OPTIMIZATION, never the source of truth: a missing,
 * corrupt or stale object falls back to reading `history/{symbol}.json`.
 */
import { stampToMs } from './session'

export const HISTORY_MANIFEST_KEY = 'history-manifest.json'
export const HISTORY_MANIFEST_VERSION = 1

export interface HistoryManifest {
  version: typeof HISTORY_MANIFEST_VERSION
  updatedAt: number
  /** symbol → newest stored session (`YYYYMMDD`). Sorted for stable bytes. */
  symbols: Record<string, string>
}

export function emptyHistoryManifest(nowMs: number = Date.now()): HistoryManifest {
  return { version: HISTORY_MANIFEST_VERSION, updatedAt: nowMs, symbols: {} }
}

function validStamp(value: unknown): value is string {
  return typeof value === 'string' && /^\d{8}$/.test(value) && stampToMs(value) > 0
}

/** Validates a stored/uploaded object; any malformed entry is dropped. */
export function readHistoryManifest(raw: unknown): HistoryManifest | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null
  const obj = raw as { version?: unknown; updatedAt?: unknown; symbols?: unknown }
  if (obj.version !== HISTORY_MANIFEST_VERSION) return null
  if (typeof obj.updatedAt !== 'number' || !Number.isFinite(obj.updatedAt) || obj.updatedAt <= 0)
    return null
  if (!obj.symbols || typeof obj.symbols !== 'object' || Array.isArray(obj.symbols)) return null
  const symbols: Record<string, string> = {}
  for (const [symbol, stamp] of Object.entries(obj.symbols)) {
    if (!symbol || !validStamp(stamp)) continue
    symbols[symbol] = stamp
  }
  symbolsSorted(symbols)
  return { version: HISTORY_MANIFEST_VERSION, updatedAt: obj.updatedAt, symbols }
}

/** Merges updates into a manifest; existing entries never disappear. */
export function mergeHistoryManifest(
  current: HistoryManifest | null,
  updates: ReadonlyMap<string, string> | Readonly<Record<string, string>>,
  nowMs: number = Date.now()
): HistoryManifest {
  const symbols: Record<string, string> = { ...(current?.symbols ?? {}) }
  const entries = updates instanceof Map ? updates.entries() : Object.entries(updates)
  for (const [symbol, stamp] of entries) {
    if (!symbol || !validStamp(stamp)) continue
    symbols[symbol] = stamp
  }
  symbolsSorted(symbols)
  return { version: HISTORY_MANIFEST_VERSION, updatedAt: nowMs, symbols }
}

/** The manifest as the safety guard's `symbol → newest session ms` map. */
export function manifestLastDates(manifest: HistoryManifest): Map<string, number> {
  const out = new Map<string, number>()
  for (const [symbol, stamp] of Object.entries(manifest.symbols)) {
    const ms = stampToMs(stamp)
    if (ms > 0) out.set(symbol, ms)
  }
  return out
}

function symbolsSorted(symbols: Record<string, string>): void {
  const keys = Object.keys(symbols).sort()
  for (const key of keys) {
    const stamp = symbols[key]!
    delete symbols[key]
    symbols[key] = stamp
  }
}
