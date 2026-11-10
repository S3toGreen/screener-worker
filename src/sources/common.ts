/**
 * The TWO exchanges' reader scaffold — every TWSE/TPEx table is the same
 * walk: polite-gated GET → positional rows resolved by field name → one
 * normalized row per symbol (+ optionally the payload's session date).
 */
import { DAY_MS } from '@odf/screener-worker/session'
import {
  dateToMs,
  getJson,
  indexer,
  parseTable,
  type FetchLike
} from '@odf/screener-worker/httpJson'
import { sourced, type Sourced } from './types'

/** The table endpoints are multi-MB payloads from a government host that
 *  stalls under load (a 3 MB T86 measured 37 s) — the 10 s `getJson` default
 *  fails a request that would have succeeded. */
export const TABLE_TIMEOUT_MS = 60_000

/** A TWSE rwd table title's ROC date (`115年11月03日 …`) names the session. */
export function titleIsSession(title: string | undefined, stamp: string): boolean {
  if (!title) return false
  const m = /^(\d{2,3})年(\d{1,2})月(\d{1,2})日/.exec(title.trim())
  if (!m) return false
  return (
    Number(m[1]) === Number(stamp.slice(0, 4)) - 1911 &&
    Number(m[2]) === Number(stamp.slice(4, 6)) &&
    Number(m[3]) === Number(stamp.slice(6, 8))
  )
}

/** Map an ALREADY-FETCHED payload (the caller needed its headers); `null`
 *  drops the row. */
export function mapTableRows<T>(
  raw: unknown,
  map: (read: (field: string) => string) => T | null
): T[] {
  const t = parseTable(raw)
  const at = indexer(t.fields)
  const out: T[] = []
  for (const r of t.rows) {
    const mapped = map((field) => r[at(field)] ?? '')
    if (mapped !== null) out.push(mapped)
  }
  return out
}

/** Fetch one table and map its rows. */
export async function readTableRows<T>(
  url: string,
  fetchImpl: FetchLike | undefined,
  map: (read: (field: string) => string) => T | null,
  timeoutMs?: number
): Promise<T[]> {
  return mapTableRows(await getJson(url, fetchImpl, timeoutMs ?? TABLE_TIMEOUT_MS), map)
}

export interface SourcedTableSpec<T> {
  url: string
  timeoutMs?: number
  /** The row's dedupe key column (usually the symbol). */
  keyField: string
  /** The payload's own session-date column (absent = the table carries none). */
  dateField?: string
  /** `key` is the trimmed keyField value; the row builder never has to re-read
   *  the key column. */
  map: (read: (field: string) => string, key: string) => T
}

/** Map an ALREADY-FETCHED payload into the keyed table + its session date. */
export function mapSourcedTable<T>(spec: SourcedTableSpec<T>, raw: unknown): Sourced<T> {
  const out = sourced<T>(0)
  const rows = mapTableRows(raw, (read) => {
    const key = read(spec.keyField).trim()
    if (!key) return null
    const dateMs = spec.dateField ? dateToMs(read(spec.dateField)) : 0
    return { key, dateMs, value: spec.map(read, key) }
  })
  for (const row of rows) {
    out.data.set(row.key, row.value)
    if (out.dateMs === 0 && row.dateMs > 0) out.dateMs = row.dateMs
    out.rows++
  }
  return out
}

/** A table keyed by symbol, with the payload's session date. */
export async function readSourcedTable<T>(
  spec: SourcedTableSpec<T>,
  fetchImpl?: FetchLike
): Promise<Sourced<T>> {
  return mapSourcedTable(
    spec,
    await getJson(spec.url, fetchImpl, spec.timeoutMs ?? TABLE_TIMEOUT_MS)
  )
}

/** A membership list OR-ed into a bitmask; `window` gates a list that only
 *  applies while the anchor session falls inside each row's own window. */
export async function readMembers(
  url: string,
  fetchImpl: FetchLike | undefined,
  add: (symbol: string, bits: number) => void,
  codeField: string,
  bits: number,
  window?: { fromField: string; toField: string; anchorMs: number }
): Promise<number> {
  const rows = await readTableRows(url, fetchImpl, (read) => {
    const symbol = read(codeField).trim()
    if (!symbol) return null // the exchanges' "nothing today" placeholder row
    if (window) {
      const from = dateToMs(read(window.fromField))
      const to = dateToMs(read(window.toField))
      if (!inWindow(window.anchorMs, from, to)) return null
    }
    return symbol
  })
  for (const symbol of rows) add(symbol, bits)
  return rows.length
}

/** `anchor` inside `[from, to]`, tolerating a missing/one-sided window. */
export function inWindow(anchor: number, from: number, to: number): boolean {
  if (anchor <= 0) return true
  if (from > 0 && anchor < from) return false
  if (to > 0 && anchor > to + DAY_MS) return false
  return true
}
