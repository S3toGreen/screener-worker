/**
 * The ONE web-response reader for every public market-data host (the ETL, the
 * backfill and the hub's TAIFEX specs all import it): the polite gate, GET
 * JSON/text, the three table shapes (a JSON array of objects, TWSE rwd
 * `{fields,data}`, TPEx rwd `{tables:[{fields,data}]}`), CSV, and the
 * date/number coercions.
 *
 * These endpoints are public read-only with no auth; the only discipline they
 * need is pacing, which the module-level gate enforces for every caller (one
 * request at a time, a `POLITE_MS` gap — never a burst at a government host).
 */
import { TAIPEI_OFFSET_MS } from './session'

/** The Fetch slice this module needs — shared stays DOM-lib free; Node 18+,
 *  Bun, workerd and browsers all satisfy it structurally. */
export interface HttpResult {
  ok: boolean
  status: number
  json(): Promise<unknown>
  text(): Promise<string>
  /** The raw body — besides `text()` (always UTF-8) a Big5 page needs bytes. */
  arrayBuffer(): Promise<ArrayBuffer>
}
/** A fetch-shaped function — the ONE seam every source reader is built on. */
export interface FetchInit {
  method?: string
  headers?: Record<string, string>
  body?: string
  signal?: unknown
}
export type FetchLike = (url: string, init?: FetchInit) => Promise<HttpResult>

const defaultFetch = (url: string, init?: FetchInit): Promise<HttpResult> =>
  (globalThis as unknown as { fetch: FetchLike }).fetch(url, init)

/** TWSE's RWD host answers HTTP 428 to Bun's default `Bun/…` User-Agent for
 *  historical `MI_MARGN`/`MI_INDEX` pages (verified 2026-11-11). A browser UA
 *  is the accepted request shape and is harmless for the other public hosts. */
const BROWSER_UA =
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'

/** `AbortSignal.timeout` where the runtime has it (every target does today). */
const timeoutSignal = (ms: number): unknown =>
  (
    globalThis as unknown as { AbortSignal?: { timeout(ms: number): unknown } }
  ).AbortSignal?.timeout(ms)

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    ;(globalThis as unknown as { setTimeout(fn: () => void, ms: number): unknown }).setTimeout(
      resolve,
      ms
    )
  })

/** Pacing between two external requests (~4/s max, still trivial for a
 *  batch pull and far below anything that reads as abuse). */
export const POLITE_MS = 250

let gate: Promise<void> = Promise.resolve()
let lastRequestAt = 0

/** Serializes every external GET and spaces them by `POLITE_MS`. */
function polite(): Promise<void> {
  gate = gate.then(async () => {
    const wait = lastRequestAt + POLITE_MS - Date.now()
    if (wait > 0) await sleep(wait)
    lastRequestAt = Date.now()
  })
  return gate
}

/** One polite GET. Throws `HTTP <status>` on a non-OK response (the caller
 *  decides whether that is fatal — the ETL degrades per source). */
async function request(url: string, fetchImpl: FetchLike, timeoutMs: number): Promise<HttpResult> {
  await polite()
  const res = await fetchImpl(url, {
    headers: { 'user-agent': BROWSER_UA },
    signal: timeoutSignal(timeoutMs)
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res
}

/** One UTF-8 JSON body, BOM stripped — some exchange payloads (TAIFEX's
 *  institutional reports) start with `\uFEFF`, which a raw JSON.parse (and
 *  Bun's `Response.json()`) rejects with "Unrecognized token". */
export async function getJson(
  url: string,
  fetchImpl: FetchLike = defaultFetch,
  timeoutMs = 10_000
): Promise<unknown> {
  const text = await (await request(url, fetchImpl, timeoutMs)).text()
  return JSON.parse(text.charCodeAt(0) === 0xfeff ? text.slice(1) : text)
}

/** `TextDecoder` read off the global struct — shared stays DOM-lib free (the
 *  same pattern as `fetch`/`AbortSignal`/`setTimeout` above); every target
 *  (Node 18+, Bun with full ICU, workerd, browsers) provides it. */
const decodeWith = (charset: string, bytes: ArrayBuffer): string => {
  const Decoder = (
    globalThis as unknown as {
      TextDecoder?: new (label: string) => { decode(input: ArrayBuffer): string }
    }
  ).TextDecoder
  if (!Decoder) throw new Error('TextDecoder unavailable')
  return new Decoder(charset).decode(bytes)
}

/** GET + decode the body with `charset` (the MOPS static archives are Big5;
 *  `Response.text()` is always UTF-8, so a non-UTF-8 page reads its bytes). */
export async function getText(
  url: string,
  fetchImpl: FetchLike = defaultFetch,
  timeoutMs = 10_000,
  charset = 'utf-8'
): Promise<string> {
  const res = await request(url, fetchImpl, timeoutMs)
  if (charset.toLowerCase() === 'utf-8') return res.text()
  return decodeWith(charset, await res.arrayBuffer())
}

/** POST a form body + return the text (MOPS's report endpoints answer HTML to
 *  a POST — no GET/JSON/CSV shape covers them). Paced by the SAME gate. */
export async function postText(
  url: string,
  body: string,
  fetchImpl: FetchLike = defaultFetch,
  timeoutMs = 20_000
): Promise<string> {
  await polite()
  const res = await fetchImpl(url, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded',
      'user-agent': BROWSER_UA
    },
    body,
    signal: timeoutSignal(timeoutMs)
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.text()
}

/** One record's cell by column name (missing → ''). */
export type RecordRow = (field: string) => string

/**
 * GET a host that answers EITHER a JSON array of objects or CSV for the SAME
 * endpoint (TAIFEX mixes both, and its CSV bodies start with a BOM): the body
 * is read once and the shape is picked from its first byte. Returns null for
 * a shape that is neither (an HTML error page) so a caller can degrade
 * per source, never guess.
 */
export async function getRecords(
  url: string,
  fetchImpl: FetchLike = defaultFetch,
  timeoutMs = 20_000
): Promise<RecordRow[] | null> {
  const raw = await getText(url, fetchImpl, timeoutMs)
  const body = raw.charCodeAt(0) === 0xfeff ? raw.slice(1) : raw
  const head = body.trimStart().charAt(0)
  if (head === '[' || head === '{') {
    let parsed: unknown
    try {
      parsed = JSON.parse(body)
    } catch {
      return null
    }
    const list = Array.isArray(parsed)
      ? parsed
      : Array.isArray((parsed as { data?: unknown }).data)
        ? (parsed as { data: unknown[] }).data
        : null
    if (!list) return null
    return list
      .filter((r): r is Record<string, unknown> => !!r && typeof r === 'object')
      .map((r) => (field: string) => String(r[field] ?? ''))
  }
  if (head === '<' || body.trim() === '') return null
  const table = parseCsv(body)
  const at = new Map(table.fields.map((f, i) => [f.trim(), i]))
  return table.rows.map((cells) => (field: string) => cells[at.get(field) ?? -1] ?? '')
}

/** A parsed tabular payload: the field names and positional string rows. */
export interface WsTable {
  fields: string[]
  rows: string[][]
}

export function parseCsv(text: string): WsTable {
  const lines = text.split(/\r?\n/)
  let fields: string[] = []
  const rows: string[][] = []
  for (const line of lines) {
    const clean = line.replace(/^\uFEFF/, '').trim()
    if (clean === '') continue
    const cells = clean.split(',')
    if (fields.length === 0) {
      fields = cells.map((c) => c.trim())
      continue
    }
    rows.push(cells.map((c) => c.trim()))
  }
  return { fields, rows }
}

const EMPTY_TABLE: WsTable = { fields: [], rows: [] }

function cell(value: unknown): string {
  return value === null || value === undefined ? '' : String(value)
}

function rowOf(value: unknown): string[] {
  return Array.isArray(value) ? value.map(cell) : []
}

/** Reads the three shapes these hosts return: a JSON array of objects (the
 *  OpenAPI endpoints — the field order of the first row is the schema), TWSE
 *  rwd's `{fields, data}` and TPEx rwd's `{tables:[{fields, data}]}`. Anything
 *  else is an empty table, never a throw. */
export function parseTable(value: unknown): WsTable {
  if (Array.isArray(value)) {
    const first = value.find((row) => row && typeof row === 'object')
    if (!first) return EMPTY_TABLE
    // The first row's key order is the schema; every row is read BY KEY so a
    // reordered/extra-key row can never shift a neighbour's value.
    const fields = Object.keys(first as Record<string, unknown>)
    const rows: string[][] = []
    for (const row of value) {
      if (!row || typeof row !== 'object') continue
      const record = row as Record<string, unknown>
      rows.push(fields.map((field) => cell(record[field])))
    }
    return { fields, rows }
  }
  if (value && typeof value === 'object') {
    const o = value as { fields?: unknown; data?: unknown; tables?: unknown }
    if (Array.isArray(o.fields) && Array.isArray(o.data)) {
      return { fields: o.fields.map(cell), rows: o.data.map(rowOf) }
    }
    if (Array.isArray(o.tables) && o.tables.length > 0) {
      const t = o.tables[0] as { fields?: unknown; data?: unknown } | null
      if (t && Array.isArray(t.fields) && Array.isArray(t.data)) {
        return { fields: t.fields.map(cell), rows: t.data.map(rowOf) }
      }
    }
  }
  return EMPTY_TABLE
}

/** A numeric cell: trims whitespace (incl. the full-width space these payloads
 *  carry), strips thousands separators and '%', and answers NaN for the
 *  exchange's empty markers (`-`, `''`, `NULL`). */
export function num(value: string | number | undefined | null): number {
  if (typeof value === 'number') return Number.isFinite(value) ? value : NaN
  if (typeof value !== 'string') return NaN
  const cleaned = value
    .replace(/[\s\u3000]/g, '')
    .replace(/,/g, '')
    .replace(/%/g, '')
  if (cleaned === '' || cleaned === '-' || cleaned === '--' || /^null$/i.test(cleaned)) return NaN
  const n = Number(cleaned)
  return Number.isFinite(n) ? n : NaN
}

/** ms of Taipei-local midnight for a calendar date; 0 when impossible. */
function taipeiMidnight(year: number, month: number, day: number): number {
  if (month < 1 || month > 12 || day < 1 || day > 31) return 0
  const ms = Date.UTC(year, month - 1, day)
  const back = new Date(ms)
  if (back.getUTCFullYear() !== year || back.getUTCMonth() !== month - 1) return 0
  if (back.getUTCDate() !== day) return 0
  return ms - TAIPEI_OFFSET_MS
}

/** A ROC date (`'1151014'`, `'115/10/14'`) → ms; 0 when unparseable. ROC year
 *  = Gregorian − 1911, so `115` is 2026. */
export function rocToMs(value: string): number {
  const m = /^(\d{3})[/-]?(\d{1,2})[/-]?(\d{1,2})$/.exec(value.trim())
  if (!m) return 0
  return taipeiMidnight(Number(m[1]) + 1911, Number(m[2]), Number(m[3]))
}

/** A Gregorian date (`'20261015'`, `'2026-10-15'`, `'2026/10/15'`) → ms; 0 when
 *  unparseable. (TPEx mixes both calendars across its paths.) */
export function gregToMs(value: string): number {
  const m = /^(\d{4})[/-]?(\d{1,2})[/-]?(\d{1,2})$/.exec(value.trim())
  if (!m) return 0
  return taipeiMidnight(Number(m[1]), Number(m[2]), Number(m[3]))
}

/** A session-named date from either calendar — the "which day is this payload
 *  reporting" question every screener source has to answer. */
export function dateToMs(value: string): number {
  // `115年11月06日` (the ex-dividend calendar's own form) → `115/11/06`.
  const trimmed = value.trim().replace(/年/g, '/').replace(/月/g, '/').replace(/日/g, '')
  const ms = trimmed.replace(/[/-]/g, '').length === 8 ? gregToMs(trimmed) : rocToMs(trimmed)
  return ms !== 0 ? ms : rocToMs(trimmed) || gregToMs(trimmed)
}

/** `'11509'` (a ROC year-month, the 資料年月 field) → ms of Taipei midnight on
 *  the 1st of that month; 0 when unparseable. */
export function ymToMs(value: string): number {
  const m = /^(\d{3})(\d{2})$/.exec(value.trim())
  if (!m) return 0
  return rocToMs(`${m[1]}${m[2]}01`)
}

/** The numeric run inside a decorated value — `'新台幣    10.0000元'` → 10,
 *  `'1,234股'` → 1234; NaN when there is no number. */
export function extractNumber(value: string | undefined | null): number {
  if (typeof value !== 'string') return num(value)
  const cleaned = value.replace(/[^\d.-]/g, '')
  return cleaned === '' || cleaned === '-' ? NaN : num(cleaned)
}

/** Raw ×100 from a decimal price string (the app's price convention). */
export function rawPrice(v: string | undefined): number {
  const n = num(v)
  return Number.isFinite(n) ? Math.round(n * 100) : NaN
}

/** The whole-market tables' `Change` (a signed NT$ delta) → the session's
 *  change in percent. */
export function changePctOf(close: number, change: number): number {
  if (!Number.isFinite(close) || !Number.isFinite(change)) return NaN
  const prev = close - Math.round(change * 100)
  if (!Number.isFinite(prev) || prev <= 0) return NaN
  return (change * 100 * 100) / prev
}

/** Field-name → column index, ONE trimmed map per table (an absent name is
 *  -1, so `row[-1]` reads undefined and every coercion handles it). */
export function indexer(fields: string[]): (name: string) => number {
  const byName = new Map<string, number>()
  fields.forEach((f, i) => byName.set(f.trim(), i))
  return (name: string) => byName.get(name.trim()) ?? -1
}

/** The exchanges' "yes" cells — a full-width Ｙ in some tables, a plain Y in
 *  others; padding varies too, so trim first. */
export function isExchangeYes(v: string | undefined): boolean {
  const s = (v ?? '').trim()
  return s === 'Y' || s === 'Ｙ'
}
