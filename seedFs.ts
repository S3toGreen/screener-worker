/**
 * The workstation + CI harness the batch scripts share (NOT part of the Pages
 * bundle — `functions/` never imports it):
 *
 *  - a filesystem-backed `R2_BUCKET` so the real ETL runs locally unchanged;
 *  - BUCKET READS (the freshness gate + every prior history) try, in order:
 *    the local file, then R2 itself through the S3 API when credentials are in
 *    the environment (the GitHub Actions path — a Pages outage can no longer
 *    be mistaken for "no previous data"), then the live API;
 *  - the bucket uploader (the S3 API, one client, parallel PUTs);
 *  - the cache-control strings the objects are written with.
 *
 * `.seed-out/` mirrors the bucket keys exactly (`screener-latest.json`,
 * `benchmark.json`, `history/{symbol}.json`) and is gitignored.
 */
import { existsSync } from 'node:fs'
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, join, relative } from 'node:path'
import { rm } from 'node:fs/promises'
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import {
  SCREENER_MARKET_KEY,
  SCREENER_SNAPSHOT_KEY,
  screenerHistoryUrl,
  screenerMarketUrl,
  screenerSnapshotUrl
} from '@odf/screener-worker/screener'
import type { ScreenerEnv } from './src/env'
import { CACHE_VERSION_KEY, DATA_CACHE_CONTROL } from './src/keys'
import {
  HISTORY_MANIFEST_KEY,
  mergeHistoryManifest,
  readHistoryManifest,
  type HistoryManifest
} from './src/manifest'

export const SEED_ROOT = join(import.meta.dir, '.seed-out')
/** Parallel PUTs: the S3 API takes far more than the wrangler REST path did.
 *  ~2 400 objects land in ONE batch (one client, one wave); the push is
 *  latency-bound (a CI runner sits ~1 s from R2 per object), so the pool — not
 *  the object count — sets the wall time. 96 workers ≈ 2× the 64-worker rate
 *  with Class A cost still per-object. */
const UPLOAD_CONCURRENCY = 96

/** The public route for one bucket key (`history/2330.json` → `/api/history/2330`). */
function liveUrl(base: string, key: string): string {
  if (key === SCREENER_SNAPSHOT_KEY) return `${screenerSnapshotUrl(base)}?t=${Date.now()}`
  if (key === SCREENER_MARKET_KEY) return `${screenerMarketUrl(base)}?t=${Date.now()}`
  return screenerHistoryUrl(base, key.replace(/^history\//, '').replace(/\.json$/, ''))
}

/**
 * The bucket binding: a directory of JSON files for WRITES, with reads trying
 * the local file first, then R2 itself when S3 credentials are configured, and
 * the live API last. The fallbacks are what make the daily run safe — a fresh
 * `.seed-out/` must never read as "no previous data".
 *
 * The order matters for the freshness gate: a stale edge copy of the snapshot
 * is at most one session behind, but a HARD read failure has to surface as an
 * error so the ETL can carry previous rows — never silently rebuild from
 * today's payload alone.
 */
export function createFsEnv(apiBase = 'https://odf-trading.pages.dev'): ScreenerEnv {
  const base = apiBase.replace(/\/+$/, '')
  const readS3Object = createS3Reader()
  const bucket = {
    async get(key: string) {
      const path = join(SEED_ROOT, key)
      if (existsSync(path)) {
        const text = await readFile(path, 'utf8')
        return {
          async text() {
            return text
          },
          async json() {
            return JSON.parse(text)
          }
        }
      }
      if (readS3Object) {
        // null ONLY for a genuinely absent key (NoSuchKey); anything else
        // rethrows so the ETL's per-seed handling sees a failure, not absence.
        const bodyText = await readS3Object(key)
        if (bodyText !== null) {
          return {
            async text() {
              return bodyText
            },
            async json() {
              return JSON.parse(bodyText)
            }
          }
        }
        return null
      }
      const res = await fetch(liveUrl(base, key))
      // 404 = genuinely absent (first run); ANY other failure THROWS, because
      // an outage read as absence would rebuild a symbol's history from
      // today's payload alone — the mirror of the corruption bug.
      if (res.status === 404) return null
      if (!res.ok) throw new Error(`live API ${key} ${res.status}`)
      // One body read: `text()` is what the corrupt-history quarantine stores.
      const text = await res.text()
      return {
        async text() {
          return text
        },
        async json() {
          return JSON.parse(text)
        }
      }
    },
    async put(key: string, body: string) {
      const path = join(SEED_ROOT, key)
      await mkdir(dirname(path), { recursive: true })
      await writeFile(path, body)
    }
  }
  // Only `R2_BUCKET.get/put` are touched by runEtl — the stub stands in for the binding.
  return { R2_BUCKET: bucket } as unknown as ScreenerEnv
}

/** A plain backoff sleep — the two retry paths share the one delay helper. */
function delay(ms: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>()
  setTimeout(resolve, ms)
  return promise
}

/** `NoSuchKey` on the AWS SDK shapes (v3 error name / raw Code / 404 status). */
function isNoSuchKey(err: unknown): boolean {
  const e = err as { name?: string; Code?: string; $metadata?: { httpStatusCode?: number } }
  return e?.name === 'NoSuchKey' || e?.Code === 'NoSuchKey' || e?.$metadata?.httpStatusCode === 404
}

/**
 * Reads ONE object straight from R2 (one retry after a short backoff), or
 * returns null when the credentials are absent (the workstation fallback is
 * then the live API). Absence answers a literal `null`; every other failure
 * rethrows after the retries — an outage must never read as absence.
 */
function createS3Reader(): ((key: string) => Promise<string | null>) | null {
  let config: UploadConfig
  try {
    config = uploadConfigFromEnv()
  } catch (err) {
    // In CI the credentials are a configuration, not a preference: without them
    // the run would read ~2 400 histories one live-API request at a time and
    // then fail at the upload. Fail in the first second instead — a missing
    // repository secret must never look like a working daily job (it did: the
    // 2026-10-30 run "passed" only because the session was already current).
    if (process.env.CI) {
      throw new Error(
        `[seedFs] R2 credentials missing in CI — set the R2_ACCESS_KEY_ID / ` +
          `R2_SECRET_ACCESS_KEY / R2_ENDPOINT repository secrets (${String(err).split('\n')[0]})`
      )
    }
    // A workstation run without credentials reads the LIVE API, which is one
    // session behind R2 — the log is how the operator knows which tier answered.
    console.log(
      `[seedFs] S3 unavailable — reads fall back to the live API: ${String(err).split('\n')[0]}`
    )
    return null
  }
  const client = new S3Client({
    region: 'auto',
    endpoint: config.endpoint,
    credentials: { accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey },
    forcePathStyle: true,
    // A hung socket must fail the object (and retry) instead of stalling the
    // ETL before its first write — the uploader carries the same ceiling.
    requestHandler: { requestTimeout: 45_000, throwOnRequestTimeout: true }
  })
  return async (key: string): Promise<string | null> => {
    for (let attempt = 1; ; attempt++) {
      try {
        const obj = await client.send(new GetObjectCommand({ Bucket: config.bucket, Key: key }))
        const body = await obj.Body?.transformToString('utf8')
        if (body === undefined) throw new Error('R2 returned no object body')
        return body
      } catch (err) {
        if (isNoSuchKey(err)) return null
        if (attempt >= 2) throw err
        await delay(750)
      }
    }
  }
}

export async function resetSeedRoot(): Promise<void> {
  await rm(SEED_ROOT, { recursive: true, force: true })
}

/** Every object file, recursively. */
export async function seedFiles(): Promise<string[]> {
  const out: string[] = []
  async function walk(dir: string): Promise<void> {
    for (const entry of await readdir(dir, { withFileTypes: true })) {
      const full = join(dir, entry.name)
      if (entry.isDirectory()) await walk(full)
      else out.push(full)
    }
  }
  if (existsSync(SEED_ROOT)) await walk(SEED_ROOT)
  return out
}

export interface UploadConfig {
  bucket: string
  endpoint: string
  accessKeyId: string
  secretAccessKey: string
}

/** The R2 S3 credentials from the environment (see `.dev.vars`). */
export function uploadConfigFromEnv(): UploadConfig {
  const accessKeyId = process.env.R2_ACCESS_KEY_ID ?? ''
  const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY ?? ''
  const bucket = process.env.R2_BUCKET ?? 'screener'
  const endpoint =
    process.env.R2_ENDPOINT ??
    (process.env.CLOUDFLARE_ACCOUNT_ID
      ? `https://${process.env.CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com`
      : '')
  if (!accessKeyId || !secretAccessKey || !endpoint) {
    throw new Error(
      'R2 S3 credentials missing — set R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY and R2_ENDPOINT ' +
        '(or CLOUDFLARE_ACCOUNT_ID) in the environment / .dev.vars'
    )
  }
  return { bucket, endpoint, accessKeyId, secretAccessKey }
}

/**
 * Pushes every `.seed-out` file with ONE S3 client (parallel PUTs, no per-object
 * process). Idempotent: a re-run overwrites the same keys.
 *
 * The SNAPSHOT is published in a SECOND PHASE, after every other object has
 * settled: it names every row the histories must already carry, so a reader
 * that lands mid-upload must never see a grid whose history objects are still
 * the previous session's. (A plain sort only guarantees the snapshot is
 * *dequeued* last — up to `UPLOAD_CONCURRENCY` objects would still be in
 * flight.)
 */
async function putVersion(client: S3Client, config: UploadConfig): Promise<void> {
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      await client.send(
        new PutObjectCommand({
          Bucket: config.bucket,
          Key: CACHE_VERSION_KEY,
          Body: String(Date.now()),
          ContentType: 'text/plain; charset=utf-8',
          CacheControl: 'no-store'
        })
      )
      return
    } catch (err) {
      if (attempt === 3) {
        console.log(`[upload] WARN cache-version not written: ${String(err).slice(0, 200)}`)
        return
      }
      await delay(500 * 2 ** (attempt - 1))
    }
  }
}

async function readRemoteManifest(
  client: S3Client,
  config: UploadConfig
): Promise<HistoryManifest | null> {
  try {
    const obj = await client.send(
      new GetObjectCommand({ Bucket: config.bucket, Key: HISTORY_MANIFEST_KEY })
    )
    const text = await obj.Body?.transformToString('utf8')
    if (!text) return null
    const manifest = readHistoryManifest(JSON.parse(text))
    if (!manifest) console.log('[upload] remote history manifest unreadable — replacing from local')
    return manifest
  } catch (err) {
    // A genuinely absent object is a first upload; any other read failure keeps
    // the local entries (the safety guard's per-object fallback covers the rest).
    if (!isNoSuchKey(err)) {
      console.log(
        `[upload] remote history manifest read failed — merging local only: ${String(err)}`
      )
    }
    return null
  }
}

export async function uploadSeedRoot(
  config: UploadConfig = uploadConfigFromEnv()
): Promise<{ files: number; failed: number }> {
  const files = await seedFiles()
  if (files.length === 0) return { files: 0, failed: 0 }
  const keyOf = (path: string): string => relative(SEED_ROOT, path).split('\\').join('/')
  const snapshotPath = files.find((p) => keyOf(p) === SCREENER_SNAPSHOT_KEY) ?? null
  const manifestPath = files.find((p) => keyOf(p) === HISTORY_MANIFEST_KEY) ?? null
  const rest = files.filter((p) => p !== snapshotPath && p !== manifestPath)

  const client = new S3Client({
    region: 'auto',
    endpoint: config.endpoint,
    credentials: { accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey },
    // R2 requires path-style addressing on the account endpoint.
    forcePathStyle: true,
    // A hung socket must fail the object (and retry) instead of stalling the
    // whole batch until the 30-minute job ceiling kills it.
    requestHandler: { requestTimeout: 45_000, throwOnRequestTimeout: true }
  })

  let done = 0
  let failed = 0
  let historyFailed = 0
  /** Any non-history object (snapshot/market/holdings/benchmark) that made it
   *  successfully — its edge entry keys on the publication stamp. */
  let nonHistoryDone = 0
  const put = async (path: string): Promise<boolean> => {
    const key = relative(SEED_ROOT, path).split('\\').join('/')
    const body = await readFile(path)
    for (let attempt = 1; attempt <= 4; attempt++) {
      try {
        await client.send(
          new PutObjectCommand({
            Bucket: config.bucket,
            Key: key,
            Body: body,
            ContentType: 'application/json; charset=utf-8',
            CacheControl: DATA_CACHE_CONTROL
          })
        )
        done++
        if (!key.startsWith('history/')) nonHistoryDone++
        if (done % 250 === 0) console.log(`[upload] ${done}/${files.length} (failed ${failed})`)
        return true
      } catch (err) {
        if (attempt === 4) {
          failed++
          if (key.startsWith('history/')) historyFailed++
          console.log(
            `[upload] FAIL ${key} (after ${attempt} attempts): ${String(err).slice(0, 300)}`
          )
          return false
        }
        await delay(500 * 2 ** (attempt - 1))
      }
    }
    return false
  }
  const drain = async (paths: string[]): Promise<void> => {
    const queue = [...paths]
    await Promise.all(
      Array.from({ length: Math.min(UPLOAD_CONCURRENCY, queue.length) }, async () => {
        for (let path = queue.shift(); path !== undefined; path = queue.shift()) await put(path)
      })
    )
  }
  await drain(rest)
  // The ONE history manifest is MERGED with R2, never replaced by a local
  // partial: a gap repair updates only its targets, and the daily ETL only its
  // own session's symbols. A missing manifest is a safe empty merge.
  // The manifest is a CLAIM about the history objects. If any history PUT
  // failed, publishing a newer manifest would make the next safety guard trust
  // a date the object does not actually carry, so leave the previous index in
  // place and let the per-object fallback detect the gap.
  if (manifestPath !== null && historyFailed === 0) {
    let local: HistoryManifest | null = null
    try {
      local = readHistoryManifest(JSON.parse(await readFile(manifestPath, 'utf8')))
    } catch (err) {
      console.log(`[upload] local history manifest unreadable — skipped: ${String(err)}`)
    }
    if (local) {
      const remote = await readRemoteManifest(client, config)
      const merged = mergeHistoryManifest(remote, local.symbols, Date.now())
      try {
        await client.send(
          new PutObjectCommand({
            Bucket: config.bucket,
            Key: HISTORY_MANIFEST_KEY,
            Body: JSON.stringify(merged),
            ContentType: 'application/json; charset=utf-8',
            CacheControl: 'no-store'
          })
        )
        done++
        console.log(`[upload] history manifest ${Object.keys(merged.symbols).length} symbols`)
      } catch (err) {
        // The manifest is an OPTIMIZATION; the histories themselves uploaded.
        // A private-index failure must never fail the data job (the safety
        // guard falls back to per-object reads and rebuilds the index).
        console.log(`[upload] WARN ${HISTORY_MANIFEST_KEY}: ${String(err).slice(0, 300)}`)
      }
    }
  }
  // The snapshot names every row the histories must already carry. A failed
  // history upload therefore WITHHOLDS the publication: a reader must never
  // see a grid whose history objects are still a session behind. The previous
  // snapshot stays live; the next ETL run repairs and republishes.
  let snapshotPublished = false
  if (snapshotPath !== null) {
    if (historyFailed > 0) {
      console.log(
        `[upload] snapshot withheld — ${historyFailed} history object(s) failed; ` +
          'the previous snapshot remains published'
      )
    } else {
      snapshotPublished = await put(snapshotPath)
      if (snapshotPublished) console.log('[upload] snapshot published last')
    }
  }
  // The publication stamp LAST: every non-history edge entry (grid, market,
  // holdings, dividends, benchmark) keys on it, so a new session/object replaces
  // the cached copy within the Function's one-minute memo. A histories-only
  // backfill deliberately skips it; a market-only repair still needs it. A
  // failed history leaves the snapshot withheld, but a successfully uploaded
  // independent object must still be served.
  if (nonHistoryDone > 0) await putVersion(client, config)
  client.destroy()
  return { files: files.length, failed }
}
