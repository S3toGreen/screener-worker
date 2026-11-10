/**
 * One daily ETL run from the workstation, then the bucket upload — the same
 * command the GitHub Actions schedule runs.
 *
 *   bun seed.ts             # ETL + upload
 *   bun seed.ts --force    # rewrite today's session
 *   bun seed.ts --dry-run  # files only
 *
 * Without `--force` the ETL's own freshness gate skips a session the bucket
 * already holds (weekend / holiday / duplicate dispatch ⇒ zero R2 writes), so
 * a holiday run exits successfully having written nothing.
 *
 * Exit codes: 0 complete · 3 incomplete (a FAILED source after its forced
 * retry, or a source that legitimately has not published yet — `late`) ·
 * 1 storage failure. A partial run still uploads what it built — the snapshot
 * carries every row it could (a carried row is better than none). A local run
 * without R2 credentials performs the ETL, leaves `.seed-out/`, and exits 0.
 *
 * THE RETRY RULE: only a FAILED read (a thrown source after its own retry)
 * triggers the forced retry. A source that answered "not this session yet" is
 * `late` — retrying seconds later cannot change that, so the LATER cron slot
 * (21:15, then the 03:45 guard) repairs it.
 */
import { createFsEnv, resetSeedRoot, uploadConfigFromEnv, uploadSeedRoot } from './seedFs'
import { runEtl, type EtlResult } from './src/etl'

const dryRun = process.argv.includes('--dry-run')
const force = process.argv.includes('--force')

if (!process.argv.includes('--keep')) await resetSeedRoot()
const started = Date.now()
let result: EtlResult = await runEtl(createFsEnv(), Date.now(), { force })
console.log(`[seed] etl ${JSON.stringify(result)} in ${Date.now() - started} ms`)

// ONE forced retry for FAILED reads only: a transient outage (a WAF HTML page
// instead of JSON, a reset socket) clears within seconds, and a force re-run
// must heal the same session's history (the freshness gate is bypassed, the
// append preserves every value the first pass published). This holds for the
// 03:45 guard too — a forced run whose FIRST pass failed a source is exactly
// the case a second attempt can still fix. `late` sources (the exchange simply
// has not published them yet) NEVER trigger a retry: the later cron slot is
// the retry.
if (result.partial.length > 0) {
  console.log(`[seed] partial (${result.partial.join(',')}) — retrying with --force`)
  const retryStarted = Date.now()
  result = await runEtl(createFsEnv(), Date.now(), { force: true })
  console.log(`[seed] retry ${JSON.stringify(result)} in ${Date.now() - retryStarted} ms`)
}
if (result.late.length > 0) {
  console.log(`[seed] not yet published: ${result.late.join(' ')} — the later slot repairs them`)
}

const incomplete = result.partial.length + result.late.length
if (result.skipped) {
  console.log('[seed] nothing to upload (the freshness gate skipped this session)')
  process.exit(incomplete > 0 ? 3 : 0)
}

// A standalone checkout with no `.dev.vars` has no R2 credentials: the clean
// `bun run seed` must still complete the ETL and leave `.seed-out/` for
// inspection. CI is different — `createFsEnv` already throws there when the
// secrets are missing, so a green production run can never silently skip R2.
let upload = !dryRun
if (upload) {
  try {
    uploadConfigFromEnv()
  } catch (err) {
    console.log(`[seed] ${String(err).split('\n')[0]} — upload skipped (local .seed-out only)`)
    upload = false
  }
}
const { files, failed } = upload ? await uploadSeedRoot() : { files: 0, failed: 0 }
if (!upload) console.log('[seed] objects left in .seed-out/ (not uploaded)')
else console.log(`[seed] done: ${files - failed}/${files} uploaded, ${failed} failed`)
if (failed > 0) process.exit(1)
// A credentials-less local run is a filesystem rehearsal: late sources and a
// partial grid are informational, not a failed production publish. CI never
// reaches this branch (its missing secrets fail in createFsEnv).
if (!upload) process.exit(0)
if (incomplete > 0) {
  // `force` means THIS run was the retry (the catch-up cron / a dispatch).
  const detail = [
    result.partial.length > 0 ? `failed: ${result.partial.join(' ')}` : '',
    result.late.length > 0 ? `not yet published: ${result.late.join(' ')}` : ''
  ]
    .filter(Boolean)
    .join(' · ')
  console.error(
    `[seed] INCOMPLETE${force ? ' (already forced, no retry)' : ' after the forced retry'}: ${detail}`
  )
  process.exit(3)
}
process.exit(0)
