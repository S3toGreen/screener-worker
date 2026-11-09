/**
 * The bucket keys + the ONE cache-control pair — a leaf module so the HTTP
 * handler does not pull the ETL (and its ~20 exchange readers) into the
 * request bundle.
 */
/** Edge TTL: the payloads change once per session; a publication stamp
 *  (`cache-version.json`) invalidates the grid immediately, so the half hour is
 *  only the failure bound (and the history objects' own refresh window). */
export const DATA_CACHE_CONTROL = 'public, max-age=300, s-maxage=1800, stale-while-revalidate=86400'
/** The publication stamp the ETL writes AFTER the snapshot — non-history edge
 *  entries key on it, so a publish replaces the cached grid at every colo. */
export const CACHE_VERSION_KEY = 'cache-version.json'

/** The weekly TDCC 集保 object (one small file, refilled by `holdings.ts`). */
export const HOLDINGS_KEY = 'holdings.json'

/** The accumulated ex-dividend calendar (the daily ETL folds each window in). */
export const DIVIDENDS_KEY = 'dividends.json'

/** The R2 folder of the per-symbol histories (edge-invalidation excluded). */
export const HISTORY_KEY_PREFIX = 'history/'

export function historyKey(symbol: string): string {
  return `${HISTORY_KEY_PREFIX}${symbol}.json`
}
