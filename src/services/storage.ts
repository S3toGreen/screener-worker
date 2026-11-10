/**
 * The ONE async persistence seam for shared services. The desktop binds it to
 * `lib/settings`' primitives (the single localStorage owner); a mobile shell
 * binds `@capacitor/preferences`; a plain-web host would bind localStorage.
 *
 * Shared is DOM-lib free by convention (`wsClient.ts` does the same), so any
 * platform store is declared structurally at the binding site.
 */
export interface StorageAdapter {
  /** The parsed value, or `null` when the key is absent/corrupt. */
  get<T>(key: string): Promise<T | null>
  set<T>(key: string, value: T): Promise<void>
  remove(key: string): Promise<void>
}
