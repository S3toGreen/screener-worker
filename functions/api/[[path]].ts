/**
 * Pages Functions entry: every `/api/*` request (the landing page and any future
 * static files are served from `public/` instead).
 */
import type { ScreenerEnv } from '../../src/env'
import { serveApi } from '../../src/serve'

export const onRequest: PagesFunction<ScreenerEnv> = (ctx) => serveApi(ctx.request, ctx.env, ctx)
