import { rmSync } from 'node:fs'
import * as esbuild from 'esbuild'

rmSync('dist', { recursive: true, force: true })

await esbuild.build({
  entryPoints: ['src/index.js', 'src/sqlite.js'],
  bundle: true,
  platform: 'node',
  format: 'esm',
  outdir: 'dist',
  legalComments: 'none',
  logLevel: 'info'
})
