import { rmSync } from 'node:fs'
import * as esbuild from 'esbuild'

rmSync('dist', { recursive: true, force: true })

await esbuild.build({
  entryPoints: ['src/index.js'],
  bundle: true,
  platform: 'node',
  format: 'esm',
  outfile: 'dist/index.js',
  legalComments: 'none',
  logLevel: 'info'
})
