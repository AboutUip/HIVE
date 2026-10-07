import fs from 'node:fs'
import { rebuildCentralIndexes } from '../dist/index.js'

const [, , dataDir, token, nodesPath] = process.argv
if (!dataDir || !token) {
  console.error('用法: node scripts/rebuild-index.js <中央数据目录> <集群令牌> [节点清单.json]')
  console.error('节点清单.json 在中央库是空的时候必须给：{ "nodes": [ { "id", "name", "role", "region", "ip", "alias", "covers" } ] }')
  process.exit(1)
}

const nodes = nodesPath ? JSON.parse(fs.readFileSync(nodesPath, 'utf8')).nodes || null : null

const result = await rebuildCentralIndexes({ dataDir, token, nodes })
console.log(JSON.stringify(result, null, 2))
