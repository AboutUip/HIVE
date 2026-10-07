import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { startCentral, startDr, startService } from '../dist/index.js'

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'hive-example-'))
const token = 'example-token'
const central = await startCentral({
  dataDir: path.join(root, 'central'),
  token,
  host: '127.0.0.1',
  nodes: [
    { id: 'svc-a', name: '华北节点', role: 'service', region: '华北', alias: 'svc-a' },
    { id: 'svc-b', name: '华东节点', role: 'service', region: '华东', alias: 'svc-b' },
    { id: 'dr-1', name: '灾备甲', role: 'dr', region: '集中', alias: 'dr-1', covers: ['svc-a', 'svc-b'] }
  ]
})
const link = { host: central.host, port: central.http.port }
const north = await startService({
  dataDir: path.join(root, 'svc-a'),
  token,
  id: 'svc-a',
  name: '华北节点',
  region: '华北',
  host: '127.0.0.1',
  central: link,
  reportIntervalMs: 60_000
})
const east = await startService({
  dataDir: path.join(root, 'svc-b'),
  token,
  id: 'svc-b',
  name: '华东节点',
  region: '华东',
  host: '127.0.0.1',
  central: link,
  reportIntervalMs: 60_000
})
const dr = await startDr({
  dataDir: path.join(root, 'dr'),
  token,
  id: 'dr-1',
  name: '灾备甲',
  region: '集中',
  covers: ['svc-a', 'svc-b'],
  host: '127.0.0.1',
  central: link
})

await north.write({ userId: '林夏', entries: [{ key: '城市', value: '北京' }, { key: '电话', value: '13800000000' }] })
const listed = north.list({ userId: '林夏' })
console.log(`华北节点临时列出 ${listed.rows.length} 条地址，自身数据也写进这张表。用完即弃，不写回持久索引。`)
console.log(listed.rows.map((row) => `${row.data_key} → ${row.node_id} ${row.ip}`).join('，'))
await north.report()
const city = await east.read({ userId: '林夏', keys: ['城市'] })
console.log(`华东节点${city.results[0].message} ${city.results[0].key}=${city.results[0].value}`)
await north.remove({ userId: '林夏', keys: ['电话'] })
await north.report()
const phone = await east.read({ userId: '林夏', keys: ['电话'] })
console.log(`华东节点读取 电话：${phone.results[0].message}`)

await north.close()
await east.close()
await dr.close()
await central.close()
fs.rmSync(root, { recursive: true, force: true })
