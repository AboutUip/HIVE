import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { createHive } from '../dist/index.js'

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'hive-demo-'))
const hive = createHive({ dataDir: dir })
hive.reset()

hive.write({
  nodeId: 'svc-a',
  userId: '林夏',
  entries: [
    { key: '城市', value: '北京' },
    { key: '电话', value: '13810248816' }
  ]
})
const listed = hive.list({ nodeId: 'svc-a', userId: '林夏' })
console.log(listed.message)
console.log(listed.rows.map((row) => `${row.data_key} → ${row.alias} ${row.ip}`).join('，'))

hive.advance('sync')
const read = hive.read({ nodeId: 'svc-b', userId: '林夏', keys: ['城市'] })
console.log(read.message)

hive.remove({ nodeId: 'svc-a', userId: '林夏', keys: ['电话'] })
hive.advance('sync')
const gone = hive.read({ nodeId: 'svc-b', userId: '林夏', keys: ['电话'] })
console.log(gone.message)

hive.close()
fs.rmSync(dir, { recursive: true, force: true })
