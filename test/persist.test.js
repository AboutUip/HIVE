import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { createHive, MemoryStore } from '../dist/index.js'

let SqliteStore = null
try {
  SqliteStore = (await import('../dist/sqlite.js')).SqliteStore
} catch {
  SqliteStore = null
}

function tempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'hive-persist-'))
}

function nodeOf(hive, id) {
  return hive.getState().nodes.find((item) => item.id === id)
}

test('关闭后用同一个目录重新打开', () => {
  const dir = tempDir()
  const clock = new Date('2026-03-01T00:00:00.000Z')
  const hive = createHive({ dataDir: dir, now: () => clock })
  assert.throws(() => hive.getState(), /还没有局面/)
  hive.reset()
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '先写' }] })
  hive.advance('sync')
  hive.close()
  assert.throws(() => hive.getState(), /库已关闭/)

  const again = createHive({ dataDir: dir, now: () => clock })
  const state = again.getState()
  assert.equal(state.tick, 2)
  assert.equal(nodeOf(again, 'svc-a').fragments.find((row) => row.data_key === 'A').value, '先写')
  assert.equal(nodeOf(again, 'svc-b').indexEntries.find((row) => row.data_key === 'A').node_id, 'svc-a')
  again.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'B', value: '后写' }] })
  const stamps = nodeOf(again, 'svc-a').fragments.map((row) => row.updated_at)
  assert.ok(stamps.find((item) => item.endsWith('.001Z')))
  again.close()
  fs.rmSync(dir, { recursive: true, force: true })
})

test('装入中断后重开，局面仍停在这次提交之前', () => {
  const dir = tempDir()
  const hive = createHive({ dataDir: dir })
  hive.reset()
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '甲一' }] })
  hive.advance('dr')
  hive.lose('svc-a')
  hive.close()
  const again = createHive({ dataDir: dir })
  assert.equal(again.getState().mergeJob.targetId, 'svc-b')
  again.finishMerge()
  assert.equal(nodeOf(again, 'svc-b').fragments.find((row) => row.data_key === 'A').value, '甲一')
  assert.equal(again.getState().mergeJob, null)
  again.close()
  fs.rmSync(dir, { recursive: true, force: true })
})

test('一个键的碎片和待上报一起提交，装入中途失败不留半套地址', { skip: !SqliteStore }, () => {
  class BoomStore extends SqliteStore {
    constructor(dataDir) {
      super(dataDir)
      this.mode = ''
      this.loads = 0
    }

    putFragment(holderId, row) {
      if (this.mode === 'merge') {
        this.loads += 1
        if (this.loads === 2) {
          const error = new Error('写到一半')
          error.expose = true
          throw error
        }
      }
      if (this.mode === 'tear') {
        super.putFragment(holderId, row)
        const error = new Error('碎片写了但队列还没有')
        error.expose = true
        throw error
      }
      return super.putFragment(holderId, row)
    }
  }

  const dir = tempDir()
  const store = new BoomStore(dir)
  const hive = createHive({ dataDir: dir, store })
  hive.reset()
  store.mode = 'tear'
  assert.throws(
    () => hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '甲一' }] }),
    /碎片写了但队列还没有/
  )
  assert.equal(nodeOf(hive, 'svc-a').fragments.length, 0)
  assert.equal(nodeOf(hive, 'svc-a').outbox.length, 0)
  store.mode = ''
  assert.throws(
    () =>
      hive.write({
        nodeId: 'svc-a',
        userId: '甲',
        entries: [
          { key: 'A', value: '甲一' },
          { key: 'B', value: '' }
        ]
      }),
    /还没有内容/
  )
  assert.equal(nodeOf(hive, 'svc-a').fragments.length, 1)
  assert.equal(nodeOf(hive, 'svc-a').outbox.length, 1)

  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'B', value: '乙二' }] })
  hive.advance('dr')
  hive.lose('svc-a')
  store.mode = 'merge'
  const rolled = hive.finishMerge()
  assert.match(rolled.message, /写到一半/)
  assert.equal(nodeOf(hive, 'svc-b').status, 'lost')
  assert.equal(nodeOf(hive, 'svc-a').status, 'lost')
  assert.equal(hive.getState().mergeJob, null)
  assert.equal(nodeOf(hive, 'svc-b').fragments.length, 0)
  const home = nodeOf(hive, 'dr-1').records.filter((row) => row.user_id === '甲')
  assert.deepEqual(
    home.map((row) => row.home_node),
    ['svc-a', 'svc-a']
  )
  hive.close()
  fs.rmSync(dir, { recursive: true, force: true })
})

test('内存存储走同一套引擎', () => {
  const dir = tempDir()
  const store = new MemoryStore()
  const hive = createHive({ dataDir: dir, store })
  hive.reset()
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '甲一' }] })
  hive.advance('sync')
  hive.close()
  const again = createHive({ dataDir: dir, store })
  const read = again.read({ nodeId: 'svc-b', userId: '甲', keys: ['A'] })
  assert.equal(read.results[0].source, 'remote')
  assert.equal(read.results[0].value, '甲一')
  assert.equal(read.results[0].storedLocally, false)
  again.close()
  fs.rmSync(dir, { recursive: true, force: true })
})
