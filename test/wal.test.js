import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { createHive, FileStore } from '../dist/index.js'

function tempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'hive-wal-'))
}

function nodeOf(state, id) {
  return state.nodes.find((item) => item.id === id)
}

test('只有 WAL 增量时，重开也能回放完整局面', () => {
  const dir = tempDir()
  const store = new FileStore(dir, { snapshotEvery: 1000 })
  const hive = createHive({ dataDir: dir, store })
  hive.reset()
  for (let i = 0; i < 6; i += 1) {
    hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: `K${i}`, value: `值${i}` }] })
  }
  hive.advance('sync')
  assert.ok(fs.existsSync(path.join(dir, 'hive.wal')), '快照间隔大时变化只进 WAL')
  hive.close()

  const again = createHive({ dataDir: dir })
  const state = again.getState()
  assert.equal(nodeOf(state, 'svc-a').fragments.length, 6)
  assert.equal(nodeOf(state, 'svc-a').fragments.find((row) => row.data_key === 'K5').value, '值5')
  assert.equal(state.tick, 2)
  assert.ok(!fs.existsSync(path.join(dir, 'hive.wal')), '回放后 WAL 清掉')
  again.close()
  fs.rmSync(dir, { recursive: true, force: true })
})

test('写够次数后快照归并，state.json 更新、WAL 清空', () => {
  const dir = tempDir()
  const store = new FileStore(dir, { snapshotEvery: 4 })
  const hive = createHive({ dataDir: dir, store })
  hive.reset()
  for (let i = 0; i < 8; i += 1) {
    hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: `K${i}`, value: `值${i}` }] })
  }
  assert.ok(fs.existsSync(path.join(dir, 'state.json')), '写够次数后快照文件已经出现')
  hive.close()
  const again = createHive({ dataDir: dir })
  assert.equal(nodeOf(again.getState(), 'svc-a').fragments.length, 8)
  assert.ok(!fs.existsSync(path.join(dir, 'hive.wal')), '重开后 WAL 已回放并清空')
  again.close()
  fs.rmSync(dir, { recursive: true, force: true })
})

test('WAL 里有一行写坏的，重开跳过它，其余照常', () => {
  const dir = tempDir()
  const store = new FileStore(dir, { snapshotEvery: 1000 })
  const hive = createHive({ dataDir: dir, store })
  hive.reset()
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '甲一' }] })
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'B', value: '甲二' }] })
  fs.appendFileSync(path.join(dir, 'hive.wal'), '{"tables":{"fragments":{')
  hive.close()

  const again = createHive({ dataDir: dir })
  const fragments = nodeOf(again.getState(), 'svc-a').fragments
  assert.equal(fragments.length, 2, '坏行不影响前面已经写好的')
  again.close()
  fs.rmSync(dir, { recursive: true, force: true })
})

test('失败的事务不进 WAL，重开后局面停在失败之前', () => {
  const dir = tempDir()
  const store = new FileStore(dir, { snapshotEvery: 1000 })
  const hive = createHive({ dataDir: dir, store })
  hive.reset()
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '甲一' }] })
  assert.throws(
    () => hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'B', value: '' }] }),
    /还没有内容/
  )
  hive.close()

  const again = createHive({ dataDir: dir })
  const fragments = nodeOf(again.getState(), 'svc-a').fragments
  assert.equal(fragments.length, 1)
  assert.equal(fragments[0].data_key, 'A')
  again.close()
  fs.rmSync(dir, { recursive: true, force: true })
})
