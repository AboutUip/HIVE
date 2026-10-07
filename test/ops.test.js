import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { call, createHive, parseAddress, rebuildCentralIndexes, startCentral, startDr, startService } from '../dist/index.js'

const token = 'hive-ops-token'

function tempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'hive-ops-'))
}

async function boot(centralOptions = {}) {
  const root = tempDir()
  const central = await startCentral({
    dataDir: path.join(root, 'central'),
    token,
    host: '127.0.0.1',
    port: 0,
    checkIntervalMs: 60_000,
    nodes: [
      { id: 'svc-a', name: '华北节点', role: 'service', region: '华北', alias: 'svc-a' },
      { id: 'svc-b', name: '华东节点', role: 'service', region: '华东', alias: 'svc-b' },
      { id: 'dr-1', name: '灾备甲', role: 'dr', region: '集中', alias: 'dr-1', covers: ['svc-a', 'svc-b'] }
    ],
    ...centralOptions
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
  return {
    root,
    central,
    north,
    east,
    dr,
    async close() {
      await north.close()
      await east.close()
      await dr.close()
      await central.close()
      fs.rmSync(root, { recursive: true, force: true })
    }
  }
}

async function get(url) {
  const { host, port } = parseAddress(url)
  return call({ host, port, method: 'GET', path: '/v1/health', token })
}

test('三个角色都有 health 端点，形状完整', async () => {  const cluster = await boot()
  try {
    const central = await get(cluster.central.address)
    assert.equal(central.role, 'central')
    assert.equal(central.nodes.length, 2)
    assert.equal(central.mergeJob, null)
    assert.equal(typeof central.acceptedLogs, 'number')

    const north = await get(cluster.north.address)
    assert.equal(north.role, 'service')
    assert.equal(north.id, 'svc-a')
    assert.deepEqual(north.counters, { writes: 0, reads: 0, reports: 0 })

    const dr = await get(cluster.dr.address)
    assert.equal(dr.role, 'dr')
    assert.deepEqual(dr.covers, ['svc-a', 'svc-b'])

    await cluster.north.write({ userId: '甲', entries: [{ key: 'A', value: '甲一' }] })
    const after = await get(cluster.north.address)
    assert.equal(after.counters.writes, 1)
    assert.equal(after.fragments, 1)
    assert.equal(after.outbox, 1)
  } finally {
    await cluster.close()
  }
})

test('中央局面全丢后，从碎片和灾备重建索引', async () => {  const cluster = await boot()
  try {
    await cluster.north.write({ userId: '甲', entries: [{ key: 'A', value: '甲一' }] })
    await cluster.north.report()
    await cluster.central.organize({ force: true })

    const address = cluster.central.address
    const centralDir = path.join(cluster.root, 'central')
    await cluster.central.close()
    fs.rmSync(path.join(centralDir, 'state.json'), { force: true })
    fs.rmSync(path.join(centralDir, 'hive.wal'), { force: true })

    const rebuilt = await rebuildCentralIndexes({
      dataDir: centralDir,
      token,
      nodes: [
        { id: 'svc-a', name: '华北节点', role: 'service', region: '华北', alias: 'svc-a', ip: cluster.north.address },
        { id: 'svc-b', name: '华东节点', role: 'service', region: '华东', alias: 'svc-b', ip: cluster.east.address },
        { id: 'dr-1', name: '灾备甲', role: 'dr', region: '集中', alias: 'dr-1', ip: cluster.dr.address, covers: ['svc-a', 'svc-b'] }
      ]
    })
    assert.ok(rebuilt.masters >= 1, `应重建至少一条索引，实际 ${rebuilt.masters}`)
    assert.equal(rebuilt.pushed, 2)

    const central = await startCentral({
      dataDir: centralDir,
      token,
      host: '127.0.0.1',
      port: 0,
      checkIntervalMs: 60_000,
      nodes: [
        { id: 'svc-a', name: '华北节点', role: 'service', region: '华北', alias: 'svc-a' },
        { id: 'svc-b', name: '华东节点', role: 'service', region: '华东', alias: 'svc-b' },
        { id: 'dr-1', name: '灾备甲', role: 'dr', region: '集中', alias: 'dr-1', covers: ['svc-a', 'svc-b'] }
      ]
    })
    try {
      const master = central.store.listMasters().find((row) => row.data_key === 'A')
      assert.equal(master?.node_id, 'svc-a')
      assert.equal(master?.suspended, 0)
      assert.match(address, /^127\.0\.0\.1:/)
    } finally {
      await central.close()
    }
  } finally {
    await cluster.close()
  }
})

async function bootServices(extra) {
  const root = tempDir()
  const central = await startCentral({
    dataDir: path.join(root, 'central'),
    token,
    host: '127.0.0.1',
    port: 0,
    checkIntervalMs: 60_000,
    nodes: [
      { id: 'svc-a', name: '华北节点', role: 'service', region: '华北', alias: 'svc-a' },
      { id: 'svc-b', name: '华东节点', role: 'service', region: '华东', alias: 'svc-b' }
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
    reportIntervalMs: 60_000,
    ...(extra?.north || {})
  })
  const east = await startService({
    dataDir: path.join(root, 'svc-b'),
    token,
    id: 'svc-b',
    name: '华东节点',
    region: '华东',
    host: '127.0.0.1',
    central: link,
    reportIntervalMs: 60_000,
    ...(extra?.east || {})
  })
  return { root, central, north, east }
}

test('待上报积压超过上限时，写入被拒绝而不是继续堆积', async () => {
  const { root, central, north, east } = await bootServices({ north: { maxOutbox: 2 } })
  try {
    await north.write({ userId: '甲', entries: [{ key: 'A', value: '甲一' }] })
    await north.write({ userId: '甲', entries: [{ key: 'B', value: '甲二' }] })
    await assert.rejects(
      () => north.write({ userId: '甲', entries: [{ key: 'C', value: '甲三' }] }),
      /积压超过上限/
    )
    await assert.rejects(
      () => north.remove({ userId: '甲', keys: ['A'] }),
      /积压超过上限/
    )
  } finally {
    await north.close()
    await east.close()
    await central.close()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('连续取不到同一别名后熔断，health 里能看到', async () => {
  const { root, central, north, east } = await bootServices({
    east: { readBreakerFails: 2, readBreakerCooldownMs: 60_000, callTimeoutMs: 150 }
  })
  try {
    await north.write({ userId: '甲', entries: [{ key: 'A', value: '甲一' }] })
    await north.report()
    await north.close()

    const first = await east.read({ userId: '甲', keys: ['A'] })
    assert.equal(first.results[0].source, 'rejected')
    const second = await east.read({ userId: '甲', keys: ['A'] })
    assert.equal(second.results[0].source, 'rejected')

    const health = await get(east.address)
    const breaker = health.readBreakers.find((row) => row.alias === 'svc-a')
    assert.equal(breaker?.open, true, '连续失败两次后熔断应打开')
  } finally {
    await east.close()
    await central.close()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('留档文件超过上限时轮转成 .1', () => {
  const dir = tempDir()
  const hive = createHive({ dataDir: dir, retentionMaxBytes: 200 })
  hive.reset()
  hive.setRetention(true)
  for (let i = 0; i < 8; i += 1) {
    hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: `K${i}`, value: `值${i}一些留档内容` }] })
  }
  hive.advance('dr')
  for (let i = 8; i < 12; i += 1) {
    hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: `K${i}`, value: `值${i}一些留档内容` }] })
  }
  hive.advance('dr')
  const retained = path.join(dir, 'retained.log')
  assert.ok(fs.existsSync(retained), '留档文件应该存在')
  assert.ok(fs.existsSync(`${retained}.1`), '超过上限后应该轮转出 .1')
  const rotated = fs.readFileSync(`${retained}.1`, 'utf8')
  const latest = fs.readFileSync(retained, 'utf8')
  assert.ok(rotated.includes('K0'), '第一轮留档进了 .1')
  assert.ok(latest.includes('K11') && !latest.includes('K0'), '最新一轮留档在正本里')
  hive.close()
  fs.rmSync(dir, { recursive: true, force: true })
})
