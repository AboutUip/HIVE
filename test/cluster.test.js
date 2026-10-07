import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { startCentral, startDr, startService } from '../dist/index.js'

const token = 'hive-test-token'

function tempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'hive-cluster-'))
}

function serviceSpec(id, name, region) {
  return { id, name, role: 'service', region, alias: id }
}

async function boot(clock) {
  const root = tempDir()
  const central = await startCentral({
    dataDir: path.join(root, 'central'),
    token,
    host: '127.0.0.1',
    port: 0,
    clock,
    reportIntervalMs: 1000,
    drIntervalMs: 5000,
    toleranceMs: 500,
    checkIntervalMs: 60_000,
    nodeSeq: 4,
    nodes: [
      serviceSpec('svc-a', '华北节点', '华北'),
      serviceSpec('svc-b', '华东节点', '华东'),
      serviceSpec('svc-c', '华南节点', '华南'),
      { id: 'dr-1', name: '灾备甲', role: 'dr', region: '集中', alias: 'dr-1', covers: ['svc-a', 'svc-b', 'svc-c'] }
    ]
  })
  const link = { host: central.host, port: central.http.port }
  const dr = await startDr({
    dataDir: path.join(root, 'dr'),
    token,
    id: 'dr-1',
    name: '灾备甲',
    region: '集中',
    covers: ['svc-a', 'svc-b', 'svc-c'],
    host: '127.0.0.1',
    central: link
  })
  const services = {}
  for (const spec of [
    ['svc-a', '华北节点', '华北'],
    ['svc-b', '华东节点', '华东'],
    ['svc-c', '华南节点', '华南']
  ]) {
    services[spec[0]] = await startService({
      dataDir: path.join(root, spec[0]),
      token,
      id: spec[0],
      name: spec[1],
      region: spec[2],
      host: '127.0.0.1',
      central: link,
      reportIntervalMs: 60_000,
      now: () => new Date(clock())
    })
  }
  return {
    root,
    central,
    dr,
    ...services,
    async close() {
      await services['svc-a'].close()
      await services['svc-b'].close()
      await services['svc-c'].close()
      await dr.close()
      await central.close()
      fs.rmSync(root, { recursive: true, force: true })
    }
  }
}

test('三个服务节点经中央完成写入、直达读取和删除', async () => {
  let now = Date.parse('2026-04-01T00:00:00.000Z')
  const clock = () => now
  const cluster = await boot(clock)
  try {
    const before = await cluster['svc-b'].read({ userId: '林夏', keys: ['城市'] })
    assert.equal(before.results[0].source, 'rejected')

    await cluster['svc-a'].write({ userId: '林夏', entries: [{ key: '城市', value: '北京' }] })
    await cluster['svc-a'].report()
    const read = await cluster['svc-b'].read({ userId: '林夏', keys: ['城市'] })
    assert.equal(read.results[0].source, 'remote')
    assert.equal(read.results[0].value, '北京')
    assert.equal(read.results[0].storedLocally, false)
    assert.equal(cluster['svc-b'].store.countFragments('svc-b'), 0)

    await cluster['svc-a'].remove({ userId: '林夏', keys: ['城市'] })
    await cluster['svc-a'].report()
    const gone = await cluster['svc-c'].read({ userId: '林夏', keys: ['城市'] })
    assert.equal(gone.results[0].source, 'rejected')
  } finally {
    await cluster.close()
  }
})

test('节点失联后从灾备装入另一台，其他节点按新地址读取', async () => {
  let now = 1_000_000
  const clock = () => now
  const cluster = await boot(clock)
  try {
    await cluster['svc-a'].write({ userId: '甲', entries: [{ key: 'A', value: '甲一' }] })
    await cluster['svc-a'].report()
    await cluster.central.organize({ force: true })
    await cluster['svc-a'].close()
    now += 1000 + 500
    await cluster.central.checkLiveness()
    await cluster.central.checkLiveness()
    const read = await cluster['svc-c'].read({ userId: '甲', keys: ['A'] })
    assert.equal(read.results[0].value, '甲一')
    assert.equal(read.results[0].source, 'remote')
    assert.equal(read.results[0].storedLocally, false)
    assert.match(read.results[0].fromIp, /127\.0\.0\.1:/)
  } finally {
    await cluster['svc-b'].close()
    await cluster['svc-c'].close()
    await cluster.dr.close()
    await cluster.central.close()
    fs.rmSync(cluster.root, { recursive: true, force: true })
  }
})

test('随机写入上报后，任意节点读到的都是较新的一份', async () => {
  let now = Date.parse('2026-05-01T00:00:00.000Z')
  const clock = () => {
    now += 1
    return now
  }
  const cluster = await boot(clock)
  try {
    let seed = 1
    const next = () => {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0
      return seed
    }
    const nodes = [cluster['svc-a'], cluster['svc-b'], cluster['svc-c']]
    const keys = ['A', 'B', 'C', 'D']
    const latest = new Map()
    for (let step = 0; step < 36; step += 1) {
      const node = nodes[next() % nodes.length]
      const key = keys[next() % keys.length]
      const value = `v${step}`
      await node.write({ userId: '甲', entries: [{ key, value }] })
      await node.report()
      latest.set(key, value)
    }
    for (const node of nodes) {
      const read = await node.read({ userId: '甲', keys: [...latest.keys()] })
      for (const item of read.results) assert.equal(item.value, latest.get(item.key), item.key)
    }
  } finally {
    await cluster.close()
  }
})

test('到了上报间隔，另一台节点不用手动上报也能读到', async () => {
  let now = Date.parse('2026-07-01T00:00:00.000Z')
  const root = tempDir()
  const central = await startCentral({
    dataDir: path.join(root, 'central'),
    token,
    host: '127.0.0.1',
    port: 0,
    clock: () => now,
    checkIntervalMs: 60_000,
    nodes: [serviceSpec('svc-a', '华北节点', '华北'), serviceSpec('svc-b', '华东节点', '华东')]
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
    reportIntervalMs: 40,
    now: () => new Date(now)
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
    now: () => new Date(now)
  })
  try {
    await north.write({ userId: '林夏', entries: [{ key: '城市', value: '北京' }] })
    const deadline = Date.now() + 2000
    let value
    while (Date.now() < deadline) {
      const read = await east.read({ userId: '林夏', keys: ['城市'] })
      if (read.results[0].value === '北京') {
        value = read.results[0].value
        break
      }
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
    assert.equal(value, '北京')
  } finally {
    await north.close()
    await east.close()
    await central.close()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('原地址不通时，按别名向中央要一次新地址再读取', async () => {
  let now = Date.parse('2026-08-01T00:00:00.000Z')
  const clock = () => now
  const cluster = await boot(clock)
  const link = { host: cluster.central.host, port: cluster.central.http.port }
  const dir = cluster['svc-a'].dataDir
  try {
    await cluster['svc-a'].write({ userId: '甲', entries: [{ key: 'A', value: '换地址' }] })
    await cluster['svc-a'].report()
    await cluster['svc-a'].close()
    const again = await startService({
      dataDir: dir,
      token,
      id: 'svc-a',
      name: '华北节点',
      region: '华北',
      host: '127.0.0.1',
      central: link,
      reportIntervalMs: 60_000,
      now: () => new Date(clock())
    })
    const read = await cluster['svc-b'].read({ userId: '甲', keys: ['A'] })
    assert.equal(read.results[0].value, '换地址')
    assert.equal(read.results[0].via, 'alias')
    assert.equal(read.results[0].storedLocally, false)
    await again.close()
  } finally {
    await cluster['svc-b'].close()
    await cluster['svc-c'].close()
    await cluster.dr.close()
    await cluster.central.close()
    fs.rmSync(cluster.root, { recursive: true, force: true })
  }
})

test('服务节点重启后仍能读到自己写下的数据', async () => {
  let now = Date.parse('2026-06-01T00:00:00.000Z')
  const clock = () => now
  const cluster = await boot(clock)
  const link = { host: cluster.central.host, port: cluster.central.http.port }
  const dir = cluster['svc-a'].dataDir
  try {
    await cluster['svc-a'].write({ userId: '甲', entries: [{ key: 'A', value: '还在' }] })
    await cluster['svc-a'].close()
    const again = await startService({
      dataDir: dir,
      token,
      id: 'svc-a',
      name: '华北节点',
      region: '华北',
      host: '127.0.0.1',
      central: link,
      reportIntervalMs: 60_000,
      now: () => new Date(clock())
    })
    const read = await again.read({ userId: '甲', keys: ['A'] })
    assert.equal(read.results[0].source, 'local')
    assert.equal(read.results[0].value, '还在')
    await again.close()
  } finally {
    await cluster['svc-b'].close()
    await cluster['svc-c'].close()
    await cluster.dr.close()
    await cluster.central.close()
    fs.rmSync(cluster.root, { recursive: true, force: true })
  }
})
