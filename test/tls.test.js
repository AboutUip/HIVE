import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'
import { startCentral, startDr, startService } from '../dist/index.js'

const here = path.dirname(fileURLToPath(import.meta.url))
const key = fs.readFileSync(path.join(here, 'fixtures', 'tls-key.pem'))
const cert = fs.readFileSync(path.join(here, 'fixtures', 'tls-cert.pem'))
const otherCert = fs.readFileSync(path.join(here, 'fixtures', 'tls-other-cert.pem'))

function tempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'hive-tls-'))
}

test('整个集群跑在 TLS 上，写入、直达读取、恢复照常', async () => {
  const root = tempDir()
  const token = 'hive-tls-token'
  const tls = { key, cert, ca: cert }
  const central = await startCentral({
    dataDir: path.join(root, 'central'),
    token,
    host: '127.0.0.1',
    port: 0,
    tls,
    checkIntervalMs: 60_000,
    nodes: [
      { id: 'svc-a', name: '华北节点', role: 'service', region: '华北', alias: 'svc-a' },
      { id: 'svc-b', name: '华东节点', role: 'service', region: '华东', alias: 'svc-b' },
      { id: 'dr-1', name: '灾备甲', role: 'dr', region: '集中', alias: 'dr-1', covers: ['svc-a', 'svc-b'] }
    ]
  })
  assert.equal(central.http.tls, true)
  const link = { host: central.host, port: central.http.port }
  const north = await startService({
    dataDir: path.join(root, 'svc-a'),
    token,
    id: 'svc-a',
    name: '华北节点',
    region: '华北',
    host: '127.0.0.1',
    central: link,
    tls,
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
    tls,
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
    central: link,
    tls
  })
  try {
    await north.write({ userId: '甲', entries: [{ key: 'A', value: '加密链路' }] })
    await north.report()
    const read = await east.read({ userId: '甲', keys: ['A'] })
    assert.equal(read.results[0].value, '加密链路')
    assert.equal(read.results[0].fromNodeId, 'svc-a')
  } finally {
    await north.close()
    await east.close()
    await dr.close()
    await central.close()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('CA 不对时握手失败，节点连不上中央', async () => {
  const root = tempDir()
  const token = 'hive-tls-token'
  const central = await startCentral({
    dataDir: path.join(root, 'central'),
    token,
    host: '127.0.0.1',
    port: 0,
    tls: { key, cert, ca: cert },
    checkIntervalMs: 60_000,
    nodes: [{ id: 'svc-a', name: '华北节点', role: 'service', region: '华北', alias: 'svc-a' }]
  })
  try {
    await assert.rejects(
      () =>
        startService({
          dataDir: path.join(root, 'svc-a'),
          token,
          id: 'svc-a',
          name: '华北节点',
          region: '华北',
          host: '127.0.0.1',
          central: { host: central.host, port: central.http.port },
          tls: { ca: otherCert },
          helloTimeoutMs: 400
        }),
      /无法连接/
    )
  } finally {
    await central.close()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('中央跑 TLS、服务节点明文去连，同样连不上', async () => {
  const root = tempDir()
  const token = 'hive-tls-token'
  const central = await startCentral({
    dataDir: path.join(root, 'central'),
    token,
    host: '127.0.0.1',
    port: 0,
    tls: { key, cert, ca: cert },
    checkIntervalMs: 60_000,
    nodes: [{ id: 'svc-a', name: '华北节点', role: 'service', region: '华北', alias: 'svc-a' }]
  })
  try {
    await assert.rejects(
      () =>
        startService({
          dataDir: path.join(root, 'svc-a'),
          token,
          id: 'svc-a',
          name: '华北节点',
          region: '华北',
          host: '127.0.0.1',
          central: { host: central.host, port: central.http.port },
          helloTimeoutMs: 400
        }),
      /无法连接/
    )
  } finally {
    await central.close()
    fs.rmSync(root, { recursive: true, force: true })
  }
})
