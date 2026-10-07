import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { createHive, MemoryStore, startCentral, startService } from '../dist/index.js'

let SqliteStore = null
try {
  SqliteStore = (await import('../dist/sqlite.js')).SqliteStore
} catch {
  SqliteStore = null
}

const USERS = ['甲', '乙', '丙']
const KEYS = ['A', 'B', 'C', 'D', 'E']
const VALUES = ['甲一', '乙二', '丙三', '改写', '新值', '再改']

function prng(seed) {
  let s = seed >>> 0
  return () => {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0
    return s / 2 ** 32
  }
}

function byId(state, id) {
  return state.nodes.find((node) => node.id === id)
}

function serviceNodes(state) {
  return state.nodes.filter((node) => node.role === 'service')
}

function collectStamps(state) {
  const stamps = new Map()
  for (const node of serviceNodes(state)) {
    let max = null
    const rows = [...(node.fragments || []), ...(node.outbox || []), ...(node.indexEntries || [])]
    for (const row of rows) {
      if (!max || row.updated_at > max) max = row.updated_at
    }
    if (max) stamps.set(node.id, max)
  }
  return stamps
}

function checkInvariants(state, tag, failures) {
  const job = state.mergeJob
  const merging = serviceNodes(state).filter((node) => node.status === 'merging')

  for (const master of state.indexMaster) {
    const home = byId(state, master.node_id)
    if (home && home.role === 'service' && home.status === 'lost' && master.suspended !== 1) {
      failures.push(`I1 ${tag}: ${master.user_id}/${master.data_key} 的家 ${master.node_id} 已挂失但索引未挂起`)
    }
  }

  if (job) {
    const lost = byId(state, job.lostId)
    const target = byId(state, job.targetId)
    if (!lost || lost.status !== 'lost') failures.push(`I2 ${tag}: 融合任务里的丢失节点 ${job.lostId} 不是 lost`)
    if (!target || target.status !== 'merging') failures.push(`I2 ${tag}: 融合任务里的接替节点 ${job.targetId} 不是 merging`)
    if (merging.length !== 1) failures.push(`I2 ${tag}: 融合节点数不是 1，是 ${merging.length}`)
  } else if (merging.length > 0) {
    failures.push(`I2 ${tag}: 没有融合任务却有 merging 节点`)
  }

  if (job) {
    for (const node of serviceNodes(state)) {
      if (node.id === job.lostId || node.status === 'lost' || node.dbBroken) continue
      for (const entry of node.indexEntries || []) {
        if (entry.node_id === job.lostId && entry.suspended !== 1) {
          failures.push(`I3 ${tag}: ${node.id} 上指向丢失节点 ${job.lostId} 的索引未挂起`)
        }
      }
    }
  }

  for (const dr of state.nodes.filter((node) => node.role === 'dr')) {
    for (const record of dr.records || []) {
      if (!dr.covers.includes(record.home_node)) {
        failures.push(`I6 ${tag}: 灾备 ${dr.id} 有不在负责范围的记录 ${record.user_id}/${record.data_key}（家 ${record.home_node}）`)
      }
    }
  }
}

function checkRead(state, nodeId, user, key, result, failures) {
  if (result.source === 'local') {
    const node = byId(state, nodeId)
    const fragment = (node.fragments || []).find((row) => row.user_id === user && row.data_key === key)
    if (!fragment || fragment.value !== result.value) {
      failures.push(`I5 读取声称本机有 ${user}/${key}=${result.value}，但本机库里不是这份`)
    }
    return
  }
  if (result.source === 'remote') {
    const home = byId(state, result.fromNodeId)
    const fragment = (home?.fragments || []).find((row) => row.user_id === user && row.data_key === key)
    if (!fragment || fragment.value !== result.value) {
      failures.push(`I5 读取声称从 ${result.fromNodeId} 取回 ${user}/${key}=${result.value}，但那里不是这份`)
    }
  }
}

function checkConvergence(hive, failures) {
  const state = hive.getState()
  const up = serviceNodes(state).filter((node) => node.status === 'up' && !node.dbBroken && node.reachable)
  if (up.length === 0) return
  let checked = 0
  for (const master of state.indexMaster) {
    if (checked >= 8) break
    if (master.suspended === 1) continue
    const home = byId(state, master.node_id)
    if (!home || home.status !== 'up' || home.dbBroken) continue
    const expected = (home.fragments || []).find((row) => row.user_id === master.user_id && row.data_key === master.data_key)
    if (!expected) {
      failures.push(`I7 收敛：家 ${home.id} 上没有 ${master.user_id}/${master.data_key} 这份数据`)
      continue
    }
    for (const node of up) {
      const read = hive.read({ nodeId: node.id, userId: master.user_id, keys: [master.data_key] })
      const result = read.results[0]
      if (result.value !== expected.value || (result.source !== 'local' && result.source !== 'remote')) {
        failures.push(
          `I7 收敛：${node.id} 读 ${master.user_id}/${master.data_key} 得到 ${result.source}:${result.value}，期望 ${expected.value}（${result.message}）`
        )
      }
    }
    checked += 1
  }
}

function quiesce(hive, failures) {
  for (let i = 0; i < 60; i += 1) {
    try {
      hive.advance('sync')
    } catch {
      return
    }
  }
  failures.push('收敛推进超过上限，集群始终没有静下来')
}

function convergedState(hive) {
  const state = hive.getState()
  if (state.mergeJob) return false
  return serviceNodes(state).every(
    (node) => node.status === 'up' && !node.dbBroken && node.reachable
  )
}

function runHarness(seed, steps, makeHive, { restartEvery = 0 } = {}) {
  const rnd = prng(seed)
  const failures = []
  let hive = makeHive()
  let lastStamp = new Map()

  const syncLastStamp = () => {
    for (const [id, stamp] of collectStamps(hive.getState())) {
      const prev = lastStamp.get(id)
      if (!prev || stamp > prev) lastStamp.set(id, stamp)
    }
  }

  try {
    hive.reset()
    for (let step = 0; step < steps; step += 1) {
      const state = hive.getState()
      const up = serviceNodes(state).filter((node) => node.status === 'up' && !node.dbBroken)
      const roll = rnd()

      if (roll < 0.34 && up.length > 0) {
        const node = up[Math.floor(rnd() * up.length)]
        const user = USERS[Math.floor(rnd() * USERS.length)]
        const key = KEYS[Math.floor(rnd() * KEYS.length)]
        const value = VALUES[Math.floor(rnd() * VALUES.length)]
        const prev = lastStamp.get(node.id) || null
        hive.write({ nodeId: node.id, userId: user, entries: [{ key, value }] })
        const fragment = byId(hive.getState(), node.id).fragments.find(
          (row) => row.user_id === user && row.data_key === key
        )
        if (prev && !(fragment.updated_at > prev)) {
          failures.push(`I8 ${step}: ${node.id} 新写的时间戳 ${fragment.updated_at} 没有超过上一次的 ${prev}`)
        }
        if (fragment) lastStamp.set(node.id, fragment.updated_at)
      } else if (roll < 0.58 && up.length > 0) {
        const node = up[Math.floor(rnd() * up.length)]
        const user = USERS[Math.floor(rnd() * USERS.length)]
        const key = KEYS[Math.floor(rnd() * KEYS.length)]
        const read = hive.read({ nodeId: node.id, userId: user, keys: [key] })
        checkRead(hive.getState(), node.id, user, key, read.results[0], failures)
      } else if (roll < 0.66) {
        const holders = []
        for (const node of serviceNodes(state)) {
          for (const fragment of node.fragments || []) holders.push({ nodeId: node.id, ...fragment })
        }
        if (holders.length > 0) {
          const pick = holders[Math.floor(rnd() * holders.length)]
          try {
            hive.remove({ nodeId: pick.nodeId, userId: pick.user_id, keys: [pick.data_key] })
          } catch {
            // 状态变了，这次删除作罢。
          }
        }
      } else if (roll < 0.74) {
        hive.advance('one')
      } else if (roll < 0.79) {
        try {
          hive.advance('sync')
        } catch {
          // 没有节点到期，跳过。
        }
      } else if (roll < 0.84) {
        try {
          hive.advance('dr')
        } catch {
          // 没有节点到期，跳过。
        }
      } else if (roll < 0.88 && up.length > 0) {
        const node = up[Math.floor(rnd() * up.length)]
        try {
          hive.lose(node.id, { cause: '性质测试强制挂失' })
        } catch {
          // 没有接替节点或已有融合，跳过。
        }
      } else if (roll < 0.90 && state.mergeJob) {
        try {
          hive.finishMerge()
        } catch {
          // 回退或接替不可达，由不变式来验证局面。
        }
      } else if (roll < 0.93 && !state.mergeJob) {
        const joining = serviceNodes(state).filter((node) => node.status === 'joining')
        if (joining.length > 0 && rnd() < 0.5) {
          try {
            hive.ready(joining[0].id)
          } catch {
            // 状态变了，跳过。
          }
        } else {
          try {
            hive.join({ region: '西南', name: '西南节点' })
          } catch {
            // 没有源节点，跳过。
          }
        }
      } else if (roll < 0.96) {
        const node = serviceNodes(state)[Math.floor(rnd() * serviceNodes(state).length)]
        hive.setReachable(node.id, rnd() < 0.72)
      } else if (restartEvery > 0 && step % restartEvery === 0 && step > 0 && !state.mergeJob) {
        const prev = lastStamp
        hive.close()
        hive = makeHive()
        lastStamp = prev
        for (const [id, stamp] of collectStamps(hive.getState())) {
          const before = lastStamp.get(id)
          if (!before || stamp > before) lastStamp.set(id, stamp)
        }
      }

      checkInvariants(hive.getState(), `第 ${step} 步`, failures)
      syncLastStamp()
    }

    if (convergedState(hive)) {
      quiesce(hive, failures)
      checkInvariants(hive.getState(), '静默后', failures)
      if (convergedState(hive)) checkConvergence(hive, failures)
    }
  } finally {
    hive.close()
  }
  return failures
}

function tempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'hive-invariants-'))
}

test('不变式：随机操作 × 内存存储', () => {
  const store = new MemoryStore()
  const dir = tempDir()
  const failures = runHarness(101, 320, () => createHive({ dataDir: dir, store }), { restartEvery: 64 })
  fs.rmSync(dir, { recursive: true, force: true })
  assert.equal(failures.length, 0, failures.map((item) => `- ${item}`).join('\n'))
})

test('不变式：随机操作 × 文件存储（含重启）', () => {
  const dir = tempDir()
  const failures = runHarness(202, 260, () => createHive({ dataDir: dir }), { restartEvery: 52 })
  fs.rmSync(dir, { recursive: true, force: true })
  assert.equal(failures.length, 0, failures.map((item) => `- ${item}`).join('\n'))
})

test('不变式：随机操作 × SQLite 存储', { skip: !SqliteStore }, () => {
  const dir = tempDir()
  const failures = runHarness(303, 200, () => createHive({ dataDir: dir, store: new SqliteStore(dir) }))
  fs.rmSync(dir, { recursive: true, force: true })
  assert.equal(failures.length, 0, failures.map((item) => `- ${item}`).join('\n'))
})

test('时钟偏差一小时，因果在后的写仍然赢（生产路径）', async () => {
  const root = tempDir()
  const token = 'hive-skew-token'
  const base = Date.parse('2026-09-01T00:00:00.000Z')
  const central = await startCentral({
    dataDir: path.join(root, 'central'),
    token,
    host: '127.0.0.1',
    port: 0,
    clock: () => base,
    checkIntervalMs: 60_000,
    nodes: [
      { id: 'svc-a', name: '华北节点', role: 'service', region: '华北', alias: 'svc-a' },
      { id: 'svc-b', name: '华东节点', role: 'service', region: '华东', alias: 'svc-b' }
    ]
  })
  const link = { host: central.host, port: central.http.port }
  const ahead = await startService({
    dataDir: path.join(root, 'svc-a'),
    token,
    id: 'svc-a',
    name: '华北节点',
    region: '华北',
    host: '127.0.0.1',
    central: link,
    reportIntervalMs: 60_000,
    now: () => new Date(base + 3_600_000)
  })
  const behind = await startService({
    dataDir: path.join(root, 'svc-b'),
    token,
    id: 'svc-b',
    name: '华东节点',
    region: '华东',
    host: '127.0.0.1',
    central: link,
    reportIntervalMs: 60_000,
    now: () => new Date(base)
  })
  try {
    await ahead.write({ userId: '甲', entries: [{ key: 'K', value: '甲一' }] })
    await ahead.report()
    const first = ahead.store.getFragment('svc-a', '甲', 'K').updated_at

    await behind.write({ userId: '甲', entries: [{ key: 'K', value: '乙二' }] })
    const second = behind.store.getFragment('svc-b', '甲', 'K').updated_at
    assert.ok(second > first, `华东墙钟落后一小时，但它的时间戳 ${second} 必须晚于华北的 ${first}`)

    await behind.report()
    const read = await ahead.read({ userId: '甲', keys: ['K'] })
    assert.equal(read.results[0].value, '乙二')
    assert.equal(read.results[0].fromNodeId, 'svc-b')
    assert.equal(ahead.store.getFragment('svc-a', '甲', 'K'), null)
  } finally {
    await ahead.close()
    await behind.close()
    await central.close()
    fs.rmSync(root, { recursive: true, force: true })
  }
})
