import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'
import { Hive } from '../dist/index.js'

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'hive-stage1-'))
const hive = new Hive(dir)
const failures = []

function check(condition, message) {
  if (!condition) failures.push(message)
}

function node(id) {
  return hive.getState().nodes.find((item) => item.id === id)
}

function keysOf(rows, field = 'data_key') {
  return rows.map((row) => row[field]).join(',')
}

function fragment(id, key, user = '甲') {
  return node(id).fragments.find((row) => row.user_id === user && row.data_key === key)
}

function indexAt(id, key, user = '甲') {
  return node(id).indexEntries.find((row) => row.user_id === user && row.data_key === key)
}

function master(key, user = '甲') {
  return hive.getState().indexMaster.find((row) => row.user_id === user && row.data_key === key)
}

function writeAbc() {
  hive.write({
    nodeId: 'svc-a',
    userId: '甲',
    entries: [
      { key: 'A', value: '甲一' },
      { key: 'B', value: '甲二' },
      { key: 'C', value: '甲三' }
    ]
  })
}

function scatter() {
  hive.reset()
  writeAbc()
  check(node('svc-a').fragments.length === 3, '写入后华北应有三份碎片')
  check(node('svc-a').outbox.length === 3, '日志应先留在华北，不立刻到中央')
  check(node('svc-b').fragments.length === 0, '华东此时不该有碎片')

  const early = hive.read({ nodeId: 'svc-b', userId: '甲', keys: ['A', 'B', 'C'] })
  check(early.results.every((item) => item.source === 'rejected'), '同步前华东应拒绝')
  check(node('svc-b').fragments.length === 0, '拒绝时华东不落库')

  hive.advance('sync')
  check(hive.getState().tick === 2, '第一次同步应落在第 2 拍')
  check(node('svc-a').indexEntries.length === 0, '交日志的华北不接收这次索引')
  check(node('svc-b').indexEntries.length === 3, '华东应拿到三份地址')
  check(node('svc-c').indexEntries.length === 3, '华南应拿到三份地址')
  check(hive.getState().indexMaster.every((row) => row.node_id === 'svc-a'), '中央索引应指向华北')

  const fetched = hive.read({ nodeId: 'svc-b', userId: '甲', keys: ['A', 'B', 'C'] })
  check(fetched.results.every((item) => item.source === 'remote' && item.storedLocally === false), '华东应直达华北且不落库')
  check(node('svc-b').fragments.length === 0, '调取后华东库仍是空的')

  hive.write({
    nodeId: 'svc-b',
    userId: '甲',
    entries: [
      { key: 'C', value: '甲三改' },
      { key: 'D', value: '甲四' }
    ]
  })
  check(fragment('svc-a', 'C')?.value === '甲三', '同步前华北仍保留旧 C')
  check(fragment('svc-b', 'C')?.value === '甲三改' && fragment('svc-b', 'D')?.value === '甲四', '华东只保存 CD')

  const stale = hive.read({ nodeId: 'svc-c', userId: '甲', keys: ['C'] })
  check(stale.results[0].source === 'remote' && stale.results[0].value === '甲三', '窗口内华南仍会取到旧 C')
  check(!fragment('svc-c', 'C'), '华南取到旧 C 后不保存')

  hive.advance('sync')
  check(keysOf(node('svc-a').fragments) === 'A,B', '同步后华北只剩 AB')
  check(keysOf(node('svc-b').fragments) === 'C,D', '华东仍是 CD')
  check(master('C').node_id === 'svc-b' && master('D').node_id === 'svc-b', 'C 和 D 的地址改为华东')
  check(master('A').node_id === 'svc-a', '没被改过的 A 仍在华北')
  check(indexAt('svc-a', 'C')?.node_id === 'svc-b', '华北的索引副本应指向华东的 C')
  check(indexAt('svc-b', 'C')?.node_id === 'svc-a', '华东不接收自己刚交的索引，地址副本仍停在旧的华北')
  check(fragment('svc-b', 'C')?.value === '甲三改', '华东读取时以本地新碎片为准')

  const missing = hive.read({ nodeId: 'svc-c', userId: '甲', keys: ['E'] })
  check(missing.results[0].source === 'rejected', '没有索引的 E 应拒绝')
}

function disaster() {
  hive.advance('dr')
  const dr = node('dr-1')
  check(dr.records.length === 4, '五条日志应收成四条稳定数据')
  check(hive.getState().logCount === 0, '整理进灾备后，用过的日志从中央删除')
  const recordC = dr.records.find((row) => row.data_key === 'C')
  check(recordC?.value === '甲三改' && recordC.home_node === 'svc-b', '灾备里的 C 是整理后的新值，住在华东')
  check(dr.records.find((row) => row.data_key === 'A')?.home_node === 'svc-a', '灾备里的 A 住在华北')
}

function failover() {
  hive.write({ nodeId: 'svc-c', userId: '甲', entries: [{ key: 'A', value: '华南新写' }] })
  check(node('svc-c').fragmentCount === 1, '融合前华南先有自己的一份 A')
  const lost = hive.lose('svc-a')
  check(lost.message.includes('华南节点'), '同负载时应落到编号更小的华南，而不是华东')
  check(node('svc-a').status === 'lost', '华北标记挂失')
  check(node('svc-c').status === 'merging', '华南进入装入')
  check(master('B').suspended === 1, '还在华北的 B 索引挂起')
  check(hive.getState().nodes.every((item) => item.buffer?.length !== undefined ? item.buffer.length === 0 : true), '挂失不走缓冲区')

  const held = hive.read({ nodeId: 'svc-b', userId: '甲', keys: ['B'] })
  check(held.results[0].source === 'suspended', '其他节点不向挂失节点发请求')
  const own = hive.read({ nodeId: 'svc-c', userId: '甲', keys: ['A'] })
  check(own.results[0].source === 'local' && own.results[0].value === '华南新写', '装入期间本机自己的数据仍可读取')
  const ownD = hive.read({ nodeId: 'svc-b', userId: '甲', keys: ['D'] })
  check(ownD.results[0].source === 'local', '与丢失节点无关的本地数据照常读取')

  hive.finishMerge()
  check(node('svc-c').status === 'up', '融合后华南恢复服务')
  check(fragment('svc-c', 'A')?.value === '华南新写', '较新的本地 A 不被旧灾备盖住')
  check(fragment('svc-c', 'B')?.value === '甲二', '灾备里的 B 装进华南')
  check(master('A').node_id === 'svc-c' && master('A').suspended === 0, 'A 的地址改到华南并解开挂起')
  check(master('B').ip === '10.3.0.1', 'B 只更新到新地址')
  check(fragment('svc-a', 'B'), '挂失节点库里的旧数据还在，但不再作为恢复来源')
  const after = hive.read({ nodeId: 'svc-b', userId: '甲', keys: ['B'] })
  check(after.results[0].source === 'remote' && after.results[0].fromNodeId === 'svc-c', '之后从新地址读取')
  check(!fragment('svc-b', 'B'), '这次读取仍然不落库')
  const drA = node('dr-1').records.find((row) => row.data_key === 'A')
  check(drA?.home_node === 'svc-c' && drA.value === '华南新写', '灾备记录改到新地址，并保留较新的值')
}

function joining() {
  hive.reset()
  writeAbc()
  hive.advance('sync')
  const joined = hive.join({ region: '西南', name: '西南节点' })
  const id = joined.nodeId
  check(node(id).status === 'joining', '新节点先处于对齐')
  check(node(id).indexEntries.length === 3, '初始索引来自一台低负载节点')
  check(node(id).buffer.length === 0, '刚加入时缓冲区是空的')
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'F', value: '甲六' }] })
  hive.advance('sync')
  check(!indexAt(id, 'F'), '就绪前动态索引不直接写进新节点')
  check(node(id).buffer.some((row) => row.data_key === 'F'), '动态索引进入缓冲区')
  hive.ready(id)
  check(node(id).status === 'up', '就绪后投入服务')
  check(indexAt(id, 'F')?.node_id === 'svc-a', '缓冲区在就绪时补上')
  check(node(id).buffer.length === 0, '缓冲区交完即清空')
  check(!node('dr-1').covers.includes(id), '新节点不会自动纳入灾备')
}

function joinOwnData() {
  hive.reset()
  hive.write({
    nodeId: 'svc-a',
    userId: '林夏',
    entries: [
      { key: '姓名', value: '林夏' },
      { key: '电话', value: '13810248816' }
    ]
  })
  hive.advance('sync')
  hive.write({
    nodeId: 'svc-b',
    userId: '周衡',
    entries: [
      { key: '姓名', value: '周衡' },
      { key: '城市', value: '杭州' },
      { key: '公司', value: '临安茶业' }
    ]
  })
  hive.write({
    nodeId: 'svc-c',
    userId: '陈麦',
    entries: [
      { key: '姓名', value: '陈麦' },
      { key: '城市', value: '广州' },
      { key: '门店', value: '天河正佳店' }
    ]
  })
  hive.advance('sync')
  check(node('svc-a').fragmentCount === 2, '对齐前华北只剩两份自己的数据，成为最空的节点')
  check(!indexAt('svc-a', '姓名', '林夏'), '华北的索引副本里仍然没有自己的姓名')
  const northIndex = node('svc-a').indexEntries.length
  const joined = hive.join({ region: '西南', name: '西南节点' })
  const id = joined.nodeId
  check(indexAt(id, '姓名', '林夏')?.node_id === 'svc-a', '临时表把林夏的姓名地址带给西南')
  check(indexAt(id, '电话', '林夏')?.node_id === 'svc-a', '临时表把林夏的电话地址带给西南')
  check(indexAt(id, '城市', '周衡')?.node_id === 'svc-b', '华北已经收到的地址也一起送出')
  check(!indexAt('svc-a', '姓名', '林夏'), '临时表不写回华北')
  check(node('svc-a').indexEntries.length === northIndex, '华北自己的索引条数不变')
  hive.ready(id)
  const got = hive.read({ nodeId: id, userId: '林夏', keys: ['姓名'] })
  check(got.results[0].source === 'remote' && got.results[0].value === '林夏' && got.results[0].fromNodeId === 'svc-a', '西南就绪后按临时地址取回姓名')
  check(!fragment(id, '姓名', '林夏'), '这次取回不落在西南')
}

function unsentLost() {
  hive.reset()
  writeAbc()
  hive.lose('svc-a')
  hive.finishMerge()
  check(!fragment('svc-b', 'A') && !fragment('svc-c', 'A'), '没上报也没进灾备的数据不会被恢复')
  check(master('A')?.suspended === 1 || !master('A'), '中央还没有这些索引，或它们保持不可用')
}

test('数据放置、读取、灾备与新节点', () => {
  scatter()
  disaster()
  failover()
  joining()
  joinOwnData()
  unsentLost()
  hive.close()
  fs.rmSync(dir, { recursive: true, force: true })
  assert.equal(failures.length, 0, failures.map((item) => `- ${item}`).join('\n'))
})
