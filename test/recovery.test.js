import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import assert from 'node:assert/strict'
import test from 'node:test'
import { createHive, Hive } from '../dist/index.js'

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'hive-stage2-'))
const hive = new Hive(dir)
const failures = []

function check(condition, message) {
  if (!condition) failures.push(message)
}

function node(id) {
  return hive.getState().nodes.find((item) => item.id === id)
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

function outboxStaysUntilAccepted() {
  hive.reset()
  hive.setReachable('svc-a', false)
  writeAbc()
  hive.advance('sync')
  check(node('svc-a').outbox.length === 3, '中央没收下之前，待上报日志还在节点上')
  check(hive.getState().logCount === 0, '没收到上报时中央没有这些日志')
  check(node('svc-a').status === 'up', '第一次没报到还在容错里，不立刻挂失')
  hive.setReachable('svc-a', true)
  hive.advance('sync')
  check(node('svc-a').outbox.length === 0, '中央确认后才删掉待上报')
  check(indexAt('svc-b', 'A')?.node_id === 'svc-a', '收下之后才派发地址')
}

function missedReportBecomesLoss() {
  hive.reset()
  hive.setReachable('svc-a', false)
  hive.advance('one')
  hive.advance('one')
  check(node('svc-a').status === 'up', '窗口刚过、还没超过容错时先记下没收到')
  hive.advance('one')
  check(node('svc-a').status === 'up', '主动要日志失败后先重试')
  const lost = hive.advance('one')
  check(node('svc-a').status === 'lost', '重试仍不可达，中央强制挂失')
  check(lost.state.events.some((item) => item.message.includes('未按预期上报')), '挂失原因是未按预期上报')
}

function windowNoticeExcusesSilence() {
  hive.reset()
  hive.setReachable('svc-a', false)
  hive.advance('one')
  hive.advance('one')
  hive.setWindows(5, 6)
  hive.advance('one')
  hive.advance('one')
  hive.advance('one')
  hive.advance('one')
  check(node('svc-a').status === 'up', '改过窗口之后，这段空档不算漏报')
}

function foldCentralLogsBeforeRestore() {
  hive.reset()
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '仅日志' }] })
  hive.advance('sync')
  check(hive.getState().logCount === 1, '灾备窗口未到时，日志先留在中央')
  check(node('dr-1').records.length === 0, '这时灾备还没有这份数据')
  hive.lose('svc-a')
  hive.finishMerge()
  check(fragment('svc-b', 'A')?.value === '仅日志', '恢复时先把中央日志并进灾备，再装到接替节点')
  check(hive.getState().logCount === 0, '并进灾备之后日志删除')
}

function retentionWritesLogFile() {
  hive.reset()
  hive.setRetention(true)
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '要留档' }] })
  hive.advance('dr')
  check(hive.getState().logCount === 0, '要求保留时，工作表里的日志仍然删除')
  const file = path.join(dir, 'retained.log')
  check(fs.existsSync(file) && fs.readFileSync(file, 'utf8').includes('要留档'), '保留的内容写在 log 文件里')
}

function utcOnTheWritingNode() {
  hive.reset()
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '先写' }] })
  const first = fragment('svc-a', 'A').updated_at
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '后写' }] })
  const second = fragment('svc-a', 'A')
  check(/\.\d{3}Z-\d{6}$/.test(first) && /\.\d{3}Z-\d{6}$/.test(second.updated_at), '时间是这台节点自己的 HLC')
  check(second.updated_at > first, '后写的时间更晚，不跟中央对时')
  check(second.value === '后写', '同一节点上后写的值留下')
  check(hive.getState().clock === undefined, '中央不再发统一时钟')
}

function sameInstantStillOrdersOnOneNode() {
  const clock = new Date('2026-01-01T00:00:00.000Z')
  const clockDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hive-stage2-clock-'))
  const local = new Hive(clockDir, { now: () => clock })
  local.reset()
  local.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '第一下' }] })
  local.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '第二下' }] })
  const rows = local.getState().nodes.find((item) => item.id === 'svc-a').fragments
  const row = rows.find((item) => item.data_key === 'A')
  check(row?.value === '第二下', '同一毫秒里，这台节点自己把后一次写得更晚')
  local.close()
  fs.rmSync(clockDir, { recursive: true, force: true })
}

function mergeTargetRollsBack() {
  hive.reset()
  writeAbc()
  hive.advance('sync')
  hive.advance('dr')
  hive.lose('svc-a')
  check(node('svc-b').status === 'merging', '空载更小的华东先接替')
  hive.setReachable('svc-b', false)
  const rolled = hive.finishMerge()
  check(rolled.message.includes('回退'), '接替节点失败时立刻回退')
  check(node('svc-b').status === 'lost', '接替节点也标记挂失')
  check(node('svc-a').status === 'lost', '原来的挂失仍然算数')
  check(hive.getState().mergeJob === null, '这次装入已取消')
  check(!fragment('svc-b', 'A'), '回退后华东没有装入华北的数据')
  hive.recover('svc-a')
  check(node('svc-c').status === 'merging', '换一台还活着的节点继续装')
  hive.finishMerge()
  check(fragment('svc-c', 'A')?.value === '甲一', '灾备里的数据装进华南')
}

function lostNodeReturnsAsNew() {
  const again = hive.rejoin('svc-a', { region: '华北', name: '华北新节点' })
  check(node('svc-a').status === 'lost', '旧编号继续保持挂失')
  check(node('svc-a').fragments.length === 0, '回来时清空旧库')
  check(node(again.nodeId).status === 'joining', '作为新节点重新对齐')
  check(again.nodeId !== 'svc-a', '新节点用新的编号')
}

function aliasRepairsIpOnce() {
  hive.reset()
  hive.aliasLookups = 0
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '甲一' }] })
  hive.advance('sync')
  check(indexAt('svc-b', 'A')?.ip === '10.1.0.1', '索引里先用原来的 IP')
  hive.changeIp('svc-a', '10.8.8.8')
  check(indexAt('svc-b', 'A')?.ip === '10.1.0.1', 'IP 变更不推给其他节点')
  const first = hive.read({ nodeId: 'svc-b', userId: '甲', keys: ['A'] })
  check(first.results[0].via === 'alias' && first.results[0].fromIp === '10.8.8.8', 'IP 不通时按别名向中央要一次新地址')
  check(first.results[0].storedLocally === false, '核对地址之后仍然直接读取，不落库')
  check(hive.aliasLookups === 1, '这一次读取只问了中央一次')
  const second = hive.read({ nodeId: 'svc-b', userId: '甲', keys: ['A'] })
  check(second.results[0].via === 'ip' && hive.aliasLookups === 1, '地址更新后，下一次直接用 IP')
}

function listKeepsOwnDataTemporary() {
  hive.reset()
  writeAbc()
  hive.advance('sync')
  hive.write({
    nodeId: 'svc-b',
    userId: '甲',
    entries: [
      { key: 'C', value: '甲三改' },
      { key: 'D', value: '甲四' }
    ]
  })
  const listed = hive.list({ nodeId: 'svc-b', userId: '甲' })
  const keys = listed.rows.map((row) => row.data_key).sort()
  check(keys.join(',') === 'A,B,C,D', '列名单时，索引和自身数据临时合在一起')
  const localC = listed.rows.find((row) => row.data_key === 'C')
  check(localC?.node_id === 'svc-b', '同一键以这台节点自己的数据为准')
  check(indexAt('svc-b', 'C')?.node_id === 'svc-a', '持久索引不写回这次临时表')
}

function missedIndexArrivesOnNextReport() {
  hive.reset()
  hive.setIndexLink('svc-c', false)
  writeAbc()
  hive.advance('sync')
  check(node('svc-b').indexEntries.length === 3, '链路正常的节点当次就能收到地址')
  check(node('svc-c').indexEntries.length === 0, '没收的地址不在读取时向中央要')
  hive.setIndexLink('svc-c', true)
  hive.advance('sync')
  check(node('svc-c').indexEntries.length === 3, '下次上报时，中央把漏掉的地址补上')
}

function deleteRemovesKey() {
  hive.reset()
  hive.write({ nodeId: 'svc-a', userId: '甲', entries: [{ key: 'A', value: '甲一' }] })
  hive.advance('sync')
  hive.advance('dr')
  hive.remove({ nodeId: 'svc-a', userId: '甲', keys: ['A'] })
  check(!fragment('svc-a', 'A'), '删除后本地数据立刻没有')
  const during = hive.read({ nodeId: 'svc-b', userId: '甲', keys: ['A'] })
  check(during.results[0].source === 'rejected', '窗口里读到的是没有，不另作一种错误')
  hive.advance('sync')
  check(!master('A'), '同步后中央地址也去掉')
  const after = hive.read({ nodeId: 'svc-b', userId: '甲', keys: ['A'] })
  check(after.results[0].source === 'rejected', '其他节点此后也视为没有')
  hive.advance('dr')
  check(!node('dr-1').records.some((row) => row.data_key === 'A'), '灾备里的这份稳定数据也被删掉')
}

function databaseLossIsSelfRequested() {
  hive.reset()
  hive.breakDatabase('svc-b')
  hive.advance('one')
  check(node('svc-b').status === 'lost', '库不可达由这台节点自己申请挂失')
  check(hive.getState().events.some((item) => item.message.includes('数据库不可达')), '申请原因写明数据库不可达')
}

function layoutComesFromConfig() {
  const customDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hive-stage2-config-'))
  const custom = createHive({
    dataDir: customDir,
    config: {
      syncPeriod: 3,
      drPeriod: 9,
      reportTolerance: 1,
      nodeSeq: 8,
      newNodeId: 'n-{seq}',
      newNodeIp: '10.50.{seq}.1',
      actor: { userId: '乙', nodeId: 'n1' },
      nodes: [
        { id: 'n1', name: '甲地', role: 'service', region: '甲', ip: '10.50.1.1', alias: 'jia', covers: [] },
        { id: 'd1', name: '备', role: 'dr', region: '备', ip: '10.50.9.1', alias: 'bei', covers: ['n1'] }
      ]
    }
  })
  custom.reset()
  const state = custom.getState()
  check(state.nodes.length === 2, '有哪些节点由配置决定')
  check(state.syncPeriod === 3 && state.drPeriod === 9, '窗口由配置决定')
  check(state.actor.userId === '乙' && state.actor.nodeId === 'n1', '起始用户由配置决定')
  const joined = custom.join({ region: '乙', name: '乙地' })
  const created = custom.getState().nodes.find((item) => item.id === joined.nodeId)
  check(joined.nodeId === 'n-8' && created?.ip === '10.50.8.1', '新节点的编号和地址也来自配置')
  custom.close()
  fs.rmSync(customDir, { recursive: true, force: true })
}

function runtimeLossIsSelfRequested() {
  hive.reset()
  const lost = hive.reportRuntime('svc-c', '运行报错')
  check(node('svc-c').status === 'lost', '运行报错走节点自己的挂失')
  check(lost.message.includes('运行报错'), '原因是运行报错')
}

test('上报、挂失、恢复、别名与删除', () => {
  outboxStaysUntilAccepted()
  missedReportBecomesLoss()
  windowNoticeExcusesSilence()
  foldCentralLogsBeforeRestore()
  retentionWritesLogFile()
  utcOnTheWritingNode()
  sameInstantStillOrdersOnOneNode()
  mergeTargetRollsBack()
  lostNodeReturnsAsNew()
  aliasRepairsIpOnce()
  listKeepsOwnDataTemporary()
  missedIndexArrivesOnNextReport()
  deleteRemovesKey()
  databaseLossIsSelfRequested()
  layoutComesFromConfig()
  runtimeLossIsSelfRequested()
  hive.close()
  fs.rmSync(dir, { recursive: true, force: true })
  assert.equal(failures.length, 0, failures.map((item) => `- ${item}`).join('\n'))
})
