import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { Hive } from '../dist/index.js'

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'hive-long-'))
const hive = new Hive(dir)
const failures = []

function say() {}

function check(condition, message) {
  if (!condition) failures.push(message)
}

function node(id) {
  return hive.getState().nodes.find((item) => item.id === id)
}

function valueAt(nodeId, user, key) {
  return node(nodeId).fragments.find((row) => row.user_id === user && row.data_key === key)?.value
}

function master(user, key) {
  return hive.getState().indexMaster.find((row) => row.user_id === user && row.data_key === key)
}

function drRecord(user, key) {
  return node('dr-1').records.find((row) => row.user_id === user && row.data_key === key)
}

function write(nodeId, user, entries) {
  hive.write({
    nodeId,
    userId: user,
    entries: entries.map(([key, value]) => ({ key, value }))
  })
}

function read(nodeId, user, keys) {
  return hive.read({ nodeId, userId: user, keys })
}

hive.reset()
write('svc-a', '林夏', [
  ['姓名', '林夏'],
  ['电话', '13810248816'],
  ['城市', '北京'],
  ['会员', '普通'],
  ['备注', '朝阳区望京，工作日晚间送达']
])
write('svc-b', '周衡', [
  ['姓名', '周衡'],
  ['电话', '18602117740'],
  ['城市', '杭州'],
  ['公司', '临安茶业'],
  ['备注', '每周二补货，送到西湖区灵隐路 18 号']
])
write('svc-c', '陈麦', [
  ['姓名', '陈麦'],
  ['电话', '15902006631'],
  ['城市', '广州'],
  ['门店', '天河正佳店'],
  ['订单-0904', '2026-10-06 手冲壶 1 个 268 元，到店自提']
])
say(`第 ${hive.tick()} 拍，三份档案还在各自节点的待上报日志里。中央日志 ${hive.getState().logCount} 条。`)
check(hive.getState().logCount === 0, '同步前中央还没有日志')
check(valueAt('svc-a', '林夏', '城市') === '北京', '林夏的城市先写在华北')

const early = read('svc-b', '林夏', ['姓名', '电话', '城市'])
check(early.results.every((item) => item.source === 'rejected'), '同步前华东读不到林夏')
check(!valueAt('svc-b', '林夏', '姓名'), '拒绝之后华东没有留下林夏的姓名')

say('')
say('### 第一次同步')
hive.advance('sync')
say(`第 ${hive.tick()} 拍完成同步。`)
const fetched = read('svc-b', '林夏', ['姓名', '电话', '城市', '会员', '备注'])
check(fetched.results.every((item) => item.source === 'remote' && item.storedLocally === false), '华东按地址取回林夏的五份档案，不写入本机')
check(fetched.results.find((item) => item.key === '城市')?.value === '北京', '此时取回的城市是北京')
check(!valueAt('svc-b', '林夏', '城市'), '取回之后华东库里仍然没有林夏的城市')
check(node('svc-a').indexEntries.filter((row) => row.user_id === '林夏').length === 0, '华北不接收自己刚交的这批地址')
const listed = hive.list({ nodeId: 'svc-a', userId: '林夏' })
check(
  listed.rows.some((row) => row.data_key === '姓名' && row.node_id === 'svc-a'),
  '华北列完整索引时，临时带上自己的姓名'
)
check(
  !node('svc-a').indexEntries.some((row) => row.user_id === '林夏' && row.data_key === '姓名'),
  '临时名单不写回华北的持久索引'
)

say('')
say('### 林夏到了华东，改地址')
write('svc-b', '林夏', [
  ['城市', '上海'],
  ['地址', '浦东新区世纪大道 100 号'],
  ['备注', '改为白天送达，不要放门口']
])
const stale = read('svc-c', '林夏', ['城市'])
check(stale.results[0].source === 'remote' && stale.results[0].value === '北京', '窗口内华南仍按旧地址取到北京')
check(valueAt('svc-a', '林夏', '城市') === '北京', '同步前华北仍保留旧城市')
check(valueAt('svc-b', '林夏', '城市') === '上海', '新城市只写在华东')

say('')
say('### 第二次同步，按时间拆开')
hive.advance('sync')
say(`第 ${hive.tick()} 拍完成同步。`)
check(valueAt('svc-a', '林夏', '城市') === undefined, '华北去掉被盖住的旧城市')
check(valueAt('svc-a', '林夏', '备注') === undefined, '华北去掉被盖住的旧备注')
check(valueAt('svc-a', '林夏', '姓名') === '林夏' && valueAt('svc-a', '林夏', '电话') === '13810248816', '没被改过的姓名和电话仍在华北')
check(valueAt('svc-b', '林夏', '城市') === '上海' && valueAt('svc-b', '林夏', '地址') === '浦东新区世纪大道 100 号', '城市和地址留在华东')
check(master('林夏', '城市')?.node_id === 'svc-b', '中央索引把城市指到华东')
check(master('林夏', '姓名')?.node_id === 'svc-a', '姓名的地址仍是华北')

say('')
say('### 其他人补档案、改订单')
write('svc-a', '赵宁', [
  ['姓名', '赵宁'],
  ['电话', '13821105690'],
  ['城市', '天津'],
  ['地址', '和平区滨江道 128 号'],
  ['备注', '公司月结，发票抬头用宁远贸易']
])
write('svc-b', '孙乔', [
  ['姓名', '孙乔'],
  ['电话', '13771882045'],
  ['城市', '苏州'],
  ['地址', '工业园区星海街 58 号'],
  ['会员', '银卡'],
  ['订单-2208', '待发货 碧螺春 2 盒 196 元']
])
write('svc-c', '何岚', [
  ['姓名', '何岚'],
  ['电话', '13631669028'],
  ['城市', '深圳'],
  ['地址', '南山区科技园科苑路 15 号'],
  ['备注', '只收顺丰，工作日 10 点到 18 点']
])
write('svc-b', '孙乔', [['会员', '金卡'], ['订单-2208', '已签收 碧螺春 2 盒 196 元']])
write('svc-c', '林夏', [
  ['会员', '金卡'],
  ['订单-2201', '2026-10-07 上海仓发往广州 滤杯套装 1 套 128 元']
])
write('svc-b', '周衡', [
  ['订单-1842', '2026-10-07 龙井 4 罐 312 元，西湖区自提'],
  ['订单-1843', '2026-10-08 待配送 白茶样包 6 袋']
])
write('svc-c', '陈麦', [['订单-0904', '2026-10-06 手冲壶 1 个 268 元，已取走']])
write('svc-a', '赵宁', [['订单-0731', '2026-10-05 天津仓 马克杯 6 个 210 元']])

const missing = read('svc-c', '林夏', ['发票抬头'])
check(missing.results[0].source === 'rejected', '林夏从未写过发票抬头，华南直接拒绝')

const beforeMemberSync = read('svc-b', '林夏', ['会员'])
check(beforeMemberSync.results[0].value === '普通', '会员改写还没同步时，华东仍从华北取到普通')

say('')
say('### 再同步，并做第一次灾备整理')
hive.advance('dr')
say(`第 ${hive.tick()} 拍。中央还留着的日志 ${hive.getState().logCount} 条，灾备稳定记录 ${node('dr-1').records.length} 条。`)
check(hive.getState().logCount === 0, '整理进灾备后，用过的日志从中央删除')
check(node('dr-1').records.length > 0, '重复改写收成稳定数据后留在灾备')
check(valueAt('svc-a', '林夏', '会员') === undefined, '华北去掉旧会员')
check(valueAt('svc-c', '林夏', '会员') === '金卡', '金卡会员留在华南')
check(drRecord('林夏', '城市')?.value === '上海', '灾备里的城市是上海，不是北京')
check(drRecord('孙乔', '订单-2208')?.value === '已签收 碧螺春 2 盒 196 元', '同一张订单的两次日志收成已签收')
check(drRecord('孙乔', '会员')?.value === '金卡', '灾备里的会员是金卡')
check(!node('dr-1').records.some((row) => row.value === '北京' && row.data_key === '城市'), '灾备没有留下旧城市')

say('')
say('### 西南节点加入')
const beforeJoin = hive.getState().nodes.filter((item) => item.role === 'service' && item.status === 'up')
const lightest = [...beforeJoin].sort((a, b) => a.fragmentCount - b.fragmentCount || (a.id < b.id ? -1 : 1))[0]
const joined = hive.join({ region: '西南', name: '西南节点' })
const southwest = joined.nodeId
say(joined.message)
const copied = node(southwest).indexEntries.length
const masterCount = hive.getState().indexMaster.length
const missingKeys = hive.getState().indexMaster.filter(
  (row) => !node(southwest).indexEntries.some((item) => item.user_id === row.user_id && item.data_key === row.data_key)
)
say(`初始索引 ${copied} 条，中央索引 ${masterCount} 条。`)
say(`负载最低的是${lightest.name}，碎片 ${lightest.fragmentCount} 份。临时表补上了它自己的这些数据。`)
check(node(southwest).status === 'joining', '西南先处于对齐，还没服务')
check(missingKeys.length === 0, '临时带上自身数据后，西南的初始索引和中央一致')
check(
  node(southwest).indexEntries.some((row) => row.user_id === '林夏' && row.data_key === '姓名' && row.node_id === 'svc-a'),
  '林夏的姓名在对齐时就指向华北'
)
check(
  !node('svc-a').indexEntries.some((row) => row.user_id === '林夏' && row.data_key === '姓名'),
  '临时表不写回华北自己的索引'
)
check(!node('dr-1').covers.includes(southwest), '新节点不会自动纳入灾备')

write('svc-a', '林夏', [['紧急联系人', '母亲 林婉 13701028820']])
hive.advance('sync')
check(!node(southwest).indexEntries.some((row) => row.data_key === '紧急联系人'), '就绪前，紧急联系人不进西南的正式索引')
check(node(southwest).buffer.some((row) => row.data_key === '紧急联系人'), '这条新地址先堆在缓冲区')
hive.ready(southwest)
check(node(southwest).status === 'up', '西南就绪后开始服务')
check(node(southwest).indexEntries.some((row) => row.user_id === '林夏' && row.data_key === '紧急联系人'), '缓冲区在就绪时补上')
check(node(southwest).buffer.length === 0, '缓冲区交完即清空')

say('')
say('### 把西南纳入灾备，再整理一次')
hive.setCovers('dr-1', ['svc-a', 'svc-b', 'svc-c', southwest])
hive.advance('dr')
check(drRecord('林夏', '紧急联系人')?.value === '母亲 林婉 13701028820', '紧急联系人收成稳定数据')
check(drRecord('林夏', '姓名')?.home_node === 'svc-a', '姓名的灾备记录仍住在华北')

say('')
say('### 挂失华北')
write(southwest, '赵宁', [['电话', '13911062845']])
say('西南先写下赵宁的新电话，这批日志还没上报。')
const lost = hive.lose('svc-a')
say(lost.message)
check(lost.message.includes('西南节点'), '华北的数据装进当前更空的西南，不是地理上更近的华东')
check(node('svc-a').status === 'lost', '华北标记挂失')
check(node(southwest).status === 'merging', '西南一边装入，一边还能服务')

const held = read('svc-b', '赵宁', ['姓名'])
check(held.results[0].source === 'suspended', '赵宁的姓名还指向华北，华东把请求停住')
const localCity = read('svc-b', '林夏', ['城市'])
check(localCity.results[0].source === 'local' && localCity.results[0].value === '上海', '林夏的城市本来就在华东，挂失期间照常读到')
const ownPhone = read(southwest, '赵宁', ['电话'])
check(ownPhone.results[0].source === 'local' && ownPhone.results[0].value === '13911062845', '西南自己刚写的新电话仍可读取')

say('')
say('### 完成融合')
const merged = hive.finishMerge()
say(merged.message)
check(node(southwest).status === 'up', '融合后西南恢复为服务中')
check(valueAt(southwest, '赵宁', '电话') === '13911062845', '较新的电话没被灾备里的旧号码盖住')
check(valueAt(southwest, '赵宁', '姓名') === '赵宁', '灾备里的姓名装进西南')
check(valueAt(southwest, '林夏', '姓名') === '林夏' && valueAt(southwest, '林夏', '电话') === '13810248816', '林夏留在华北的姓名和电话装进西南')
check(valueAt('svc-a', '赵宁', '电话') === '13821105690', '挂失节点的库里仍留着旧电话，恢复不读它')
check(master('赵宁', '姓名')?.ip === '10.4.0.1' && master('赵宁', '姓名')?.suspended === 0, '姓名的索引改到西南的新地址')
check(master('林夏', '城市')?.node_id === 'svc-b', '本来住在华东的城市没有被搬走')
check(drRecord('赵宁', '电话')?.value === '13911062845' && drRecord('赵宁', '电话')?.home_node === southwest, '灾备改记新电话，并改到西南')

const after = read('svc-b', '赵宁', ['姓名', '电话'])
check(after.results.every((item) => item.source === 'remote' && item.fromNodeId === southwest), '华东之后从西南取回赵宁的姓名和电话')
check(!valueAt('svc-b', '赵宁', '姓名'), '这次取回仍然不落库')

say('')
say('## 结束时数据住在哪')
say('')
say('| 用户 | 键 | 内容 | 实际所在 | 索引地址 |')
say('| --- | --- | --- | --- | --- |')
const services = hive.getState().nodes.filter((item) => item.role === 'service')
const rows = []
for (const service of services) {
  for (const fragment of service.fragments) {
    const address = master(fragment.user_id, fragment.data_key)
    rows.push({
      user: fragment.user_id,
      key: fragment.data_key,
      value: fragment.value,
      home: `${service.name}${service.status === 'lost' ? '（挂失，不作为恢复来源）' : ''}`,
      ip: address ? `${address.ip}${address.suspended ? ' 挂起' : ''}` : '无'
    })
  }
}
rows.sort((a, b) => a.user.localeCompare(b.user, 'zh') || a.key.localeCompare(b.key, 'zh'))
for (const row of rows) {
  say(`| ${row.user} | ${row.key} | ${row.value} | ${row.home} | ${row.ip} |`)
}

say('')
say('## 数量')
say('')
const state = hive.getState()
const dr = node('dr-1')
say(`- 中央完整日志 ${state.logCount} 条，其中 ${state.unorganizedLogCount} 条还没整理。`)
say(`- 灾备稳定记录 ${dr.records.length} 条。负责 ${dr.covers.map((id) => node(id).name).join('、')}。`)
for (const service of services) {
  say(`- ${service.name} ${service.status}，碎片 ${service.fragmentCount} 份，地址副本 ${service.indexEntries.length} 条，待上报 ${service.outbox.length} 条。`)
}
say('')
say(`断言 ${failures.length === 0 ? '全部通过' : `失败 ${failures.length} 条`}。`)

hive.close()
fs.rmSync(dir, { recursive: true, force: true })

test('多用户数据分布', () => {
  assert.equal(failures.length, 0, failures.map((item) => `- ${item}`).join('\n'))
})
