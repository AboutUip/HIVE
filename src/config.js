import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))

export const defaultConfigPath = path.join(here, '..', 'config', 'default.json')

function fail(message) {
  const error = new Error(message)
  error.expose = true
  throw error
}

function positiveInteger(value, label) {
  const number = Number(value)
  if (!Number.isInteger(number) || number < 1) fail(`${label}必须是不小于 1 的整数`)
  return number
}

function pattern(value, label) {
  const text = String(value || '')
  if (!text.includes('{seq}')) fail(`${label}里要有 {seq}`)
  return text
}

export function loadConfig(source = defaultConfigPath) {
  const raw = typeof source === 'string' ? JSON.parse(fs.readFileSync(source, 'utf8')) : source
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) fail('配置必须是一个对象')
  const syncPeriod = positiveInteger(raw.syncPeriod, '上报周期')
  const drPeriod = positiveInteger(raw.drPeriod, '灾备周期')
  const reportTolerance = positiveInteger(raw.reportTolerance, '上报容错')
  const nodeSeq = positiveInteger(raw.nodeSeq, '新节点序号')
  const newNodeId = pattern(raw.newNodeId, '新节点编号')
  const newNodeIp = pattern(raw.newNodeIp, '新节点地址')
  if (!Array.isArray(raw.nodes) || raw.nodes.length === 0) fail('配置里至少要有一台节点')
  const nodes = raw.nodes.map((node, index) => normalizeNode(node, index))
  const ids = new Set()
  const aliases = new Set()
  const ips = new Set()
  for (const node of nodes) {
    if (ids.has(node.id)) fail(`节点编号重复：${node.id}`)
    if (aliases.has(node.alias)) fail(`节点别名重复：${node.alias}`)
    if (ips.has(node.ip)) fail(`节点地址重复：${node.ip}`)
    ids.add(node.id)
    aliases.add(node.alias)
    ips.add(node.ip)
  }
  const services = new Set(nodes.filter((node) => node.role === 'service').map((node) => node.id))
  if (services.size === 0) fail('配置里至少要有一台服务节点')
  for (const node of nodes) {
    if (node.role !== 'dr') continue
    for (const cover of node.covers) {
      if (!services.has(cover)) fail(`${node.name}的负责范围里没有服务节点 ${cover}`)
    }
  }
  const actor = raw.actor
  if (!actor || typeof actor !== 'object') fail('配置里要写上起始用户和所在节点')
  const userId = String(actor.userId || '').trim()
  const nodeId = String(actor.nodeId || '').trim()
  if (!userId) fail('起始用户不能是空的')
  if (!services.has(nodeId)) fail('起始用户必须站在一台服务节点上')
  return {
    syncPeriod,
    drPeriod,
    reportTolerance,
    nodeSeq,
    newNodeId,
    newNodeIp,
    actor: { userId, nodeId },
    nodes
  }
}

function normalizeNode(node, index) {
  if (!node || typeof node !== 'object') fail(`第 ${index + 1} 台节点不是对象`)
  const id = String(node.id || '').trim()
  const name = String(node.name || '').trim()
  const role = String(node.role || '').trim()
  const region = String(node.region || '').trim()
  const ip = String(node.ip || '').trim()
  const alias = String(node.alias || id).trim()
  if (!id || !name || !region || !ip || !alias) fail(`${name || id || '有一台节点'}缺少编号、名称、区域、地址或别名`)
  if (role !== 'service' && role !== 'dr') fail(`${name}的角色只能是 service 或 dr`)
  const covers = role === 'dr' ? uniqueCovers(node.covers, name) : []
  return { id, name, role, region, ip, alias, covers }
}

function uniqueCovers(covers, name) {
  if (!Array.isArray(covers)) fail(`${name}的负责范围必须是数组`)
  return [...new Set(covers.map((id) => String(id || '').trim()).filter(Boolean))]
}
