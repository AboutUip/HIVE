# API

本文描述安装之后可以调用的接口。产品行为见 [方案.md](方案.md)。文档目录见 [README.md](README.md)。

发布入口是包名 `hive`，对应编译结果 `dist/index.js`。源码在 `src/`，不作为对外入口。

```javascript
import { createHive, loadConfig, defaultConfigPath } from 'hive'

const hive = createHive({ dataDir: './data' })
hive.reset()
```

调用方指定节点编号，用来代替 CDN 的就近判断。数据时间是各节点写下那一刻的 UTC。上报和灾备按拍推进。

`Hive` 上还有未在本文列出的方法。它们不是公开接口。

需要 Node.js 22.5 或更高版本。

## 创建

### createHive(options)

| 字段 | 必须 | 说明 |
| --- | --- | --- |
| `dataDir` | 是 | 数据目录。中央库存为 `central.db`，每台节点一份 `{id}.db`。 |
| `config` | 否 | 配置对象。传入之后不再读文件。形状与 `config/default.json` 相同。 |
| `configPath` | 否 | 配置文件路径。默认是仓库里的 `config/default.json`。与 `config` 同时传入时，以 `config` 为准。 |
| `now` | 否 | 节点取时间的函数。默认 `() => new Date()`。可以返回 `Date`，或能被 `Date.parse` 解析的字符串。 |
| `retentionFile` | 否 | 要求保留日志时写入的文件。默认是 `dataDir/retained.log`。 |

未提供 `dataDir` 时抛出 `要指定数据目录`。

`new Hive(dataDir, options)` 得到同一个对象。`createHive` 是对外入口。构造之后还没有库，先调用 `reset()`。

`now` 只决定数据上的 UTC 时间，不推进拍，也不和中央协商。同一节点上，若这次取出的时间不晚于它上一次写下的时间，SDK 把这次记成晚 1 毫秒。

### reset()

清空目录中的库表，按当前配置重建起始局面，返回普通结果。每次 `reset()` 都会重新读取配置。改了 `config/default.json` 之后，下一次 `reset()` 生效。

默认配置在 `config/default.json`。起始节点是：

| id | 名称 | 角色 | 区域 | IP | 别名 | 状态 |
| --- | --- | --- | --- | --- | --- | --- |
| `svc-a` | 华北节点 | 服务 | 华北 | `10.1.0.1` | `svc-a` | `up` |
| `svc-b` | 华东节点 | 服务 | 华东 | `10.2.0.1` | `svc-b` | `up` |
| `svc-c` | 华南节点 | 服务 | 华南 | `10.3.0.1` | `svc-c` | `up` |
| `dr-1` | 灾备甲 | 灾备 | 集中 | `10.9.0.1` | `dr-1` | `up` |

`dr-1` 的负责范围是 `svc-a`、`svc-b`、`svc-c`。

这些名字、地址、负责范围、上报周期、灾备周期、容错、起始用户，以及新节点的编号和地址样式，都来自配置，不写在引擎里。默认是上报 2 拍、灾备 6 拍、容错 1 拍，起始用户「甲」在 `svc-a`。新节点编号用配置里的 `newNodeId`，地址用 `newNodeIp`，其中 `{seq}` 换成 `nodeSeq`，用过之后序号加 1。默认从 `svc-4`、`10.4.0.1` 开始。日志不留档。

`loadConfig(source)` 读取并检查配置。`source` 可以是文件路径或对象。缺字段、编号重复、负责范围指向不存在的服务节点时抛出。`defaultConfigPath` 是默认文件的绝对路径。

Windows 上关闭 SQLite 后，文件可能仍被锁住。`reset()` 会在重新打开后清空表，不依赖把文件删掉。

### close()

关闭已打开的库。没有返回值。进程结束前调用，便于删掉临时目录。

## 返回与错误

除 `close()`、`tick()` 和下面注明的只读方法外，成功时返回：

```javascript
{
  message: '给人看的一句说明',
  state: { /* 与 getState() 相同 */ }
}
```

部分方法还会带上 `results`、`rows` 或 `nodeId`。

失败时抛出 `Error`。`error.expose === true`，说明文字在 `error.message`。一次 `write` 或 `remove` 里若后面的键出错，前面已经处理的键会留下。

## 拍

`tick()` 返回当前拍，从 0 开始。

`advance(mode)` 向前走。

| mode | 行为 |
| --- | --- |
| `'one'` | 走 1 拍。 |
| `'sync'` | 一直走到有一台服务节点成功上报为止。最多再走「当前全局上报周期 + 1」拍。 |
| `'dr'` | 一直走到拍数能被灾备周期整除的那一拍，并在那一拍整理灾备。最多再走「灾备周期 + 1」拍。 |

走不到目标时抛出 `这一轮没有走到目标窗口`。已经走过的拍仍然生效，其中可能已经发生索取、重试或挂失。

每一拍里，正在服务或正在装入的节点若到达自己的上报周期，就向中央上报。空队列也算上报成功，并记下这一拍。数据库被标记为不可达的节点，会在这一拍自己申请挂失。

拍数能被灾备周期整除时，中央把还留着的日志整理进灾备，然后删除这些日志。

`advance('sync')` 在任意一台节点上报成功时就返回。它不表示每一台都上报了。一台节点不可达时，其他节点仍可能让这次 `advance('sync')` 成功。

容错来自配置里的 `reportTolerance`。默认文件是 1 拍。没有单独的运行时修改方法；改配置后，下一次 `reset()` 生效。某台节点的期限是「上次成功上报的拍 + 它的上报周期 + 容错」。期限之前没报到，只记一次没收到。到达期限后每拍再要一次；第 2 次仍要不到，中央以「未按预期上报」强制挂失。

`state.nextSyncIn` 和 `state.nextDrIn` 按全局周期估算下一拍，不按某一台单独改过的周期计算。

## 写入、读取、删除、名单

### write({ nodeId, userId, entries })

在 `nodeId` 这台服务节点上，为 `userId` 写下 `entries`。每一项是 `{ key, value }`。键和值都会去掉首尾空白。值为空时抛出。

数据立刻进这台节点的库，并进入待上报。中央确认前不会从队列删除。时间由这台节点的 `now` 产生。

只能写给状态为 `up` 或 `merging` 的服务节点。挂失、对齐中、灾备节点会抛出。

### read({ nodeId, userId, keys })

从 `nodeId` 读取 `userId` 的这些键。`keys` 是字符串数组。

返回的 `results` 与 `keys` 一一对应。某一键的结果是拒绝或挂起时，整个调用仍然成功，不抛出。节点本身不能读时才抛出，例如节点已挂失或还在对齐。

每项的 `source`：

| source | 含义 |
| --- | --- |
| `local` | 本机库里有。`storedLocally` 为 `true`。 |
| `remote` | 按地址取回，没有写入本机。`storedLocally` 为 `false`。 |
| `rejected` | 视为没有。包括索引里没有、对方已经没有、别名核对后仍不可达。 |
| `suspended` | 地址已挂起，或目标已挂失。请求没有发出。 |

取到值时还有 `value`、`fromNodeId`、`fromIp`、`via`。`via` 为 `local`、`ip` 或 `alias`。`alias` 表示原来的 IP 不通，已经按别名向中央要过一次新 IP，并更新了这台节点上同一别名的地址，然后直接读取。

`hive.aliasLookups` 是进程内计数，每次因 IP 不通而向中央核对别名加 1。IP 可用时不加。

### remove({ nodeId, userId, keys })

在持有这些数据的节点上删除。`keys` 是字符串数组。本机没有该键时抛出，例如 `华北节点上没有 甲 的 A，不能在这里删除`。

本机数据和本机对应地址立即去掉。删除日志留在待上报里，上报之后其他节点的地址才会去掉，下一次灾备整理才会去掉灾备记录。

### list({ nodeId, userId })

列出这台服务节点上的完整地址。`userId` 可省略；省略时列出全部用户。

返回 `rows`。每一行含 `user_id`、`data_key`、`node_id`、`ip`、`alias`、`updated_at`、`suspended`。行来自一张临时表：持久索引加上本机库里的数据。同一键以本机库为准。调用结束，持久索引的条数不变。

挂失节点不能列。对齐中的节点可以列它已经拿到的临时索引。

### setActor(nodeId, userId)

记下「当前用户站在哪台服务节点」。`write` 和 `read` 成功时也会改写这组记录。只影响 `state.actor`，不改变数据所在。

## 窗口与留档

### setWindows(syncPeriod, drPeriod)

把全局上报周期和灾备周期改成这两个正整数（拍）。同时把每台服务节点的上报周期改成同一个数，并把「上次上报」记成当前拍，重试次数清零。这就是改窗口通知：通知之后的空档不算漏报。

小于 1 或不是整数时抛出。

### setReportPeriod(nodeId, period)

只改这一台服务节点的上报周期，同样发送改窗口通知。灾备周期不变。`state.nextSyncIn` 仍按全局周期计算。

### setRetention(enabled)

`true` 时，日志整理进灾备之后仍从中央删除，并在删除前把每一条追加进留档文件。每行一个 JSON 对象，字段为 `userId`、`key`、`value`、`op`、`updatedAt`、`source`。

`false` 时只删除，不写文件。`reset()` 之后默认为 `false`。

## 节点与灾备范围

### join({ region, name })

增加一台服务节点。`region` 必填。`name` 省略时用「区域名 + 节点」。

编号和地址按配置里的 `newNodeId`、`newNodeIp` 生成，别名等于编号。默认配置下，第一台新节点是 `svc-4`，地址 `10.4.0.1`。状态为 `joining`。索引从当前负载最低的 `up` 节点拉来，并临时加上那台节点自己的数据。返回值里的 `nodeId` 是新编号。

没有正在服务的节点时抛出 `现在没有正在服务的节点可以提供索引`。新节点不在任何灾备的负责范围里。

负载是本机数据份数。份数相同，取 `id` 较小的一台。无法访问或数据库已坏的节点不参与选择。

### ready(nodeId)

把对齐中的节点改为 `up`，把缓冲区里的地址写入它的索引，并把它的上次上报记为当前拍。不是 `joining` 时抛出。

### setCovers(drId, nodeIds)

设置这台灾备的负责范围，替换原列表。不是服务节点的 id 会被丢掉。传入空数组表示谁也不负责。

这里只改声明，不立刻重放旧数据，也不自动走一拍灾备。要让新范围内的新日志进入灾备，需要之后再 `advance('dr')`。已经住在范围外的记录，要等这个键下一次被整理时才会离开灾备。

### changeIp(nodeId, ip)

把这台节点的当前 IP 改成新值。中央的节点记录、全局地址、缓冲区和待补送地址一起改。已经在各服务节点上的地址副本不改，仍用旧 IP。下次读取发现旧 IP 不通时，按别名更新。

空字符串抛出 `要写上新的地址`。别名不变。

## 挂失与恢复

同一时间只有一项装入。`state.mergeJob` 为 `null`，或为 `{ lostId, targetId, startedTick }`。

接替节点的状态在装入期间是 `merging`。它可以继续读写自己的数据。指向挂失节点的读取结果是 `suspended`。

### lose(nodeId, options)

中央强制挂失。`options.cause` 默认是 `中央强制挂失`。

选择一台 `up` 的低负载节点作为接替，把它标为 `merging`，挂起指向挂失节点的地址。待上报队列留在挂失节点上，不会补交。

已经有装入在进行、而这次挂失的又不是那台接替节点时，抛出 `已经有节点在融合，先完成这次再挂失下一台`。挂失的正是接替节点时，不抛出，改为回退并把它也标成挂失。

没有可接替节点、目标不是服务节点、已经挂失、或还在对齐时，抛出。

### applyLoss(nodeId, cause)

节点自己申请挂失。`cause` 默认 `节点申请挂失`。已经挂失时不抛出，返回「已经挂失」。其余与 `lose` 相同。

### reportRuntime(nodeId, reason)

节点报告运行错误并申请挂失。`reason` 默认 `运行报错`，并作为挂失原因。

### finishMerge()

完成当前装入。没有装入时抛出 `现在没有正在融合的节点`。

若中央还留着日志，先整理进灾备，再按灾备装入，然后删除这些日志。装入时，接替节点上严格更新的本地数据保留，其余写入灾备的值。地址改到接替节点并取消挂起。接替节点恢复为 `up`。

接替节点此时无法访问，或数据库已坏：回退已写入的部分，把它标成挂失，清空 `mergeJob`。原来的挂失保留。这次调用返回回退说明，不抛出。

### recover(nodeId)

对一台已经挂失、且当前没有装入任务的服务节点，重新选择低负载节点开始装入。数据仍来自灾备。用于接替失败之后继续转移所有权。

还没挂失，或已经有装入时，抛出。

### rejoin(nodeId, options)

挂失节点回来。清空这台旧编号上的数据、地址和待上报，然后按新节点执行与 `join` 相同的加入。返回值里的 `nodeId` 是新编号。旧编号保持 `lost`，库是空的。

`options.region` 省略时用旧区域。`options.name` 省略时用「旧区域 + 新节点」。

只有挂失的服务节点可以这样回来。当前装入仍涉及这台节点时抛出 `融合尚未结束，先处理完这次装入`。

## 故障注入

方案里的不可达、库损坏和漏收地址，在单进程里没有真实网络。下面三个方法只用于测试和演示。它们不代替 `lose`：不可达要等上报期限，库损坏要等节点自己在下一次接触时申请挂失。

### setReachable(nodeId, reachable)

`false` 表示这台节点的服务器无法访问。上报要不到它，读取时 IP 也不通。它不会立刻变成挂失，要等期限、索取和重试。`true` 恢复可访问。

### breakDatabase(nodeId)

标记这台服务节点的库不可达。调用本身不挂失。下一次 `advance`，或对它执行 `write`、`read`、`remove`、`list` 时，它以「数据库不可达」申请挂失。对它的读写会在申请之后抛出。

`getState()` 对这种节点不再读取库文件，对应节点带 `dbBroken: true`，本地数据列表为空。

### setIndexLink(nodeId, up)

`false` 时，派给这台服务节点的地址先留在中央，不写入它的索引。`true` 之后，要等它下一次成功上报，中央才把这些地址补送过去。读取不会为了补地址去问中央。

## getState()

```javascript
{
  tick,
  syncPeriod,
  drPeriod,
  tolerance,
  retention,
  nextSyncIn,
  nextDrIn,
  actor: { userId, nodeId },
  nodes,
  indexMaster,
  logs,
  logCount,
  unorganizedLogCount,
  mergeJob,
  events
}
```

`logs` 最多 12 条，是中央还没整理完的日志。整理之后它们被删除，所以 `logCount` 与 `unorganizedLogCount` 相同：还在中央的每一条都还没用完。

`events` 最多 60 条，按发生顺序排列。`kind` 包括 `info`、`write`、`delete`、`read`、`reject`、`sync`、`dr`、`drop`、`join`、`ready`、`lose`、`merge`。

`indexMaster` 的每一行：`user_id`、`data_key`、`node_id`、`ip`、`alias`、`updated_at`、`suspended`。`suspended` 为 `0` 或 `1`。没有值字段。

服务节点：

| 字段 | 说明 |
| --- | --- |
| `id` `name` `role` `region` `ip` `alias` | 身份与地址。`role` 为 `service`。 |
| `status` | `up`、`joining`、`merging`、`lost`。 |
| `covers` | 服务节点恒为空数组。 |
| `reportPeriod` `lastReport` | 这台节点的上报周期，以及上次成功上报的拍。 |
| `reachable` | 是否被标成可访问。 |
| `dbBroken` | 库是否被标成不可达。 |
| `fragmentCount` | 本机数据份数。库不可达时为 `null`。 |
| `fragments` | `user_id`、`data_key`、`value`、`updated_at`。 |
| `outbox` | 待上报。另有 `id`、`op`。`op` 为 `put` 或 `delete`。 |
| `indexEntries` | 持久地址副本。字段与全局地址相同。 |
| `buffer` | 对齐期间暂存的地址。另有 `target_node`、`op`。 |

灾备节点在上述身份字段之外有 `covers`（服务节点 id 数组）和 `records`。记录字段为 `user_id`、`data_key`、`value`、`updated_at`、`home_node`、`organized_tick`。

`updated_at` 是 UTC 的 ISO 字符串，例如 `2026-01-01T00:00:00.000Z`。比较时更晚的字符串更大。
