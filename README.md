<div align="center">

# 蜂巢

**区域化数据存储 · 数据留在写下它的节点 · 索引只记地址 · 灾备完成恢复与所有权转移**

<br/>

[![Node.js](https://img.shields.io/badge/Node.js-%E2%89%A5%2020-339933?logo=nodedotjs&logoColor=white)](package.json)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-39%2F39-4f6ef7)](test/)

<br/>

</div>

---

## 一句话

应用只调用本机服务节点。数据留在写下它的那一台，中央只记地址，灾备负责恢复，并把所有权交给接替节点。整个集群在一个进程里可以按拍核对，上线的是同一个语义的投影。

| 角色 | 进程 | 管什么 |
| --- | --- | --- |
| 中央 | `startCentral` | 地址索引、上报确认、挂失裁定 |
| 服务节点 | `startService` | 数据本身，本机读写 |
| 灾备 | `startDr` | 稳定副本，只喂恢复，不接读 |

## 什么保证

**驻留一致性**：每个键在任一时刻最多有一个家；读到的值要么来自家，要么被拒绝或挂起——绝不静默给出旧值。比纯最终一致强（拒绝显式、窗口有界），比因果一致弱（读不进依赖链）。

- 时间是 HLC（物理位 + 逻辑位）：因果在后的写必然拿到更晚的时间戳，和墙钟准不准无关
- 四个不变式（挂失必挂起、融合原子、挂起单调、灾备负责）由随机操作性质测试逐项盯守
- 挂失时请求**停在原地**，装好后统一换上新地址

完整定义见 [一致性](docs/方案/一致性.md)，谱系位置见 [谱系](docs/谱系.md)。

## 安装

需要 Node.js 20 或更高版本。默认把局面写在数据目录的 `state.json`（增量在 `hive.wal`）。

```bash
npm install
npm run build
```

## 快速开始

```javascript
import { startCentral, startService } from 'hive'

const central = await startCentral({
  dataDir: './data/central',
  token: '换成你们自己的令牌',
  nodes: [
    { id: 'svc-a', name: '华北节点', role: 'service', region: '华北', alias: 'svc-a' },
    { id: 'svc-b', name: '华东节点', role: 'service', region: '华东', alias: 'svc-b' },
    { id: 'dr-1', name: '灾备甲', role: 'dr', region: '集中', alias: 'dr-1', covers: ['svc-a', 'svc-b'] }
  ]
})

const link = { host: central.host, port: central.http.port }
const north = await startService({
  dataDir: './data/svc-a',
  token: '换成你们自己的令牌',
  id: 'svc-a',
  name: '华北节点',
  region: '华北',
  central: link
})

await north.write({ userId: '林夏', entries: [{ key: '城市', value: '北京' }] })
await north.report()
```

华东节点用同样的方式连上同一台中央之后，`read` 会按地址向华北要这一份数据，并且不写入华东的库。上报间隔默认 2 秒，灾备间隔默认 6 秒，容错 1 秒。到点没有上报，中央会来要日志；还是联系不上，就挂失并从灾备装入另一台活着的服务节点。

进程内的 `createHive` 只核对方案，不作为线上入口。

## 线上能力

| 能力 | 入口 |
| --- | --- |
| TLS | `startCentral` / `startService` / `startDr` 的 `tls` 参数，整套集群加密互通 |
| 健康检查 | 三个角色的 `GET /v1/health`，含计数器和熔断状态 |
| 索引重建 | `rebuildCentralIndexes`，中央局面全丢也能从碎片和灾备重建 |
| 背压 | 待上报积压到 `maxOutbox` 拒绝写入，挂失时不会一次丢一堆 |
| 读熔断 | 连续取不到同一别名自动跳直连，先向中央要新地址 |
| 留档 | `retention` 把用过的日志写成文件，超限自动轮转 |

细节见 [运维](docs/接口/运维.md)。

## 存储

| 存储 | 适用 | 说明 |
| --- | --- | --- |
| `FileStore` | 默认 | `state.json` 快照 + `hive.wal` 增量，事务级 fsync |
| `MemoryStore` | 核对 | 只活在这个进程里，证明逻辑不绑某一种文件 |
| `SqliteStore` | 可选 | 需要 Node.js 22.5+，入口 `hive/sqlite` |

## 文档

从 [文档目录](docs/README.md) 进入。方案、生产接口、进程内核对分成三组，一篇只讲一个题目。

| 组 | 用来 |
| --- | --- |
| [方案](docs/README.md#方案) | 数据留在哪、地址记什么、上报、灾备、挂失和所有权怎么转移 |
| [接口](docs/README.md#接口) | 在生产里启动三类进程，并调用本机服务节点 |
| [核对](docs/README.md#核对) | 把整个集群放在一个进程里，对照方案 |

## 开发

```bash
npm test           # 编译 dist/ 后跑全部测试
npm run example    # quickstart 示例
npm run pack:check # 预览发布包内容
```

## 版权

Copyright © 2026 小萱baibai，蜂巢 (Hive)。[MIT License](LICENSE)。
