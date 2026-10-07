# 蜂巢

区域化数据存储 SDK。每个区域的服务节点、中央、灾备各自一个进程。应用只调用本机服务节点。数据留在写下它的那一台。索引只记地址。灾备负责恢复，并把所有权交给接替节点。

## 安装

需要 Node.js 20 或更高版本。默认把局面写在数据目录的 `state.json`。

```bash
npm install
npm run build
```

## 使用

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

## 文档

从 [文档目录](docs/README.md) 进入。方案、生产接口、进程内核对分成三组，一篇只讲一个题目。

## 开发

```bash
npm test
npm run example
```

`npm test` 会先编译 `dist/`，再运行 `test/`。
