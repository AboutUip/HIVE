# 蜂巢

区域化数据存储 SDK。用户落在附近的服务节点，数据留在写下它的那一台。索引只记地址。灾备负责恢复，并把所有权交给接替节点。

## 安装

需要 Node.js 22.5 或更高版本。库使用 Node 自带的 `node:sqlite`。

```bash
npm install
npm run build
```

## 使用

```javascript
import { createHive } from 'hive'

const hive = createHive({ dataDir: './data' })
hive.reset()

hive.write({
  nodeId: 'svc-a',
  userId: '林夏',
  entries: [{ key: '城市', value: '北京' }]
})
hive.advance('sync')

const result = hive.read({ nodeId: 'svc-b', userId: '林夏', keys: ['城市'] })
```

起始集群、上报周期和灾备周期在 [config/default.json](config/default.json)。`reset()` 按这份配置重建。

## 文档

- [文档目录](docs/README.md)
- [方案](docs/方案.md)
- [API](docs/API.md)

## 开发

```bash
npm test
npm run example
```

`npm test` 会先编译 `dist/`，再运行 `test/`。`npm run example` 在临时目录里走一遍写入、列名单、读取和删除。
