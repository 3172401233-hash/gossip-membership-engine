# gossip-membership-engine

SWIM 风格的 gossip 成员状态机（Node.js，零依赖）。管理 `alive → suspect → dead`
状态迁移，用 incarnation 解决冲突：更高 incarnation 可以让节点重新加入，
旧 incarnation 的消息无法复活已被更高版本取代的节点。

## 状态与消息规则

每条 gossip 事件为 `{ type: 'alive'|'suspect'|'dead', node, incarnation }`：

- 事件 incarnation 低于本地记录 → 丢弃（旧消息不生效）；
- `alive` 且 incarnation 更高 → 节点（重新）加入，即使之前是 `dead`；
- `suspect`/`dead` 且 incarnation 不低于本地记录 → 生效；
- 收到关于**自己**的 `suspect`/`dead` → 本地 incarnation 提升到对方 +1 并
  gossip `alive` 进行反驳（refutation），这是分区恢复和伪造 dead 的修复机制；
- `dead` 节点只有携带**严格更高** incarnation 的 `alive` 才能回来。

## 时间模型

引擎不读系统时钟，时间完全由调用方推进：所有方法接受 `now` 参数，
`tick(now)` 驱动内部定时器（探测超时、suspect → dead 晋升）。

## 故障检测参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `probeTimeout` | 1000 ms | 直接探测 / 间接探测等待 ack 的超时；`tick` 中超时的直接探测升级为间接探测，间接探测也超时则标记 suspect |
| `indirectProbeCount` | 2 | 直接探测失败时，通过多少个中间节点做间接探测（ping-req） |
| `suspectTimeout` | 6000 ms | 节点停留在 suspect 多久后晋升为 dead |
| `gossipRetransmits` | 3 | 每条事件被 `getGossip()` 重复广播的次数（抗丢包） |

## API 摘要

- `new Membership({ nodeId, peers, incarnation, ...参数 })`
- `heartbeat(id, incarnation, now)` / `receive(event, now)` — 输入心跳与 gossip 事件
- `beginProbe(target, now)` → `{ type: 'ping' }`；`completeProbe(target, ok, now)` → 失败时返回 `[{ type: 'ping-req', via, target }]`；`completeIndirectProbe(target, via, ok, now)`
- `tick(now)` → `{ indirectProbes }`：推进超时（探测超时、suspect → dead）
- `getGossip()` — 取出待广播事件（每条重传 `gossipRetransmits` 次）
- `fullState()` / `mergeState(records, now)` — 分区合并后的反熵同步：逐条记录按
  incarnation 规则合并，两套成员表不会互相整体覆盖
- `getMember(id)` / `getMembers()` / `aliveNodes()` / `snapshot()` — 查询

## 测试

```sh
node --test
```

`test/membership.test.js` 覆盖状态机单元规则；`test/failure-scenarios.test.js`
用确定性集群模拟覆盖：随机丢包收敛、探测超时降级、单向网络分区（间接探测兜底 /
无间接探测时被怀疑方反驳后重收敛）、伪造 incarnation（受害者提升版本反驳、
旧伪造消息被丢弃）、分区恢复（旧 incarnation 心跳被拒 → 反熵合并 → 反驳重加入 →
成员表收敛）。
