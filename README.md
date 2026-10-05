# customer-identity-resolution-go

本进程内、本地存储的客户身份归并与统一画像引擎。多渠道身份信号批量接入，
被拆散的客户记录自动归并为身份簇，冲突属性按可配置规则消解为自洽的统一画像。
不依赖外部服务与真实凭据，结论与错误均可在本地稳定复现。

## 快速开始

```bash
go test ./...        # 运行全部自动化测试
go run ./cmd/idemodemo   # 本地样本信号流演示
```

```go
cfg := identity.DefaultConfig("/var/lib/idemo")
e, _ := identity.Open(cfg)   // 重启后自动恢复已提交状态
defer e.Close()
res, _ := e.IngestBatch(signals)          // 批量接入（乱序/重复/迟到均可）
clusters, _ := e.Clusters("analyst")      // 查询身份簇
p, _ := e.Profile("analyst", clusters[0].ID) // 查询统一画像
```

## 归并语义

- **确定性**：最终归属与画像只由信号内容与配置决定。同一输入在任意到达顺序、
  任意批次划分与任意并发度下逐项一致。簇 ID 取簇内最小信号 ID（`C-<minSignalID>`），
  画像版本是画像内容的 FNV-64 哈希，均与到达顺序无关。
- **归并边**：两条信号共享同一可归并键（`MatchKeys` 中相同类型+取值，如
  email/phone/device）即形成归并边；连通分量构成身份簇。常规接入走增量并查集，
  不做全量重算；撤销合并/拆分/删除等罕见管理操作才触发确定性重建。
- **幂等**：相同信号 ID 重复投递被识别并拒绝（隔离区留痕），状态不变。
- **人工合并与撤销**：`Merge` 返回 `mergeID`；`UndoMerge(mergeID)` 撤销该次合并，
  不影响其余身份簇。所有操作是追加到本地 WAL 的事件，重放保序，被撤销的归属
  不会在后续读取或重启重放中复活。`ProfileAsOf(actor, clusterID, seq)` 可查询
  合并与撤销发生前的历史时点状态。
- **拆分与再归并**：`Split(clusterID, SplitRule{ExcludeKeyTypes, Reason})` 按
  可配置依据拆分簇——对该簇成员间由指定键类型形成的归并边施加持久抑制，
  判定依据随事件留痕。拆分后再走正常 `Merge` 归并，结果与直接归并逐项一致
  （见 `TestSplitThenMergeEqualsDirectMerge`）。

## 属性冲突消解规则

同一属性键的多个候选取值按以下字典序取最大者胜出（均可配置、逐项可解释）：

1. **来源优先级**：`Config.SourcePriority[channel]`，未配置渠道用 `DefaultPriority`；
2. **取值可信度**：信号的 `Confidence`（0..1）；
3. **时间新近度**：信号的 `ObservedAt`；
4. **取值字典序**：兜底，保证完全确定。

每个胜出属性附带 `ResolvedAttr.Reason` 说明胜出依据。对外暴露的画像始终是
某个自洽的完整版本（内容哈希即版本号），不做字段级混合。

## 同意与权限

- 信号须携带覆盖 `Config.ProfileScope` 的同意范围（`Signal.Consent`）才进入画像；
  `WithdrawConsent` 撤回后立即退出画像。
- 读取画像需要 `Config.ReadScope` 权限（`Config.Actors` 配置 actor 权限，
  `*` 为通配）；越权读取被拒绝并写入审计日志（`AuditLog`）。
- `DeleteCustomer` 清除该主体的全部信号与派生画像，并留下墓碑：
  后续（包括并发的）写入无法让已删除内容复活，重投直接被拒并留痕。

## 持久化与并发

- 本地 WAL（`DataDir/events.log`，JSONL）：一批事件先整体写入并 fsync，
  再写 commit 标记。恢复时只重放带 commit 标记的完整批次；崩溃留下的
  未完成批次被识别并丢弃，不留部分写入，已提交状态不丢失、不重复应用。
- 查询读取原子发布的不可变快照（无锁），慢查询不阻塞信号接入；
  历史时点查询在写锁外重放事件副本。

## 配置项（Config）

| 字段 | 含义 | 默认 |
|---|---|---|
| `DataDir` | 本地 WAL 目录 | 必填 |
| `SourcePriority` | 渠道来源优先级（大者优） | crm=30, app=20, web=10 |
| `DefaultPriority` | 未配置渠道的默认优先级 | 0 |
| `ProfileScope` | 进入画像所需同意范围 | `profile` |
| `ReadScope` | 读取画像所需权限 | `read:profile` |
| `Actors` | actor → 权限范围列表 | admin 全权 |
| `MaxSignals` | 内存信号总量上限 | 1<<20 |
| `MaxBatchSize` | 单批信号条数上限 | 10000 |
| `MaxBatchDuration` | 单批处理时间上限 | 10s |

超限返回可区分的 `ErrLimit` / `ErrBatchTimeout`；异常信号（缺字段、可信度越界、
重复、主体已删除）进入隔离区（`Quarantined`）并附可定位原因，不静默丢弃。

## 测试覆盖

`identity/engine_test.go` 覆盖：乱序/重复/迟到/任意批次划分下的确定性、
属性冲突消解、误合撤销（含历史时点与重启重放）、拆分再归并一致性、
采集与查询并发（`-race`）、删除复活、撤回同意、越权留痕、
重启恢复与未提交批次识别、超限与隔离。
