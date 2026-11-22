# 离线加工审计器（Offline Lineage Auditor）

对一份声明式关系流程（有限无环图）做真实执行，并为**每一条原行**生成完整去向证据：
区分真正被淘汰（filter false/unknown、join 无候选/残余失败、anti 见证、top_k 截止）
与仍有贡献但不再单独成行（group 成员、distinct 合并、left join 补空保留、union 支路相遇）。

> 核心原则：**“在成品表里按业务值搜不到”不等于行丢失。** 一切结论来自实际执行落盘的
> 出现/边/淘汰证据，而不是静态读 SQL 猜测。

## 目录

```
lineage_auditor/      生产包（不依赖 tests/）
  expr.py             声明式表达式 + SQL 三值逻辑（TRUE/FALSE/UNKNOWN）
  flow.py             流程模型、schema 解析、执行前校验
  executor.py         DuckDB 执行 + 出现/边/淘汰/参与证据落盘
  audit.py            trace-source / trace-output / node-balance（递归 CTE + 分页）
  preview.py          更正预演：一批查找表更正 → 指定 top_k 名单的进出差异
  preview_support.py  run 时一并构建的预演索引（点查专用单列表键）
  package.py          自包含审计包生命周期（running/completed/failed/cancelled）
  archive.py          历史库：五张核心证据表的内容寻址归档与按版本恢复
  cli.py              命令行
tests/
  reference_interpreter.py  独立小型出现级解释器（不 import 生产包/证据构建器）
  test_acceptance.py        业务袋 + 审计边全量对账，及边界语义测试（34 用例）
  test_preview.py           预演验收（44 用例：语义/拒绝/不支持/规模/CLI）
  test_archive.py           历史库归档/恢复（16 用例：无损多重集/不可变/崩溃可见性）
  test_cli.py               CLI 端到端测试（2 用例）
  fixtures/                 main_demo / edge_cases / null_keys / unknown_residual
                            / diamond / scale_benchmark / preview_demo
                            / archive_demo_v1 / archive_demo_v2
scripts/benchmark.py        高扇出 + 大共同来源的分页/内存/证据规模实测
scripts/crosscheck_duckdb.py 把流程渲染成原生 DuckDB SQL 重算，与落盘证据对账
scripts/archive_drill.py    历史库可重跑演练（恢复对账/重试/冲突/中断/共享计量）
docs/handwritten_tables.md  主演练逐节点手写出现表与去向表
docs/acceptance_coverage.md 验收覆盖清单
vendor/duckdb_pkg/    解包的 DuckDB wheel（环境无 pip 时使用）
```

## 快速开始

```bash
# 1. 执行前校验 + 执行，生成自包含审计包
python3 -I bin/auditor run --flow tests/fixtures/main_demo.json --package out/demo_pkg

# 2. 正向溯源：一条原行的完整去向（支持分页 --page-size/--page）
python3 -I bin/auditor trace-source --package out/demo_pkg \
    --source src_main --row-id 1 --sinks grp_main,union_branches

# 3. 反向溯源：一个成品出现由哪些原行构成
python3 -I bin/auditor trace-output --package out/demo_pkg --sink partial_join --occ-id 0

# 4. 节点平衡（真实计数，非万能公式）
python3 -I bin/auditor node-balance --package out/demo_pkg

# 5. 聚合成员参与证据（count/sum 分别标注）
python3 -I bin/auditor participation --package out/demo_pkg --node grp_main --occ-id 0

# 6. 复制为可独立查询的包
python3 -I bin/auditor copy-package --package out/demo_pkg --dest /tmp/pkg_copy
```

## 历史库归档与按版本恢复（archive / restore）

每周发布只改少数记录，却整份复制 `evidence.duckdb` 会让证据占多倍空间。
`archive` 把**已完成**包的五张核心证据表（`occ`、`edges`、`rejects`、
`participation`、`node_stats`）存入一个跨版本共享的历史库；`restore` 只凭
版本名就能把当时的包无损重建到一个空目录，不依赖旧工作目录、也不重跑加工。

```bash
# 归档（只读原包，绝不修改；库目录首次自动创建）
python3 -I bin/auditor archive --package out/rel_2026w47 \
    --library out/history --version rel-2026w47

# 恢复到一个不存在的目录；恢复包与原包五表全字段多重集完全一致
python3 -I bin/auditor restore --library out/history \
    --version rel-2026w47 --dest out/review_rel_2026w47
```

**为什么只改少数证据时不用重写整套负载**：每条证据行按**类型化的规范字节**
（含表名、固定列序、NULL/布尔/整数/文本的显式标签）计算 SHA-256，行的物理
顺序与 DuckDB 页布局不参与身份。同表的行按哈希分进内容寻址的**桶块**（桶数
随表规模取 2 的幂，目标是每块约 4 行），桶块文件名就是其负载哈希；新版本里
未变化的桶哈希不变、文件名不变，因此根本不重写，只有受影响的少数桶产生新
文件。块内每条不同的行只存一份并带出现次数，所以重复边、同值不同出现都按
多重集无损保留，而不会被按行值去重。版本目录只放各自的 `flow.json`、
`manifest.json` 和一份只含哈希与计数的 `version.json`——证据负载不藏在清单或
索引里，计量时 `chunks/` 下的字节就是实际证据字节。

**不可变与幂等**：

- 同名 + 同 flow/清单/证据重复归档：成功且不重复写任何证据（`already_archived`）；
- 同名但任一内容不同：明确拒绝（exit 2），已归档版本永不覆盖；
- 不同版本名可以登记完全相同的证据（共享同一份桶块，零新增证据字节）；
- 未完成（running/failed/cancelled）或缺任一核心证据表的包一律拒绝。

**整体可见性**：桶块全部是不可变的内容寻址文件，版本仅在 `versions/<名>/`
目录被原子改名后才可见。写入中途被硬杀：旧版本照常可恢复，新版本尚不可见、
可安全重试（重试时已落盘的桶块自动跳过）；一旦可见，它引用的每个块都已在
库中，必然能完整恢复。恢复只从历史库读，先在同级临时目录构建，成功才改名到
目标；目标目录已存在则拒绝，失败不会留下伪装成 completed 的包。

恢复包保留原出现编号、端点、角色、补空标志以及拒绝与聚合参与字段，可直接用
现有 `trace-source` / `trace-output` / `node-balance` / `participation`
复核旧发布（也能通过 `scripts/crosscheck_duckdb.py` 的独立原生 SQL 对账）。
它不携带预演索引，**不能用于 `preview`**（该命令会明确拒绝），也不创建任何
新的加工或预演结果。可重跑的端到端演练见 `scripts/archive_drill.py`。


## 更正预演（preview）

名单发布前，如果要更正加工用查找表里的若干映射，`preview` 可以在**不动原审计包**
的前提下预演：拿一份已完成的审计包 + 一批拟议更正，输出指定 `top_k` 名单的
进出差异。它只预演——不修改原包、不生成第二份审计包、不提交更正。

```bash
# corrections.json: 按来源 row_id 删除整行，或保留 row_id 用整行替换（可改关联键）
# {
#   "source": "src_lookup",
#   "corrections": [
#     {"op": "replace", "row_id": 1, "values": {"k": 200, "flag": 1, "note": "moved"}},
#     {"op": "delete", "row_id": 3}
#   ]
# }
python3 -I bin/auditor run --flow tests/fixtures/preview_demo.json --package out/pv_pkg
python3 -I bin/auditor preview --package out/pv_pkg --sink top1 --corrections corrections.json
```

输出按分区给出 `added` / `removed`：完整 sink 行值 + 出现次数差（同一行值进出
相抵，仅顺序变化不算进出）；没有进出的分区不出现，整批无进出时 `partitions`
为空。同一批更正一起生效；不存在的 row_id、重复操作、类型不符的替换行会被
`rejected` 拒绝；更正当且仅当会让某分区的决胜字段元组变为空值或不再唯一时，
整批被拒绝（即使冲突在名额之外），失败不输出部分差异。

**支持范围**（不满足时以 `unsupported` 说明原因，绝不静默退回全流程重跑）：

- 一次一个以 `top_k` 结尾的 sink + 一个 inline 查找来源；
- 该来源在目标上游只能作为 `left` / `anti` join 的直接右侧 scan，且这些 join
  的左输入不依赖该来源（左输入已在包里算好，直接复用）；
- 从受影响 join 到终点只经过 filter / project / union_all 和最后的 top_k，
  允许分支重新汇合；范围以该 sink 的祖先为准，无关节点不受限制。

**语义与旧键新键**：复合等值键含 NULL 永不匹配；残余谓词只有 TRUE 才算有效
匹配。`left` 保留每个有效匹配的出现，仅无匹配时补空；`anti` 要等最后一个有效
匹配消失才释放左行。旧榜单外的行可能递补，新候选也可能把未被直接更正的记录
挤出。排名沿用声明的排序、NULLS LAST 与决胜规则。

**读取有界**：`run` 建包时已一并准备好预演索引（`preview.json` 无行结构 +
单列键索引表）。预演本身不再解析含全量源行的 flow.json，也不整表读取/重算
中间表——只按相关键、受影响出现和受影响分区做点查；输出里的 `read_stats`
是实际读取观测，无关查找键与无关分区增多时保持不变（`tests/test_preview.py`
的规模用例同时用 EXPLAIN ANALYZE 核对每条预演 SQL 都是 Index Scan）。
旧包没有预演索引：不迁移，`preview` 会说明需重新 `run` 建包。

## 节点与证据模型

| 节点 | 语义要点 | 淘汰 reason_code / 特殊边 |
|---|---|---|
| scan | 每个源 row_id 一个出现，同值不同 id 不合并 | — |
| filter | 三值逻辑 | `filter_false` / `filter_unknown` |
| project | 1:1，丢掉 `__row_id` 列不丢审计身份（pass 边承载） | — |
| union_all | 按出现拼接，两支路同值行仍是两个出现 | branch |
| join inner | 无等值候选 vs 候选均不过残余，分开记录 | `inner_join_no_equal_key_candidate` / `inner_join_residual_failed` |
| join left | 无通过匹配时左行保留、右侧补 NULL | `left_padded` 边 + `padded=true`，**不是丢失** |
| join anti | 被一个实际匹配阻断时交出见证右行 | `anti_join_matched_witness`（含 witness occ/行） |
| group | 成员折叠为组；sum=0/NULL 不影响成员身份；count/sum 参与分列入 participation | group_member |
| distinct | 合并后保留全部原行贡献，都能追到同一输出 | merge |
| top_k | 必须声明完全决胜字段；未入选给完整排序键与截止界 | `top_k_below_cutoff` |

连接键含 NULL 时永不匹配（NULL = NULL 为 UNKNOWN），复合键同理。

## 执行前拒绝（不产生正常包）

悬空列、schema 歧义（project 重名列、join 输出同名覆盖、union schema 不一致）、
成环、top_k 未声明决胜字段，均在 `validate` 阶段抛 `FlowValidationError`。
运行时若决胜字段实际取值冲突，executor 抛 `ExecutionError` 并把包标记为 failed。

## 独立验收与原生 SQL 对账

```bash
python3 -I tests/test_acceptance.py      # 34 用例，全部通过
python3 -I tests/test_preview.py         # 44 个预演用例，全部通过
python3 -I tests/test_archive.py         # 16 个历史库归档/恢复用例，全部通过
python3 -I tests/test_cli.py             # 2 个 CLI 端到端用例
python3 -I scripts/benchmark.py          # 高扇出分页/内存实测 → out/benchmark_result.json
python3 -I scripts/crosscheck_duckdb.py tests/fixtures/main_demo.json out/demo_pkg
python3 -I scripts/archive_drill.py      # 历史库演练：恢复对账/重试/冲突/中断/共享计量
```

`tests/reference_interpreter.py` 是与生产代码零共享的独立解释器：它用显式出现编号
重新计算每个节点的业务袋、审计边、淘汰与聚合参与，测试再与 DuckDB 落盘证据逐项对账。
`scripts/crosscheck_duckdb.py` 走另一条独立路径：把声明式流程渲染成**原生 DuckDB SQL**
（含三值 WHERE、inner/left/anti join、group、distinct、空聚合兜底行），在全新内存库
执行后与审计包的 occ 证据对账，证明证据与真实执行相符而非静态猜测。top_k 由参考解释器覆盖。

覆盖：filter unknown、anti 见证、复合 NULL 键、空聚合、union 两支路重复、
相同排序主键但决胜不同的 top_k、决胜运行时冲突、源自身 NULL 与补空 NULL 共存、
inner 残余条件 unknown、菱形 DAG 按出现去重。

## 分页与内存（实测，见 out/benchmark_result.json）

所有溯源走 DuckDB 递归 CTE，可达集落在服务端临时表，Python 只取当前页与计数。
规模演练（1000 左 × 100 右 = 10 万连接输出、1 万共同组成员）实测：

- 证据：111,101 个出现、210,000 条边、20,000 条参与记录，库文件约 10.8 MB；
- 追踪其中一条原行（其 100 个连接输出）：分 4 页、每页 25 行取完，
  **Python tracemalloc 峰值约 88 KB**，递归 CTE 在 DuckDB 内完成，未把边表拉进内存；
- 共同组里任一成员都能追到唯一组输出，并报告 10,000 个贡献出现。

构建采用单事务 + 多行 VALUES 批量落盘，索引在批量装载后一次性建立。

## 失败/取消包

包目录 `manifest.json` 的 status 为 running/completed/failed/cancelled。
failed/cancelled（以及硬杀后停在 running、经 `recover` 收口）的包在任何 trace
命令打开时被拒绝（`require_completed`），不会交付半成品 trace。
