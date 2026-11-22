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
  package.py          自包含审计包生命周期（running/completed/failed/cancelled）
  cli.py              命令行
tests/
  reference_interpreter.py  独立小型出现级解释器（不 import 生产包/证据构建器）
  test_acceptance.py        业务袋 + 审计边全量对账，及边界语义测试（34 用例）
  test_cli.py               CLI 端到端测试（2 用例）
  fixtures/                 main_demo / edge_cases / null_keys / unknown_residual
                            / diamond / scale_benchmark
scripts/benchmark.py        高扇出 + 大共同来源的分页/内存/证据规模实测
scripts/crosscheck_duckdb.py 把流程渲染成原生 DuckDB SQL 重算，与落盘证据对账
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
python3 -I tests/test_cli.py             # 2 个 CLI 端到端用例
python3 -I scripts/benchmark.py          # 高扇出分页/内存实测 → out/benchmark_result.json
python3 -I scripts/crosscheck_duckdb.py tests/fixtures/main_demo.json out/demo_pkg
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
