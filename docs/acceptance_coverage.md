# 验收覆盖清单

独立解释器 `tests/reference_interpreter.py` 与生产包零代码共享（不 import
`lineage_auditor`，也不调用证据构建器），只共享声明式 flow JSON 契约。
`tests/test_acceptance.py` 让两边在每个节点上对账：业务袋（多重集）、出现数、
审计边（含 role）、淘汰（含 reason_code）、聚合参与（count/sum 分列）。

## 题目要求的场景与对应用例

| 要求 | 夹具 / 用例 | 断言要点 |
|---|---|---|
| 三值逻辑 filter：false vs unknown | edge_cases `f_unknown` / `test_filter_unknown_distinguished`、`test_three_valued_logic_unit` | NULL 比较记 `filter_unknown`，false 记 `filter_false`；NULL=NULL 为 UNKNOWN |
| anti join 见证右行 | edge_cases `anti_j` / `test_anti_witness`；unknown_residual / `test_anti_null_key_survives_and_real_match_blocked` | reject detail 带 witness_right_occ 与见证行；NULL 键无候选则保留 |
| 复合 NULL 键 | null_keys / `test_null_never_equals_null_in_join` | inner 仅全非空匹配 1 行；left 补空 2 行；anti 保留 2 行 |
| 空聚合 | edge_cases `empty_grp` / `test_empty_aggregate` | 0 行输入产生 1 组：count=0、sum=NULL |
| union 两支路重复 | edge_cases `dup_union`+`dup_distinct` / `test_union_duplicates_kept_then_distinct_merged` | 4 个出现不合并，distinct 后 1 行 4 条 merge 边 |
| 相同排序主键、决胜不同 | edge_cases `top1` / `test_top_k_decisive_tiebreak_and_cutoff` | score 并列时按 decisive tie 选 401；reject 带完整排序键和截止界 |
| 决胜运行时冲突 | `test_top_k_runtime_decisive_collision_rejected` | 执行期拒绝，包标记 failed，trace 被拒 |
| 源自身 NULL 与补空 NULL 相似 | edge_cases `pad_join` / `test_padded_value_looks_like_source_but_is_distinct` | own_null 是真实值、r_* 是补空；padded 标志与 left_padded 边区分 |
| 部分匹配淘汰≠整行消失 | main_demo `partial_join` / `test_partial_match_is_not_whole_row_loss` | 两候选一过一败，左行无 reject 仍输出 |
| inner 无候选 vs 残余失败 | unknown_residual / `test_inner_reasons_split_by_candidate_and_unknown` | 两个 reason_code 分开，残余 detail 带每候选三值判定 |
| sum=0 仍是组成员、count/sum 分列 | main_demo `grp_main` / `test_sum_zero_membership`、`test_trace_distinguishes_contribution_from_loss` | 3 成员；c 的 NULL 仅排除 sum |
| distinct 同值不同 row_id 共一输出 | main_demo `distinct_dup` | 两条 row_id 都追到同一输出，contributors=2 |
| 一支路 false 阻断、另一支路补空保留 | main_demo / `test_one_source_row_blocked_and_another_kept` | verdict 为 partially_contributing；补空无 blocker |
| project 丢 row_id 不丢身份 | main_demo 两个 proj 节点（reference 对账边 role=pass） | 边连续，trace 可穿过投影 |
| 菱形 DAG 按出现去重 | diamond / `test_occurrence_counting_dedups_edge_paths` | 溯源计数是出现集合，不是边路径多重集 |
| 悬空列/schema 歧义/成环/未声明决胜 | `TestPreExecutionRejection` 六个用例 | 执行前 FlowValidationError，不落包 |
| failed/cancelled/僵尸 running 包 | `TestPackageLifecycle` | 全部拒绝 trace；recover 可收口 |
| 分页 | `test_output_pagination`、`test_source_pagination` | 页大小/偏移稳定，total 与全集一致 |
| CLI 生命周期 | tests/test_cli.py | run/trace/node-balance/copy 子进程端到端 |

## 运行

```bash
python3 -I tests/test_acceptance.py   # 34 用例
python3 -I tests/test_cli.py          # 2 个 CLI 端到端用例
python3 -I scripts/benchmark.py       # 规模实测 → out/benchmark_result.json
python3 -I scripts/crosscheck_duckdb.py tests/fixtures/main_demo.json out/demo_pkg
# 原生 DuckDB SQL 重算与落盘证据逐节点对账（top_k 由参考解释器覆盖）
```
