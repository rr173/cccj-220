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

## 更正预演（preview）覆盖

`tests/test_preview.py`（44 用例）。每个差异断言都同时与**独立全量重算**对账：
把更正应用到流程上、用 `tests/reference_interpreter.py` 重跑整个流程，再按分区
对 sink 的 top-k 业务袋做差——预演结果必须与此 oracle 一致。

| 要求 | 用例 | 断言要点 |
|---|---|---|
| 一次跨键替换构造两个分区 | preview_demo / `test_cross_key_replacement_two_partitions` | A 区最后一个真匹配消失、榜单外未改行递补并挤掉旧项；B 区新增真匹配、原榜单项被挤出；手算预期 + oracle 双重核对 |
| 同键残余 unknown 不挡递补 | 同上（lookup row 2 保留键 100、flag=NULL） | 残余 UNKNOWN 不算有效匹配，rec3 照常释放 |
| 多真匹配撤一仍不释放 anti 的对照 | `test_delete_one_of_two_matches_keeps_anti_blocked`、主用例 C 区 | 删除 row 3 后 row 4 仍阻断 rec21，C 区无进出 |
| 整批一起生效 | `test_batch_applies_jointly_swap_case` | 替换迁入键 20 + 删除原键 20 行同批结算，rec2 仍被阻断 |
| left 保留每个有效匹配出现 | `test_every_valid_match_is_an_occurrence` | 删一个匹配，另一个出现递补进榜，不按值合并 |
| 无匹配才补空 / 残余 false 不算匹配 | `test_last_match_loss_pads_and_residual_false_does_not_match` | 最后的真匹配消失→补空行被下游 filter 滤掉→榜外递补 |
| 复合键含 NULL 不匹配（旧键/新键两个方向） | `test_composite_key_with_null_component_never_matches`、`test_null_key_row_gains_a_real_key` | (7,NULL) 不再匹配；(NULL,8)→(8,8) 开始匹配 |
| 同值进出相抵 | `test_same_row_value_in_and_out_cancels` | 底层匹配行变了、投影行值不变 → 空结果 |
| 分支重新汇合不抹出现次数 | `test_release_fans_out_through_both_branches`、`test_one_replace_reblocks_and_releases_across_the_diamond` | 一次释放/封锁沿两条支路各产生一个候选，进出都是两份 |
| 排名末尾决胜冲突整批拒绝 | `test_decisive_conflict_at_ranking_end_rejects_whole_batch` | 冲突在名额外也整批 rejected；包内容不变，后续预演照常 |
| 不存在的 ID / 重复操作 / 类型错误 | `TestCorrectionRejection` 七用例 | rejected，包不变，无部分差异 |
| 不支持的包或路径须解释 | `TestUnsupported` 十二用例 | 非 top_k sink、来源不在上游、来源作左输入/inner 右侧、join 与 sink 间有 group、generate 来源、旧包无 preview.json、缺预演表、failed 包——全部 unsupported/拒绝并说明原因 |
| 空结果与确定性 | `test_empty_batch_and_noop_batches_give_empty_result`、`test_deterministic_repeat` | 空批/恒等替换/无匹配删除 → partitions 为空；同输入两次输出逐字节一致 |
| 无 partition_by 视为一个分区 | `test_join_directly_feeds_top_k_without_partitions` | 分区键输出 `[]`；join 直连 top_k 也支持 |
| NULL 分区键 | `test_null_partition_key` | 分区键以 `{"__null__": true}` 标记输出 |
| 一个来源被多个 scan 使用 | `test_source_scanned_twice_both_joins_affected` | 一批更正同时作用于两个受影响 join |
| 预演不解析含全量源行的流程文件 | `test_preview_never_parses_flow_json` | 删掉 flow.json 后预演结果不变 |
| 读取不随无关数据增长 | `test_reads_stay_flat_as_unrelated_data_grows` | 无关查找键与排名分区 ×10：diff 与 read_stats 完全一致，每条预演 SQL 经 EXPLAIN ANALYZE 核实为 Index Scan（无 Sequential Scan） |
| 包内容预演前后不变 | 每个用例的 `run_preview` 包装 + CLI 用例 | 预演前后包目录逐文件 sha256 一致 |
| 大分区 IN 列表分块 | `test_large_partition_exercises_in_list_chunking` | 650 候选分区正常点查与对账 |
| CLI 端到端 | `TestCliPreview` 四用例 | run+preview 子进程、退出码、stderr JSON 状态、copy 包可预演 |

## 运行

```bash
python3 -I tests/test_acceptance.py   # 34 用例
python3 -I tests/test_preview.py      # 44 个预演用例
python3 -I tests/test_cli.py          # 2 个 CLI 端到端用例
python3 -I scripts/benchmark.py       # 规模实测 → out/benchmark_result.json
python3 -I scripts/crosscheck_duckdb.py tests/fixtures/main_demo.json out/demo_pkg
# 原生 DuckDB SQL 重算与落盘证据逐节点对账（top_k 由参考解释器覆盖）
```
