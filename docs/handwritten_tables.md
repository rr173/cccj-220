# 主演练：逐节点出现表与去向表（手写，与 `out/demo_pkg` 实际证据一致）

流程文件：`tests/fixtures/main_demo.json`。每个节点的每一行是一个**独立出现（occurrence）**，
出现编号 `occ_id` 是节点内序号；源行身份是 `(source, row_id)`，业务值相同也不合并身份。

## 1. 扫描节点 scan_main（源 src_main）

| occ_id | row_id | grp | amount | tag |
|---:|---:|---:|---:|---|
| 0 | 1 (a) | 7 | 2 | a |
| 1 | 2 (b) | 7 | -2 | b |
| 2 | 3 (c) | 7 | NULL | c |

## 2. 分组节点 grp_main（keys=[grp]，count(*) 为 cnt，sum(amount) 为 amt_sum）

输出出现：

| occ_id | grp | cnt | amt_sum |
|---:|---:|---:|---:|
| 0 | 7 | 3 | 0 |

成员去向（edges，role=group_member）：

| 成员出现 | 去向输出出现 | count(*) 计入 | sum(amount) 计入 | sum 贡献值 |
|---|---|---|---|---:|
| scan_main#0 (a, amount=2) | grp_main#0 | 是 | 是 | 2 |
| scan_main#1 (b, amount=-2) | grp_main#0 | 是 | 是 | -2 |
| scan_main#2 (c, amount=NULL) | grp_main#0 | 是 | **否（NULL 不计 sum）** | NULL |

要点：三条原行**都是组成员**，没有一条被淘汰；`amt_sum=0` 是 2 与 -2 相抵，
不能据此说谁“没有贡献”。c 的 NULL 只被 sum 排除，count(*) 仍计入。
该事实同时落在 `participation` 表（每个聚合每个成员一行 in_count/in_sum）。

## 3. 过滤节点 f_gt2（pred = amount > 2）

输出出现：**0 行**。淘汰记录（rejects）：

| 输入出现 | reason_code | 判定依据 |
|---|---|---|
| scan_main#0 (a, 2) | filter_false | `2 > 2` 为 FALSE |
| scan_main#1 (b, -2) | filter_false | `-2 > 2` 为 FALSE |
| scan_main#2 (c, NULL) | **filter_unknown** | `NULL > 2` 为 UNKNOWN（三值逻辑） |

FALSE 与 UNKNOWN 是两种不同的首次阻断依据，分列两个 reason_code。

## 4. distinct 节点 distinct_dup（源 src_dup，keys=[val]）

扫描出现：`scan_dup#0 = row_id 10, val 42`、`scan_dup#1 = row_id 11, val 42`。

| 输出出现 | 业务值 | 合并进入的输入出现（role=merge） |
|---|---|---|
| distinct_dup#0 | val=42 | scan_dup#0 **和** scan_dup#1 |

两行业务值相同但 row_id 不同，身份不合并；二者都保留一条到同一输出的 merge 边，
trace 任意一条 row_id 都能追到这一条输出，并报告 `total_contributing_input_occurrences=2`。

## 5. 左连接节点 left_branch（left: src_lj row 40，right: src_ljr row 50，on k=k）

左行 k=99，右行只有 k=1，无等值候选。左连接语义：左贡献保留，右侧补 NULL。

| 输出出现 | 左行数据 | 右侧补空列 | padded |
|---|---|---|---|
| left_branch#0 | row_id=40, k=99, note='lonely' | r___row_id=NULL, r_k=NULL, r_info=NULL | **true** |

边 role=`left_padded`。这不是“左行丢失”，trace 中进入 `left_join_padded_kept`，
且不产生任何 reject。

## 6. 投影 proj_filter_branch / proj_left_branch 与 union_branches

- 两个 project 都把 `__row_id` 列从输出 schema 中去掉，只保留 `branch, note`；
  但审计身份由 edges（role=pass）继续承载，**丢列不丢身份**。
- f_gt2 输出 0 行 → proj_filter_branch 0 行；proj_left_branch 1 行。
- union_all 按出现拼接：输出 `union_branches#0`（来自左连接支路），
  输入出现数=1、输出出现数=1。union 永不合并：同值的两支路行若同时到达，
  会是两个独立出现（在 edge_cases 夹具的 dup_union 中专门验证：4 个出现保留，
  再由下游 distinct 合并为 1）。

## 7. 部分匹配内连接 partial_join（左 row 20，右两行 row 30/31，on k=k，残余 r_rv > 15）

| 左出现 | 等值键候选 | 候选残余判定 | 结果 |
|---|---|---|---|
| scan_partial#0 (k=1) | scan_right#0 (rv=10)：`10>15` FALSE；scan_right#1 (rv=20)：`20>15` TRUE | 一假一真 | 仅与 #1 配对输出 |

输出出现 `partial_join#0 = {row_id:20, k:1, v:'p', r___row_id:31, r_k:1, r_rv:20}`，
边：左 #0 → 输出（branch）、右 #1 → 输出（branch）。
该左行**没有任何 reject**：部分候选被残余条件淘汰不等于整行消失。
若所有候选都未过残余，才记 `inner_join_residual_failed`（detail 里带每个候选的三值判定）；
没有等值候选则记 `inner_join_no_equal_key_candidate`。

## 8. 源行 a（row_id=1）的完整多支路去向

| 走向 | 结果 | 首次阻断/保留依据 |
|---|---|---|
| scan_main → grp_main → sink | 到达 grp_main#0（组成员，sum 贡献 +2） | 无阻断，merge 保留贡献 |
| scan_main → f_gt2 → … → union_branches | 在 f_gt2 终止 | **filter_false**：`2>2` 为 FALSE |

`trace-source src_main 1` 的 verdict 是
`partially_contributing_some_branches_blocked`：成功与阻断同时呈现，
不用一个全局 survived 布尔覆盖。

## 9. 源行 40 的完整去向（补空保留，不是丢失）

`scan_lj#0 → left_branch#0 (padded=true) → proj_left_branch#0 → union_branches#0`，
verdict=`contributing`，`first_blockers=[]`，`left_join_padded_kept` 列出补空节点。
即使成品表中按右表业务值搜索不到任何匹配，该行依然可完整溯源。

## 10. 节点平衡（真实计数，非“输入=输出+丢弃”万能公式）

| 节点 | op | 输入出现 | 输出出现 | 淘汰 | 扩张 | 合并 | 适用关系 |
|---|---|---:|---:|---:|---:|---:|---|
| scan_main | scan | 0 | 3 | 0 | 0 | 0 | 每个 row_id 一个出现 |
| f_gt2 | filter | 3 | 0 | 3 | 0 | 0 | 输入=通过+淘汰（false/unknown 分列） |
| grp_main | group | 3 | 1 | 0 | 0 | 3 | 成员 3 折叠为 1 组，0 淘汰 |
| distinct_dup | distinct | 2 | 1 | 0 | 0 | 2 | 2 出现折叠为 1，全部仍是贡献者 |
| left_branch | join(left) | 2 | 1 | 0 | 0 | 0 | 左 1 保留（补空）+ 右 1 未匹配 |
| union_branches | union_all | 1 | 1 | 0 | 0 | 0 | 输出=两支路出现之和，出现保持 1:1 |
| partial_join | join(inner) | 3 | 1 | 0 | 1 | 0 | 左1+右2 输入，1 个通过配对，0 左行淘汰 |

group/distinct 的“合并”、join 的“扩张”、left join 的“补空保留”都无法用
`输入=输出+丢弃` 表达，因此 node-balance 分别报告 rejected / expanded / merged。
