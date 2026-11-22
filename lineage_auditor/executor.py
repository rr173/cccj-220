"""DuckDB-backed flow execution with full lineage evidence.

The executor never guesses fate from SQL text: every node is actually evaluated
in DuckDB, and the produced occurrence rows are then connected to their real
inputs through evidence tables:

  occ            one row per *occurrence* at every node (independent identity)
  edges          parent occurrence -> child occurrence, with a semantic role
  rejects        first-blocker records with reason codes and witness payloads
  participation  group membership plus per-aggregate count/sum participation
  node_stats     real occurrence counts per node (see audit.node_balance)

All evidence is written straight to the on-disk package database; trace queries
walk it with recursive CTEs, so a high-fanout join never has all its paths
expanded into memory at once.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import expr as E
from . import flow as F
from .package import AuditPackage

# reject reason codes ---------------------------------------------------------
R_FILTER_FALSE = "filter_false"
R_FILTER_UNKNOWN = "filter_unknown"
R_INNER_NO_CANDIDATE = "inner_join_no_equal_key_candidate"
R_INNER_RESIDUAL = "inner_join_residual_failed"
R_ANTI_MATCHED = "anti_join_matched_witness"
R_TOPK_CUTOFF = "top_k_below_cutoff"

EDGE_PASS = "pass"          # 1:1 filter/project pass-through
EDGE_BRANCH = "branch"      # union / join / filter fan-out
EDGE_MEMBER = "group_member"
EDGE_MERGE = "merge"        # distinct / group collapse
EDGE_PADDED = "left_padded"

INSERT_OCC_COLS = "(node_id, occ_id, data_json, padded)"
INSERT_EDGE_COLS = "(from_node, from_occ, to_node, to_occ, role)"
INSERT_REJ_COLS = "(reject_id, node_id, input_node, input_occ, reason_code, detail_json)"
INSERT_PART_COLS = ("(node_id, group_occ, member_node, member_occ, agg_name, "
                    "in_count, in_sum, contribution)")


def _sql_value(v: Any) -> str:
    """Inline a scalar into a multi-row VALUES statement (input is our own data)."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _values_stmt(table: str, cols: str, rows: Sequence[Sequence[Any]]) -> str:
    body = ",".join(
        "(" + ",".join(_sql_value(v) for v in row) + ")" for row in rows
    )
    return f"INSERT INTO {table} {cols} VALUES {body}"


class ExecutionError(RuntimeError):
    pass


class Executor:
    def __init__(self, flow: Dict[str, Any], pkg: AuditPackage):
        F.validate(flow)  # fail before touching the package
        self.flow = flow
        self.pkg = pkg
        self.con = pkg.connect(read_only=False)
        self.nodes = F.node_map(flow)
        self.sources = F.source_map(flow)
        self.topo = F.topological_order(flow["nodes"])
        self.schemas = F.output_schemas(flow)
        self._reject_counter = 0
        # batched insert buffers (bounded; flushed at threshold)
        self._occ_buf: List[tuple] = []
        self._edge_buf: List[tuple] = []
        self._rej_buf: List[tuple] = []
        self._part_buf: List[tuple] = []
        # multi-row VALUES statements flush at this size and at node boundaries
        self._buf_limit = 5000
        self.stats: Dict[str, Dict[str, int]] = {}

    # ------------------------------------------------------------------ #
    def run(self) -> Dict[str, Dict[str, int]]:
        # One transaction for the whole build: per-batch commits each trigger an
        # fsync on the WAL and dominate runtime. Downstream reads see earlier
        # inserts because they run on the same connection inside the transaction.
        self.con.execute("BEGIN TRANSACTION")
        try:
            for nid in self.topo:
                self._run_node(self.nodes[nid])
            self._flush_all()
            self._write_stats()
            self.con.execute("COMMIT")
        except BaseException as exc:  # includes KeyboardInterrupt cancellation
            self.con.execute("ROLLBACK")  # discard all buffered evidence atomically
            self.pkg.mark_failed(exc)
            self.con.close()
            if isinstance(exc, KeyboardInterrupt):
                raise ExecutionError("execution cancelled") from exc
            raise
        # indexes after the bulk load, in their own transaction
        from .package import EVIDENCE_INDEXES
        self.con.execute(EVIDENCE_INDEXES)
        self.con.execute("CHECKPOINT")
        self.con.close()
        self.pkg.mark_completed({"nodes": len(self.nodes)})
        return self.stats

    # ------------------------------------------------------------------ #
    # buffered writers
    # ------------------------------------------------------------------ #
    def _flush_occ(self) -> None:
        if self._occ_buf:
            self.con.execute(_values_stmt("occ", INSERT_OCC_COLS, self._occ_buf))
            self._occ_buf.clear()

    def _flush_edge(self) -> None:
        if self._edge_buf:
            self.con.execute(_values_stmt("edges", INSERT_EDGE_COLS, self._edge_buf))
            self._edge_buf.clear()

    def _flush_rej(self) -> None:
        if self._rej_buf:
            self.con.execute(_values_stmt("rejects", INSERT_REJ_COLS, self._rej_buf))
            self._rej_buf.clear()

    def _flush_part(self) -> None:
        if self._part_buf:
            self.con.execute(_values_stmt(
                "participation", INSERT_PART_COLS, self._part_buf))
            self._part_buf.clear()

    def _flush_all(self) -> None:
        self._flush_occ()
        self._flush_edge()
        self._flush_rej()
        self._flush_part()

    def add_occ(self, node_id: str, occ_id: int, data: Dict[str, Any], padded: bool) -> None:
        self._occ_buf.append(
            (node_id, occ_id, json.dumps(data, ensure_ascii=False, separators=(",", ":")), padded)
        )
        if len(self._occ_buf) >= self._buf_limit:
            self._flush_occ()

    def add_edge(self, fn: str, fo: int, tn: str, to: int, role: str) -> None:
        self._edge_buf.append((fn, fo, tn, to, role))
        if len(self._edge_buf) >= self._buf_limit:
            self._flush_edge()

    def add_reject(
        self, node_id: str, input_node: str, input_occ: int,
        reason: str, detail: Dict[str, Any],
    ) -> None:
        self._reject_counter += 1
        self._rej_buf.append(
            (self._reject_counter, node_id, input_node, input_occ, reason,
             json.dumps(detail, ensure_ascii=False, separators=(",", ":")))
        )
        if len(self._rej_buf) >= self._buf_limit:
            self._flush_rej()

    def add_participation(
        self, node_id: str, group_occ: int, member_node: str, member_occ: int,
        agg_name: str, in_count: bool, in_sum: bool, contribution: Optional[int],
    ) -> None:
        self._part_buf.append(
            (node_id, group_occ, member_node, member_occ, agg_name,
             in_count, in_sum, contribution)
        )
        if len(self._part_buf) >= self._buf_limit:
            self._flush_part()

    # ------------------------------------------------------------------ #
    # node dispatch
    # ------------------------------------------------------------------ #
    def _run_node(self, node: Dict[str, Any]) -> None:
        op = node["op"]
        runner = getattr(self, f"_run_{op}")
        runner(node)
        # make every occurrence/edge/reject durable before any downstream node
        # reads its inputs back from DuckDB (buffers are otherwise invisible)
        self._flush_all()

    def _load_scan_rows(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        if source.get("type", "inline") == "inline":
            rows = []
            for r in source["rows"]:
                row = {F.ROW_ID_COL: r["row_id"]}
                row.update(r["values"])
                rows.append(row)
        else:
            # generated scan: dense row_ids 0..count-1, schema columns generated
            # from optional expression specs; defaults: int column = id, text = 'g<id>'
            count = source["count"]
            cols = source["schema"]
            gen = source.get("generate", {})
            rows = []
            for rid in range(count):
                row = {F.ROW_ID_COL: rid}
                for c in cols:
                    spec = gen.get(c, {"kind": "lit", "value": None})
                    if spec == "row_id":
                        row[c] = rid
                    else:
                        row[c] = E.eval_expr(spec, {F.ROW_ID_COL: rid})
                rows.append(row)
        return rows

    def _run_scan(self, node: Dict[str, Any]) -> None:
        source = self.sources[node["source"]]
        rows = self._load_scan_rows(source)
        for i, row in enumerate(rows):
            self.add_occ(node["id"], i, row, False)
        self.stats[node["id"]] = self._blank_stat(op="scan", output=len(rows))

    def _read_input(self, node_id: str) -> List[Tuple[int, Dict[str, Any], bool]]:
        """Read occurrences of a node back from evidence (order = occ_id)."""
        rel = self.con.execute(
            "SELECT occ_id, data_json, padded FROM occ WHERE node_id = ? ORDER BY occ_id",
            [node_id],
        )
        return [(oid, json.loads(dj), pad) for oid, dj, pad in rel.fetchall()]

    def _run_filter(self, node: Dict[str, Any]) -> None:
        pred = node["pred"]
        in_id = node["input"]
        out_occ = 0
        rejected = 0
        for in_occ, row, padded in self._read_input(in_id):
            tv = E.eval_predicate(pred, row)
            if tv is True:
                # a padded occurrence that passes a later filter stays padded
                self.add_occ(node["id"], out_occ, row, padded)
                self.add_edge(in_id, in_occ, node["id"], out_occ, EDGE_PASS)
                out_occ += 1
            else:
                reason = R_FILTER_FALSE if tv is False else R_FILTER_UNKNOWN
                self.add_reject(
                    node["id"], in_id, in_occ, reason,
                    {"predicate_sql": E.to_sql(pred), "truth": "false" if tv is False else "unknown"},
                )
                rejected += 1
        self.stats[node["id"]] = self._blank_stat(
            "filter", input_=out_occ + rejected, output=out_occ, rejected=rejected
        )

    def _run_project(self, node: Dict[str, Any]) -> None:
        in_id = node["input"]
        cols = node["columns"]
        out_occ = 0
        for in_occ, row, pad in self._read_input(in_id):
            out_row: Dict[str, Any] = {}
            for c in cols:
                name = c["name"]
                out_row[name] = row[c["keep"]] if "keep" in c else E.eval_expr(c["expr"], row)
            self.add_occ(node["id"], out_occ, out_row, pad)
            # identity survives even if __row_id itself was projected away
            self.add_edge(in_id, in_occ, node["id"], out_occ, EDGE_PASS)
            out_occ += 1
        total = out_occ
        self.stats[node["id"]] = self._blank_stat("project", input_=total, output=total)

    def _run_union_all(self, node: Dict[str, Any]) -> None:
        out_occ = 0
        total_in = 0
        for in_id in node["inputs"]:
            for in_occ, row, pad in self._read_input(in_id):
                self.add_occ(node["id"], out_occ, row, pad)
                self.add_edge(in_id, in_occ, node["id"], out_occ, EDGE_BRANCH)
                out_occ += 1
                total_in += 1
        # union preserves every occurrence 1:1; same-looking rows from two
        # branches meet here as distinct occurrences (merge happens only downstream).
        self.stats[node["id"]] = self._blank_stat(
            "union_all", input_=total_in, output=out_occ
        )

    # ------------------------------------------------------------------ #
    # joins
    # ------------------------------------------------------------------ #
    def _join_key_tuples(
        self, rows: List[Tuple[int, Dict[str, Any], bool]], on: List[Dict[str, str]],
        side: str,
    ) -> List[Tuple[int, tuple]]:
        out = []
        for oid, row, _pad in rows:
            key = tuple(row[pair[side]] for pair in on)
            out.append((oid, key))
        return out

    def _combine_join_rows(
        self, left: Dict[str, Any], right: Dict[str, Any],
        right_schema: Dict[str, str], prefix: str, padded: bool,
    ) -> Dict[str, Any]:
        out = dict(left)
        for c in right_schema:
            out[prefix + c] = None if padded else right[c]
        return out

    def _run_join(self, node: Dict[str, Any]) -> None:
        kind = node["kind"]
        left_id, right_id = node["left"], node["right"]
        on = node["on"]
        residual = node.get("residual")
        prefix = node.get("right_prefix", "r_")
        left_rows = self._read_input(left_id)
        right_rows = self._read_input(right_id)
        right_schema = self.schemas[right_id]

        l_keys = self._join_key_tuples(left_rows, on, "left")
        r_keys = self._join_key_tuples(right_rows, on, "right")
        left_key_by_occ = dict(l_keys)
        # hash the right side; keys containing NULL never match anything
        index: Dict[tuple, List[int]] = {}
        for rid, key in r_keys:
            if any(k is None for k in key):
                continue
            index.setdefault(key, []).append(rid)
        right_by_occ = {oid: row for oid, row, _pad in right_rows}
        left_by_occ = {oid: row for oid, row, _pad in left_rows}

        out_occ = 0
        rejected = 0
        expanded = 0
        for l_occ, l_row, l_pad in left_rows:
            lkey = left_key_by_occ[l_occ]
            candidate_occs: List[int] = []
            if not any(k is None for k in lkey):
                candidate_occs = index.get(lkey, [])

            if kind == "anti":
                witness = self._first_matching(l_occ, l_row, candidate_occs,
                                               right_by_occ, right_schema, prefix, residual)
                if witness is not None:
                    r_occ, r_row = witness
                    self.add_reject(
                        node["id"], left_id, l_occ, R_ANTI_MATCHED,
                        {
                            "right_node": right_id,
                            "witness_right_occ": r_occ,
                            "witness_row": self._public_row(r_row, right_schema),
                            "equi_key": self._json_key(lkey),
                            "residual_sql": E.to_sql(residual) if residual else None,
                        },
                    )
                    rejected += 1
                else:
                    self.add_occ(node["id"], out_occ, dict(l_row), l_pad)
                    self.add_edge(left_id, l_occ, node["id"], out_occ, EDGE_PASS)
                    out_occ += 1
                continue

            # inner / left: evaluate residual against every equal-key candidate
            passing = self._passing_matches(l_row, candidate_occs, right_by_occ,
                                            right_schema, prefix, residual)
            if not passing:
                if kind == "inner":
                    reason = (R_INNER_NO_CANDIDATE if not candidate_occs
                              else R_INNER_RESIDUAL)
                    self.add_reject(
                        node["id"], left_id, l_occ, reason,
                        self._join_reject_detail(lkey, candidate_occs, right_by_occ,
                                                 right_schema, residual, l_row, prefix),
                    )
                    rejected += 1
                else:  # left join keeps the left contribution via null padding
                    combined = self._combine_join_rows(l_row, {}, right_schema, prefix, True)
                    self.add_occ(node["id"], out_occ, combined, True)
                    self.add_edge(left_id, l_occ, node["id"], out_occ, EDGE_PADDED)
                    out_occ += 1
                continue

            for r_occ in passing:
                r_row = right_by_occ[r_occ]
                combined = self._combine_join_rows(l_row, r_row, right_schema, prefix, False)
                self.add_occ(node["id"], out_occ, combined, False)
                self.add_edge(left_id, l_occ, node["id"], out_occ, EDGE_BRANCH)
                self.add_edge(right_id, r_occ, node["id"], out_occ, EDGE_BRANCH)
                out_occ += 1
                expanded += 1

        stat = self._blank_stat(
            "join",
            input_=len(left_rows) + len(right_rows),
            output=out_occ,
            rejected=rejected,
            expanded=expanded,
        )
        stat["left_input"] = len(left_rows)
        stat["right_input"] = len(right_rows)
        self.stats[node["id"]] = stat

    def _first_matching(
        self, l_occ: int, l_row: Dict[str, Any], candidates: List[int],
        right_by_occ: Dict[int, Dict[str, Any]], right_schema: Dict[str, str],
        prefix: str, residual: Optional[Dict[str, Any]],
    ) -> Optional[Tuple[int, Dict[str, Any]]]:
        for r_occ in candidates:
            r_row = right_by_occ[r_occ]
            if self._residual_true(l_row, r_row, right_schema, prefix, residual):
                return r_occ, r_row
        return None

    def _passing_matches(
        self, l_row: Dict[str, Any], candidates: List[int],
        right_by_occ: Dict[int, Dict[str, Any]], right_schema: Dict[str, str],
        prefix: str, residual: Optional[Dict[str, Any]],
    ) -> List[int]:
        out = []
        for r_occ in candidates:
            r_row = right_by_occ[r_occ]
            if self._residual_true(l_row, r_row, right_schema, prefix, residual):
                out.append(r_occ)
        return out

    def _residual_true(
        self, l_row: Dict[str, Any], r_row: Dict[str, Any],
        right_schema: Dict[str, str], prefix: str,
        residual: Optional[Dict[str, Any]],
    ) -> bool:
        if residual is None:
            return True
        combined = dict(l_row)
        for c in right_schema:
            combined[prefix + c] = r_row[c]
        return E.eval_predicate(residual, combined) is True

    def _join_reject_detail(
        self, lkey: tuple, candidates: List[int],
        right_by_occ: Dict[int, Dict[str, Any]], right_schema: Dict[str, str],
        residual: Optional[Dict[str, Any]],
        l_row: Dict[str, Any], prefix: str,
    ) -> Dict[str, Any]:
        detail: Dict[str, Any] = {
            "equi_key": self._json_key(lkey),
            "equal_key_candidate_count": len(candidates),
            "residual_sql": E.to_sql(residual) if residual else None,
        }
        if candidates and residual is not None:
            evaluated = []
            for r_occ in candidates:
                r_row = right_by_occ[r_occ]
                combined = dict(l_row)
                for c in right_schema:
                    combined[prefix + c] = r_row[c]
                tv = E.eval_predicate(residual, combined)
                evaluated.append({
                    "right_occ": r_occ,
                    "truth": {True: "true", False: "false", None: "unknown"}[tv],
                })
            detail["candidate_residual_truth"] = evaluated
        return detail

    @staticmethod
    def _json_key(key: tuple) -> List[Any]:
        return [{"__null__": True} if k is None else k for k in key]

    @staticmethod
    def _public_row(row: Dict[str, Any], schema: Dict[str, str]) -> Dict[str, Any]:
        return {c: row.get(c) for c in schema}

    # ------------------------------------------------------------------ #
    # group / distinct / top_k
    # ------------------------------------------------------------------ #
    def _run_group(self, node: Dict[str, Any]) -> None:
        in_id = node["input"]
        keys = node.get("keys", [])
        aggs = node["aggs"]
        rows = self._read_input(in_id)
        groups: Dict[tuple, List[Tuple[int, Dict[str, Any]]]] = {}
        for oid, row, _pad in rows:
            gk = tuple(row[k] for k in keys)
            groups.setdefault(gk, []).append((oid, row))

        out_occ = 0
        rejected = 0
        merged = 0
        # deterministic group ordering: by JSON key (nulls first, stable)
        ordered_keys = sorted(groups, key=lambda k: json.dumps(
            self._json_key(k), separators=(",", ":")))
        if not rows:
            # empty aggregate: exactly one group over zero rows
            out_row = {k: None for k in keys}
            for a in aggs:
                out_row[a["name"]] = 0 if a["func"] == "count" else None
            self.add_occ(node["id"], 0, out_row, False)
            out_occ = 1
        for gk in ordered_keys:
            members = groups[gk]
            out_row = {k: v for k, v in zip(keys, gk)}
            for a in aggs:
                name, func, arg = a["name"], a["func"], a.get("arg")
                if func == "count":
                    if arg is None:
                        value = len(members)  # count(*)
                    else:
                        value = sum(1 for _oid, r in members if r[arg] is not None)
                else:
                    value = sum(r[arg] for _oid, r in members if r[arg] is not None)
                out_row[name] = value
            self.add_occ(node["id"], out_occ, out_row, False)
            for m_occ, m_row in members:
                self.add_edge(in_id, m_occ, node["id"], out_occ, EDGE_MEMBER)
                for a in aggs:
                    name, func, arg = a["name"], a["func"], a.get("arg")
                    in_count = func == "count" and (arg is None or m_row[arg] is not None)
                    in_sum = func == "sum" and m_row[arg] is not None
                    contribution = m_row[arg] if in_sum else None
                    self.add_participation(
                        node["id"], out_occ, in_id, m_occ, name,
                        in_count, in_sum, contribution,
                    )
                merged += 1
            out_occ += 1

        self.stats[node["id"]] = self._blank_stat(
            "group",
            input_=len(rows),
            output=out_occ,
            rejected=rejected,
            merged=merged,
        )

    def _run_distinct(self, node: Dict[str, Any]) -> None:
        in_id = node["input"]
        keys = node["keys"]
        rows = self._read_input(in_id)
        groups: Dict[tuple, List[int]] = {}
        order: List[tuple] = []
        for oid, row, _pad in rows:
            dk = tuple(row[k] for k in keys)
            if dk not in groups:
                groups[dk] = []
                order.append(dk)
            groups[dk].append(oid)
        out_occ = 0
        merged = 0
        for dk in order:
            member_occs = groups[dk]
            first_occ = member_occs[0]
            rep_row = next(row for oid, row, _p in rows if oid == first_occ)
            out_row = {c: rep_row[c] for c in self.schemas[node["id"]]}
            self.add_occ(node["id"], out_occ, out_row, False)
            for m_occ in member_occs:
                self.add_edge(in_id, m_occ, node["id"], out_occ, EDGE_MERGE)
                merged += 1
            out_occ += 1
        self.stats[node["id"]] = self._blank_stat(
            "distinct", input_=len(rows), output=out_occ, merged=merged
        )

    def _run_top_k(self, node: Dict[str, Any]) -> None:
        in_id = node["input"]
        k = node["k"]
        partitions = node.get("partition_by", [])
        order_terms = node["order_by"]
        rows = self._read_input(in_id)
        parts: Dict[tuple, List[Tuple[int, Dict[str, Any]]]] = {}
        part_order: List[tuple] = []
        for oid, row, _pad in rows:
            pk = tuple(row[p] for p in partitions)
            if pk not in parts:
                parts[pk] = []
                part_order.append(pk)
            parts[pk].append((oid, row))

        out_occ = 0
        rejected = 0
        for pk in part_order:
            members = parts[pk]
            ranked = sorted(members, key=lambda mr: self._sort_key(mr[1], order_terms))
            # decisive tie check: the declared decisive suffix must be unique
            decisive_idx = [i for i, t in enumerate(order_terms) if t.get("decisive")]
            seen_dec: Dict[tuple, int] = {}
            for oid, row in ranked:
                dk = tuple(self._order_value(row, order_terms[i]) for i in decisive_idx)
                if dk in seen_dec:
                    raise ExecutionError(
                        f"top_k {node['id']!r}: decisive tie-break tuple {dk} "
                        f"collides for occ {oid} and occ {seen_dec[dk]}"
                    )
                seen_dec[dk] = oid
            kept = ranked[:k]
            cutoff = self._cutoff_record(ranked, k, order_terms)
            for oid, row in kept:
                self.add_occ(node["id"], out_occ, dict(row), False)
                self.add_edge(in_id, oid, node["id"], out_occ, EDGE_PASS)
                out_occ += 1
            for oid, row in ranked[k:]:
                self.add_reject(
                    node["id"], in_id, oid, R_TOPK_CUTOFF,
                    {
                        "partition": self._json_key(pk),
                        "k": k,
                        "full_ordering": [
                            {
                                "expr_sql": E.to_sql(t["expr"]),
                                "dir": t.get("dir", "asc"),
                                "decisive": bool(t.get("decisive")),
                                "value": self._order_value(row, t),
                            }
                            for t in order_terms
                        ],
                        "cutoff_last_kept": cutoff,
                        "nulls": "last for both asc and desc",
                    },
                )
                rejected += 1
        self.stats[node["id"]] = self._blank_stat(
            "top_k", input_=len(rows), output=out_occ, rejected=rejected
        )

    @staticmethod
    def _order_value(row: Dict[str, Any], term: Dict[str, Any]) -> Any:
        v = E.eval_expr(term["expr"], row)
        if v is None:
            return {"__null__": True}
        return v

    def _sort_key(
        self, row: Dict[str, Any], terms: List[Dict[str, Any]]
    ) -> List[Tuple[int, Any]]:
        key: List[Tuple[int, Any]] = []
        for term in terms:
            v = E.eval_expr(term["expr"], row)
            is_null = v is None
            # NULLS LAST regardless of direction
            null_flag = 1 if is_null else 0
            if is_null:
                key.append((null_flag, 0))
                continue
            if term.get("dir", "asc") == "desc":
                if isinstance(v, bool):
                    v = not v
                elif isinstance(v, (int, float)):
                    v = -v
                else:
                    v = _NegText(v)
            key.append((null_flag, v))
        return key

    def _cutoff_record(
        self, ranked: List[Tuple[int, Dict[str, Any]]], k: int,
        terms: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if len(ranked) <= k:
            return None
        last_kept_oid, last_kept_row = ranked[k - 1]
        return {
            "occ_id": last_kept_oid,
            "order_values": [self._order_value(last_kept_row, t) for t in terms],
        }

    # ------------------------------------------------------------------ #
    def _write_stats(self) -> None:
        for nid, st in self.stats.items():
            self.con.execute(
                "INSERT INTO node_stats VALUES (?, ?, ?, ?, ?, ?, ?)",
                [nid, st["op"], st["input_occ"], st["output_occ"],
                 st["rejected"], st["expanded"], st["merged"]],
            )

    @staticmethod
    def _blank_stat(
        op: str, input_: int = 0, output: int = 0, rejected: int = 0,
        expanded: int = 0, merged: int = 0,
    ) -> Dict[str, int]:
        return {
            "op": op,
            "input_occ": input_,
            "output_occ": output,
            "rejected": rejected,
            "expanded": expanded,
            "merged": merged,
        }


class _NegText:
    """Wrapper that reverses text ordering for DESC sorts."""
    __slots__ = ("v",)

    def __init__(self, v: str):
        self.v = v

    def __lt__(self, other: "_NegText") -> bool:
        return self.v > other.v

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _NegText) and self.v == other.v
