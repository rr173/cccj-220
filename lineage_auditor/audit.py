"""Read-side audit queries over a completed evidence package.

All lineage walking is done with recursive CTEs *inside DuckDB* against the
on-disk evidence tables, plus bounded pagination: asking for one source row
never materializes every path of a high-fanout join into Python memory.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from . import flow as F
from .package import AuditPackage, PackageError


class AuditReader:
    def __init__(self, pkg: AuditPackage):
        self.pkg = pkg
        self.pkg.require_completed()
        self.flow = pkg.load_flow()
        self.nodes = F.node_map(self.flow)
        self.con = pkg.connect(read_only=True)

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> "AuditReader":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    def _scan_node_of(self, source_name: str) -> str:
        for n in self.flow["nodes"]:
            if n["op"] == "scan" and n["source"] == source_name:
                return n["id"]
        raise PackageError(f"no scan node reads source {source_name!r}")

    def _occ_data(self, node_id: str, occ_id: int) -> Dict[str, Any]:
        dj, padded = self.con.execute(
            "SELECT data_json, padded FROM occ WHERE node_id=? AND occ_id=?",
            [node_id, occ_id],
        ).fetchone()
        row = json.loads(dj)
        if padded:
            row["__padded__"] = True
        return row

    # ------------------------------------------------------------------ #
    # forward trace
    # ------------------------------------------------------------------ #
    def trace_source(
        self,
        source_name: str,
        row_id: int,
        sinks: Optional[List[str]] = None,
        page_size: int = 50,
        page: int = 0,
    ) -> Dict[str, Any]:
        """Full fate explanation for one original source row.

        Surviving contributions and first blockers are reported per branch; a
        source row can both survive (one branch) and be blocked (another).
        """
        scan_node = self._scan_node_of(source_name)
        sink_ids = sinks or list(self.flow["sinks"])
        for s in sink_ids:
            if s not in self.nodes:
                raise PackageError(f"unknown sink {s!r}")
            if self.nodes[s]["op"] == "scan":
                raise PackageError(f"{s!r} is a scan, not a sink output")

        start = self.con.execute(
            "SELECT occ_id FROM occ WHERE node_id=? "
            "AND CAST(json_extract(data_json, ?) AS BIGINT) = ?",
            [scan_node, f'$."{F.ROW_ID_COL}"', row_id],
        ).fetchall()
        if not start:
            raise PackageError(
                f"source {source_name!r} has no row_id={row_id}"
            )
        scan_occ = start[0][0]

        # All reachability work stays inside DuckDB: a recursive CTE materializes
        # the reachable set into a temp table and every later query joins it.
        # Python only receives paginated rows and counts, never the full path set.
        self.con.execute("CREATE TEMP TABLE _reach(node_id VARCHAR, occ_id BIGINT, depth INTEGER)")
        try:
            self.con.execute(
                """
                INSERT INTO _reach
                WITH RECURSIVE reach(node_id, occ_id, depth) AS (
                    SELECT ?, ?, 0
                    UNION ALL
                    SELECT e.to_node, e.to_occ, r.depth + 1
                    FROM reach r JOIN edges e
                      ON e.from_node = r.node_id AND e.from_occ = r.occ_id
                )
                SELECT DISTINCT node_id, occ_id, min(depth) AS depth
                FROM reach GROUP BY node_id, occ_id
                """,
                [scan_node, scan_occ],
            )
            reach_count = self.con.execute("SELECT count(*) FROM _reach").fetchone()[0]

            page_rows, total_outputs = self._surviving_outputs(sink_ids, page_size, page)
            all_blockers = self._blockers()
            # scope: a blocker belongs to this trace only when its node can
            # actually lead to one of the requested sinks
            relevant_ancestors: set = set()
            for s in sink_ids:
                relevant_ancestors |= self._ancestors(s)
            blockers = [b for b in all_blockers
                        if b["blocked_at_node"] in relevant_ancestors]
            other_blockers = [
                {"blocked_at_node": b["blocked_at_node"], "reason_code": b["reason_code"]}
                for b in all_blockers
                if b["blocked_at_node"] not in relevant_ancestors
            ]
            padded = self._padded_events()
            node_hits = self._node_hits()
            branch_fate = self._branch_fate(sink_ids, blockers)
        finally:
            self.con.execute("DROP TABLE _reach")

        merges = self._merge_contributions(scan_node, scan_occ)

        return {
            "source": source_name,
            "row_id": row_id,
            "scan_node": scan_node,
            "scan_occ": scan_occ,
            "sinks_requested": sink_ids,
            "verdict": self._verdict(total_outputs, blockers),
            "reachable_occurrence_total": reach_count,
            "surviving_outputs": {
                "total": total_outputs,
                "page": page,
                "page_size": page_size,
                "rows": page_rows,
            },
            "merge_destinations": merges,
            "first_blockers": blockers,
            "blockers_on_other_paths_not_requested": other_blockers,
            "left_join_padded_kept": padded,
            "reachable_node_occurrences": node_hits,
            "branch_fate": branch_fate,
        }

    def _surviving_outputs(
        self, sink_ids: List[str], page_size: int, page: int
    ) -> tuple:
        total = self.con.execute(
            "SELECT count(*) FROM _reach r WHERE r.node_id IN (SELECT unnest(?))",
            [sink_ids],
        ).fetchone()[0]
        rows = self.con.execute(
            """
            SELECT r.node_id, r.occ_id, o.data_json, o.padded
            FROM _reach r JOIN occ o
              ON o.node_id = r.node_id AND o.occ_id = r.occ_id
            WHERE r.node_id IN (SELECT unnest(?))
            ORDER BY r.node_id, r.occ_id
            LIMIT ? OFFSET ?
            """,
            [sink_ids, page_size, page * page_size],
        ).fetchall()
        out = []
        for node_id, occ_id, dj, padded in rows:
            out.append({
                "sink": node_id,
                "occ_id": occ_id,
                "row": json.loads(dj),
                "left_join_padded": bool(padded),
            })
        return out, total

    def _blockers(self) -> List[Dict[str, Any]]:
        rows = self.con.execute(
            """
            SELECT x.node_id, x.input_node, x.input_occ, x.reason_code, x.detail_json
            FROM rejects x JOIN _reach r
              ON r.node_id = x.input_node AND r.occ_id = x.input_occ
            ORDER BY x.node_id, x.reject_id
            """
        ).fetchall()
        return [
            {
                "blocked_at_node": nid,
                "input_node": in_node,
                "input_occ": in_occ,
                "reason_code": reason,
                "detail": json.loads(dj),
            }
            for nid, in_node, in_occ, reason, dj in rows
        ]

    def _padded_events(self) -> List[Dict[str, Any]]:
        rows = self.con.execute(
            """
            SELECT DISTINCT r.node_id, r.occ_id
            FROM _reach r JOIN occ o
              ON o.node_id = r.node_id AND o.occ_id = r.occ_id
            WHERE o.padded
            ORDER BY r.node_id, r.occ_id
            """
        ).fetchall()
        return [
            {
                "node_id": nid,
                "occ_id": occ,
                "note": "left join had no passing match; right columns padded with NULL, "
                        "left contribution retained (not a loss)",
            }
            for nid, occ in rows
        ]

    def _node_hits(self) -> Dict[str, Any]:
        rows = self.con.execute(
            """
            SELECT node_id, count(*) AS hits, min(depth) AS first_depth
            FROM _reach GROUP BY node_id
            ORDER BY first_depth, node_id
            """
        ).fetchall()
        return {nid: {"occurrences_touched": hits, "first_depth": depth}
                for nid, hits, depth in rows}

    def _ancestors(self, node_id: str) -> set:
        parents = F._parents(self.flow["nodes"])
        out, stack = set(), [node_id]
        while stack:
            cur = stack.pop()
            if cur in out:
                continue
            out.add(cur)
            stack.extend(parents[cur])
        return out

    def _merge_contributions(self, scan_node: str, scan_occ: int) -> List[Dict[str, Any]]:
        rows = self.con.execute(
            """
            WITH RECURSIVE reach(node_id, occ_id) AS (
                SELECT ?, ?
                UNION ALL
                SELECT e.to_node, e.to_occ
                FROM reach r JOIN edges e
                  ON e.from_node = r.node_id AND e.from_occ = r.occ_id
            ), dedup AS (
                SELECT DISTINCT node_id, occ_id FROM reach
            )
            SELECT d.node_id, d.occ_id,
                   (SELECT count(*) FROM edges e2
                     WHERE e2.to_node = d.node_id AND e2.to_occ = d.occ_id
                       AND e2.role IN ('merge','group_member')) AS contributors
            FROM dedup d
            WHERE EXISTS (
                SELECT 1 FROM edges e
                WHERE e.to_node = d.node_id AND e.to_occ = d.occ_id
                  AND e.role IN ('merge','group_member'))
            ORDER BY d.node_id, d.occ_id
            """,
            [scan_node, scan_occ],
        ).fetchall()
        out = []
        for nid, occ, contributors in rows:
            out.append({
                "node_id": nid,
                "output_occ": occ,
                "output_row": self._occ_data(nid, occ),
                "total_contributing_input_occurrences": contributors,
                "note": "row identity merged into a shared output; still a contributor",
            })
        return out

    def _branch_fate(self, sink_ids: List[str], blockers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for s in sink_ids:
            row = self.con.execute(
                "SELECT count(*) FROM _reach WHERE node_id=?", [s]
            ).fetchone()[0]
            sink_ancestors = self._ancestors(s)
            # a blocker sits on a path to this sink iff its node is an ancestor
            # of the sink and the blocked occurrence itself is reachable
            relevant = []
            for b in blockers:
                if b["blocked_at_node"] not in sink_ancestors:
                    continue
                on_path = self.con.execute(
                    "SELECT 1 FROM _reach WHERE node_id=? AND occ_id=? LIMIT 1",
                    [b["input_node"], b["input_occ"]],
                ).fetchone()
                if on_path:
                    relevant.append(b["reason_code"])
            out.append({
                "sink": s,
                "status": "contributes" if row else "blocked_before_sink",
                "output_occurrences": [
                    o for (_n, o) in self.con.execute(
                        "SELECT node_id, occ_id FROM _reach WHERE node_id=? ORDER BY occ_id",
                        [s],
                    ).fetchall()
                ],
                "first_blocker_reasons_on_its_paths": sorted(set(relevant)),
            })
        return out

    @staticmethod
    def _verdict(total_outputs: int, blockers: List[Dict[str, Any]]) -> str:
        if total_outputs and blockers:
            return "partially_contributing_some_branches_blocked"
        if total_outputs:
            return "contributing"
        return "eliminated_first_blocker_reported"

    # ------------------------------------------------------------------ #
    # reverse trace
    # ------------------------------------------------------------------ #
    def trace_output(
        self, sink: str, occ_id: int, page_size: int = 50, page: int = 0
    ) -> Dict[str, Any]:
        if sink not in self.nodes:
            raise PackageError(f"unknown sink/node {sink!r}")
        exists = self.con.execute(
            "SELECT 1 FROM occ WHERE node_id=? AND occ_id=?", [sink, occ_id]
        ).fetchone()
        if not exists:
            raise PackageError(f"node {sink!r} has no occurrence {occ_id}")

        # occurrence-set semantics: an occurrence reached through several edge
        # paths (diamond DAG / fan-out) is counted once; DISTINCT collapses paths.
        total = self.con.execute(
            """
            WITH RECURSIVE up(node_id, occ_id, depth) AS (
                SELECT ?, ?, 0
                UNION ALL
                SELECT e.from_node, e.from_occ, u.depth + 1
                FROM up u JOIN edges e
                  ON e.to_node = u.node_id AND e.to_occ = u.occ_id
            )
            SELECT count(*) FROM (
                SELECT DISTINCT node_id, occ_id FROM up
            )
            """,
            [sink, occ_id],
        ).fetchone()[0]

        rows = self.con.execute(
            """
            WITH RECURSIVE up(node_id, occ_id, depth) AS (
                SELECT ?, ?, 0
                UNION ALL
                SELECT e.from_node, e.from_occ, u.depth + 1
                FROM up u JOIN edges e
                  ON e.to_node = u.node_id AND e.to_occ = u.occ_id
            ), dedup AS (
                SELECT node_id, occ_id, min(depth) AS depth FROM up
                GROUP BY node_id, occ_id
            )
            SELECT d.node_id, d.occ_id, d.depth, o.data_json, o.padded
            FROM dedup d JOIN occ o
              ON o.node_id = d.node_id AND o.occ_id = d.occ_id
            ORDER BY d.depth, d.node_id, d.occ_id
            LIMIT ? OFFSET ?
            """,
            [sink, occ_id, page_size, page * page_size],
        ).fetchall()
        lineage = [
            {
                "node_id": nid,
                "occ_id": oid,
                "depth": depth,
                "row": json.loads(dj),
                "left_join_padded": bool(padded),
            }
            for nid, oid, depth, dj, padded in rows
        ]
        scans = [x for x in lineage if self.nodes[x["node_id"]]["op"] == "scan"]
        return {
            "sink": sink,
            "occ_id": occ_id,
            "total_upstream_occurrences": total,
            "page": page,
            "page_size": page_size,
            "lineage_page": lineage,
            "source_rows_on_this_page": [
                {"scan_node": x["node_id"],
                 "row_id": x["row"].get(F.ROW_ID_COL)}
                for x in scans
            ],
        }

    # ------------------------------------------------------------------ #
    # node balance
    # ------------------------------------------------------------------ #
    def node_balance(self) -> List[Dict[str, Any]]:
        """Real occurrence accounting per node — never a generic in=out+drop."""
        stats = self.con.execute(
            "SELECT node_id, op, input_occ, output_occ, rejected, expanded, merged "
            "FROM node_stats ORDER BY node_id"
        ).fetchall()
        out = []
        for nid, op, inp, outp, rej, exp, merged in stats:
            entry: Dict[str, Any] = {
                "node_id": nid,
                "op": op,
                "input_occurrences": inp,
                "output_occurrences": outp,
                "rejected_occurrences": rej,
                "expansion_occurrences": exp,
                "merge_occurrences": merged,
            }
            entry["relations"] = self._balance_relations(
                op, inp, outp, rej, exp, merged, nid
            )
            out.append(entry)
        return out

    def _balance_relations(
        self, op: str, inp: int, outp: int, rej: int, exp: int, merged: int,
        nid: str,
    ) -> List[str]:
        rels: List[str] = []
        edge_in, edge_out = self._edge_counts(nid)
        if op == "scan":
            rels.append(f"output={outp} (each scan row_id is one distinct occurrence)")
        elif op == "filter":
            rels.append(f"input={inp} = pass {outp} + rejects {rej} "
                        f"(rejects split into filter_false / filter_unknown)")
        elif op == "project":
            rels.append("1:1 occurrence-preserving; dropping __row_id column does "
                        "not drop audit identity")
            rels.append(f"input={inp} = output={outp}")
        elif op == "union_all":
            rels.append(f"sum of branch inputs={inp} = output occurrences={outp}; "
                        "identical rows stay separate occurrences")
        elif op == "join":
            rels.append(
                f"left+right inputs={inp}; output={outp}; rejected left rows={rej}; "
                f"fan-out match occurrences={exp}"
            )
            rels.append(
                "inner: output = passing left×right matches; left: unmatched left "
                "rows survive NULL-padded; anti: output = left rows without a witness"
            )
        elif op == "group":
            rels.append(
                f"member occurrences={inp} collapsed into groups={outp} "
                f"(merge edges={merged}); zero rows rejected, even when sum=0"
            )
            rels.append("count/sum participation is in participation table, not "
                        "inferred from the aggregate value")
        elif op == "distinct":
            rels.append(
                f"input occurrences={inp} collapsed into distinct rows={outp}; "
                f"merge edges={merged}; every input remains a contributor"
            )
        elif op == "top_k":
            rels.append(f"input={inp} = kept {outp} + cutoff rejects {rej}; "
                        "reject detail carries the full ordering and cutoff")
        rels.append(f"evidence check: edges_in={edge_in}, edges_out={edge_out}")
        return rels

    def _edge_counts(self, nid: str) -> tuple:
        edge_in = self.con.execute(
            "SELECT count(*) FROM edges WHERE to_node=?", [nid]
        ).fetchone()[0]
        edge_out = self.con.execute(
            "SELECT count(*) FROM edges WHERE from_node=?", [nid]
        ).fetchone()[0]
        return edge_in, edge_out

    # ------------------------------------------------------------------ #
    def aggregate_participation(self, group_node: str, group_occ: int) -> Dict[str, Any]:
        """Explicit count/sum membership evidence for one aggregate output row."""
        rows = self.con.execute(
            """
            SELECT member_node, member_occ, agg_name, in_count, in_sum, contribution
            FROM participation
            WHERE node_id=? AND group_occ=?
            ORDER BY agg_name, member_node, member_occ
            """,
            [group_node, group_occ],
        ).fetchall()
        out: Dict[str, Any] = {"group_node": group_node, "group_occ": group_occ,
                               "aggregates": {}}
        for mn, mo, agg, ic, is_, contrib in rows:
            bucket = out["aggregates"].setdefault(agg, {"members": []})
            bucket["members"].append({
                "member_node": mn,
                "member_occ": mo,
                "in_count": bool(ic),
                "in_sum": bool(is_),
                "sum_contribution": contrib,
            })
        return out
