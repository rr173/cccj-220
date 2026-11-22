"""Preview the effect of a batch of lookup corrections on a top_k sink list.

A *preview* is a read-only rehearsal over a completed audit package: given one
inline lookup source and a batch of proposed corrections (delete a row_id, or
keep the row_id and replace the whole row, join key included), it reports how
the published list of one `top_k` sink would change — per partition, which
complete sink rows would enter (`added`) and which would leave (`removed`),
as occurrence-count differences.

A preview
  * never modifies the package (the evidence DB is opened read-only),
  * never writes a second audit package,
  * never commits the corrections,
  * never re-parses flow.json (it embeds full source rows) — the row-less
    preview.json structure written by `run` is used instead,
  * never scans whole intermediate tables — every read is an indexed point
    lookup by row_id / equi-key / partition key, plus a bounded edge walk
    over the affected occurrences only.

Supported shape (anything else is refused with an explicit reason, never a
silent full re-run): the corrected source appears in the sink's upstream only
as the direct right-side scan of `left`/`anti` joins whose left inputs do not
depend on the source; between those joins and the sink there are only
filter / project / union_all nodes (branches may re-merge) and the final
top_k. Nodes outside the sink's ancestor region are unrestricted.
"""
from __future__ import annotations

import json
import os
from collections import Counter, deque
from typing import Any, Dict, List, Optional, Set, Tuple

from . import expr as E
from . import flow as F
from .executor import Executor
from .package import AuditPackage
from .preview_support import (
    PREVIEW_TABLES,
    PREVIEW_VERSION,
    jkey,
    key_json,
    part_json,
    public_part_key,
)

STATUS_REJECTED = "rejected"        # the correction batch itself is invalid
STATUS_UNSUPPORTED = "unsupported"  # this package / path cannot be previewed


class PreviewError(ValueError):
    """Refusal of a preview; `status` tells rejected vs unsupported apart."""

    def __init__(self, status: str, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def _unsupported(msg: str) -> PreviewError:
    return PreviewError(STATUS_UNSUPPORTED, msg)


def _rejected(msg: str) -> PreviewError:
    return PreviewError(STATUS_REJECTED, msg)


def _canon(row: Dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _bag(rows: List[Dict[str, Any]]) -> Counter:
    return Counter(_canon(r) for r in rows)


def _bag_diff(a: List[Dict[str, Any]], b: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], int]]:
    """Multiset a - b as (row, count) pairs, deterministically ordered."""
    ca, cb = _bag(a), _bag(b)
    out = []
    for k in ca:
        d = ca[k] - cb.get(k, 0)
        if d > 0:
            out.append((json.loads(k), d))
    out.sort(key=lambda t: _canon(t[0]))
    return out


class PreviewEngine:
    def __init__(self, package_path: str):
        self.pkg = AuditPackage(package_path)
        self.pkg.require_completed()
        if not os.path.exists(self.pkg.preview_path):
            raise _unsupported(
                f"package {self.pkg.path} carries no preview support (it was "
                f"built by an auditor without preview, or the preview files "
                f"were removed); build a fresh package with `run` to enable "
                f"preview — preview never falls back to a full re-run"
            )
        with open(self.pkg.preview_path, encoding="utf-8") as f:
            meta = json.load(f)
        if meta.get("preview_version") != PREVIEW_VERSION:
            raise _unsupported(
                f"package preview format version {meta.get('preview_version')!r} "
                f"is not supported by this auditor (expects {PREVIEW_VERSION}); "
                f"rebuild the package with `run`"
            )
        self.structure = meta["structure"]
        self.con = self.pkg.connect(read_only=True)
        try:
            tables = {r[0] for r in self._raw_q(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'")}
        except Exception:
            self.con.close()
            raise
        missing = [t for t in PREVIEW_TABLES if t not in tables]
        if missing:
            self.con.close()
            raise _unsupported(
                f"package is missing preview index tables {missing}; it cannot "
                f"be previewed (no silent full re-run) — rebuild it with `run`"
            )
        self.nodes = {n["id"]: n for n in self.structure["nodes"]}
        self.sources = {s["name"]: s for s in self.structure["sources"]}
        self.parents = F._parents(self.structure["nodes"])
        self.children: Dict[str, List[str]] = {nid: [] for nid in self.nodes}
        for nid, ps in self.parents.items():
            for p in ps:
                self.children[p].append(nid)
        # read/execution observability: every read goes through _q
        self.queries = 0
        self.rows_read = 0
        self.sql_log: Optional[List[Dict[str, Any]]] = None

    def close(self) -> None:
        self.con.close()

    # ------------------------------------------------------------------ #
    # counted reads — the only database access in the whole preview
    # ------------------------------------------------------------------ #
    def _raw_q(self, sql: str, params: Optional[List[Any]] = None):
        return self.con.execute(sql, params or []).fetchall()

    def _q(self, sql: str, params: Optional[List[Any]] = None) -> List[tuple]:
        self.queries += 1
        rows = self._raw_q(sql, params)
        self.rows_read += len(rows)
        if self.sql_log is not None:
            self.sql_log.append(
                {"sql": sql, "params": list(params or []), "rows": len(rows)})
        return rows

    def _q_in(self, template: str, keys: List[str], chunk: int = 500) -> List[tuple]:
        """Run `template` (containing {ph}) as indexed IN-list point lookups."""
        out: List[tuple] = []
        for i in range(0, len(keys), chunk):
            part = keys[i:i + chunk]
            ph = ",".join("?" * len(part))
            out.extend(self._q(template.format(ph=ph), part))
        return out

    def _fetch_occ(self, refs: List[Tuple[str, int]]) -> Dict[Tuple[str, int], Dict[str, Any]]:
        """Point-fetch occurrence rows by (node_id, occ_id)."""
        if not refs:
            return {}
        keys = [jkey(n, o) for n, o in refs]
        out: Dict[Tuple[str, int], Dict[str, Any]] = {}
        for _k, nid, oid, dj in self._q_in(
            "SELECT k, node_id, occ_id, data_json FROM preview_occ WHERE k IN ({ph})",
            keys,
        ):
            out[(nid, oid)] = json.loads(dj)
        return out

    # ------------------------------------------------------------------ #
    # public entry
    # ------------------------------------------------------------------ #
    def run(
        self,
        sink_id: str,
        source_name: str,
        corrections: Any,
    ) -> Dict[str, Any]:
        sink_node, src = self._check_path(sink_id, source_name)
        ops = self._parse_corrections(corrections, src)
        affected_joins = self._affected_joins(sink_id, source_name)

        # fetch the current rows of the corrected row_ids (indexed point reads)
        old_rows: Dict[int, Dict[str, Any]] = {}
        if ops:
            scan0 = self._source_scans[0]
            keys = [jkey(scan0, rid) for _op, rid, _v in ops]
            got = self._q_in(
                "SELECT k, scan_node, row_id, data_json FROM preview_source_rows "
                "WHERE k IN ({ph})",
                keys,
            )
            for _k, _sn, rid, dj in got:
                old_rows[rid] = json.loads(dj)
            for _op, rid, _v in ops:
                if rid not in old_rows:
                    raise _rejected(
                        f"row_id {rid} does not exist in source {source_name!r}; "
                        f"corrections only delete or replace existing rows"
                    )

        # per-join output deltas
        removed_outputs: List[Tuple[str, int]] = []       # (join, occ)
        added_outputs: List[Tuple[str, Dict[str, Any]]] = []  # (join, row)
        for j in affected_joins:
            rem, add = self._join_deltas(j, ops, old_rows)
            removed_outputs.extend(rem)
            added_outputs.extend(add)

        topk_input = sink_node["input"]

        # removed candidates at the top_k input: bounded forward walk over the
        # evidence edges of the removed join outputs, inside the sink's region
        removed_cand_ids: Set[int] = set()
        if removed_outputs:
            removed_cand_ids = self._forward_reach(
                [(j, o) for j, o in removed_outputs], topk_input)
        removed_cand_rows: List[Dict[str, Any]] = []
        if removed_cand_ids:
            occ_rows = self._fetch_occ(
                [(topk_input, o) for o in sorted(removed_cand_ids)])
            removed_cand_rows = [row for _key, row in sorted(occ_rows.items())]

        # added candidates at the top_k input: local propagation of the new
        # join outputs through filter/project/union_all (occurrences kept)
        added_cand_rows = self._propagate(added_outputs, topk_input, sink_id)

        # affected partitions = partitions touched by removals or additions
        part_cols = sink_node.get("partition_by", [])

        def pkey(row: Dict[str, Any]) -> tuple:
            return tuple(row.get(p) for p in part_cols)

        affected_parts: Set[tuple] = {pkey(r) for r in removed_cand_rows}
        added_by_part: Dict[tuple, List[Dict[str, Any]]] = {}
        for r in added_cand_rows:
            pk = pkey(r)
            affected_parts.add(pk)
            added_by_part.setdefault(pk, []).append(r)

        terms = sink_node["order_by"]
        k = sink_node["k"]
        partitions_out = []
        for pk in sorted(affected_parts, key=lambda t: part_json(t)):
            pj = part_json(pk)
            cand_ids = [
                oid for (_k, oid) in self._q(
                    "SELECT k, occ_id FROM preview_topk_parts WHERE k = ?",
                    [jkey(sink_id, pj)],
                )
            ]
            cand = self._fetch_occ([(topk_input, o) for o in cand_ids])
            old_rows_p = [row for (_n, _o), row in sorted(cand.items())]
            new_rows_p = [row for (n, o), row in sorted(cand.items())
                          if o not in removed_cand_ids]
            new_rows_p.extend(added_by_part.get(pk, []))
            self._check_decisive(new_rows_p, terms, pk)
            old_top = self._rank(old_rows_p, terms)[:k]
            new_top = self._rank(new_rows_p, terms)[:k]
            added = _bag_diff(new_top, old_top)
            removed = _bag_diff(old_top, new_top)
            if added or removed:
                partitions_out.append({
                    "partition": public_part_key(pk),
                    "added": [{"row": r, "count": c} for r, c in added],
                    "removed": [{"row": r, "count": c} for r, c in removed],
                })

        return {
            "status": "ok",
            "mode": "preview",
            "package": self.pkg.path,
            "sink": sink_id,
            "source": source_name,
            "corrections": {
                "total": len(ops),
                "deletes": sum(1 for o in ops if o[0] == "delete"),
                "replaces": sum(1 for o in ops if o[0] == "replace"),
            },
            "affected_joins": affected_joins,
            "partitions_examined": len(affected_parts),
            "partitions": partitions_out,
            "note": "preview only: the audit package was not modified, no "
                    "second package was written, corrections were not committed",
            "read_stats": {"queries": self.queries, "rows_read": self.rows_read},
        }

    # ------------------------------------------------------------------ #
    # path / support validation (no correction data involved yet)
    # ------------------------------------------------------------------ #
    def _check_path(self, sink_id: str, source_name: str):
        nodes, sources = self.nodes, self.sources
        if sink_id not in nodes:
            raise _unsupported(f"unknown sink {sink_id!r}: no such node in this flow")
        if sink_id not in self.structure["sinks"]:
            raise _unsupported(
                f"{sink_id!r} is a node but not a declared sink of this flow")
        sink_node = nodes[sink_id]
        if sink_node["op"] != "top_k":
            raise _unsupported(
                f"sink {sink_id!r} ends with {sink_node['op']!r}, not 'top_k'; "
                f"preview only supports sinks ending with top_k")
        if source_name not in sources:
            raise _unsupported(f"unknown source {source_name!r}")
        src = sources[source_name]
        if src.get("type", "inline") != "inline":
            raise _unsupported(
                f"source {source_name!r} is a generated source; corrections by "
                f"row_id are only supported for inline sources")

        self.region = self._ancestors_incl(sink_id)
        self._source_scans = sorted(
            nid for nid in self.region
            if nodes[nid]["op"] == "scan" and nodes[nid]["source"] == source_name
        )
        if not self._source_scans:
            raise _unsupported(
                f"source {source_name!r} does not contribute to sink {sink_id!r}; "
                f"there is nothing to preview")
        return sink_node, src

    def _ancestors_incl(self, nid: str) -> Set[str]:
        out: Set[str] = set()
        stack = [nid]
        while stack:
            cur = stack.pop()
            if cur in out:
                continue
            out.add(cur)
            stack.extend(self.parents[cur])
        return out

    def _affected_joins(self, sink_id: str, source_name: str) -> List[str]:
        """Joins in the sink's region whose right side scans the source.

        Also validates every supported-shape constraint; any violation is an
        explicit 'unsupported' refusal with the offending node named.
        """
        nodes = self.nodes
        affected: List[str] = []
        for s in self._source_scans:
            for c in self.children.get(s, []):
                if c not in self.region:
                    continue  # usages outside the sink's upstream are unrestricted
                cn = nodes[c]
                if (cn["op"] == "join" and cn.get("right") == s
                        and cn.get("kind") in ("left", "anti")):
                    affected.append(c)
                else:
                    raise _unsupported(
                        f"scan {s!r} of source {source_name!r} feeds node {c!r} "
                        f"({cn['op']}) inside the upstream of sink {sink_id!r}; "
                        f"preview only supports the source as the direct "
                        f"right-side scan of left/anti joins")
        for j in sorted(affected):
            jn = nodes[j]
            left_anc = self._ancestors_incl(jn["left"])
            bad = [s for s in self._source_scans if s in left_anc]
            if bad:
                raise _unsupported(
                    f"join {j!r}'s left input depends on the corrected source "
                    f"via scan {bad[0]!r}; preview requires left inputs that "
                    f"are independent of the source")
            cone = self._descendants_in_region(j) - {j}
            for c in sorted(cone):
                if c == sink_id:
                    continue
                if nodes[c]["op"] not in ("filter", "project", "union_all"):
                    raise _unsupported(
                        f"node {c!r} ({nodes[c]['op']}) sits between affected "
                        f"join {j!r} and sink {sink_id!r}; only filter, project "
                        f"and union_all are supported on that path")
        return sorted(affected)

    def _descendants_in_region(self, nid: str) -> Set[str]:
        out: Set[str] = set()
        stack = [nid]
        while stack:
            cur = stack.pop()
            for c in self.children.get(cur, []):
                if c in self.region and c not in out:
                    out.add(c)
                    stack.append(c)
        return out

    # ------------------------------------------------------------------ #
    # corrections validation
    # ------------------------------------------------------------------ #
    def _parse_corrections(self, corrections: Any, src: Dict[str, Any]):
        if not isinstance(corrections, list):
            raise _rejected("corrections must be a JSON array of operations")
        schema = src["schema"]
        ops: List[Tuple[str, int, Optional[Dict[str, Any]]]] = []
        seen: Set[int] = set()
        for i, c in enumerate(corrections):
            where = f"corrections[{i}]"
            if not isinstance(c, dict):
                raise _rejected(f"{where}: each correction must be an object")
            op = c.get("op")
            rid = c.get("row_id")
            if isinstance(rid, bool) or not isinstance(rid, int):
                raise _rejected(f"{where}: row_id must be int64, got {rid!r}")
            if rid in seen:
                raise _rejected(
                    f"{where}: duplicate operation on row_id {rid}; one batch "
                    f"allows at most one operation per row_id")
            seen.add(rid)
            if op == "delete":
                ops.append(("delete", rid, None))
            elif op == "replace":
                vals = c.get("values")
                if not isinstance(vals, dict):
                    raise _rejected(f"{where}: replace needs a 'values' object")
                if set(vals) != set(schema):
                    raise _rejected(
                        f"{where}: replacement columns {sorted(vals)} do not "
                        f"match source schema {sorted(schema)}")
                for col, v in vals.items():
                    t = schema[col]
                    if v is None:
                        continue
                    if t == "int64" and (isinstance(v, bool) or not isinstance(v, int)):
                        raise _rejected(
                            f"{where}: column {col!r} value {v!r} is not int64")
                    if t == "text" and not isinstance(v, str):
                        raise _rejected(
                            f"{where}: column {col!r} value {v!r} is not text")
                    if t == "bool" and not isinstance(v, bool):
                        raise _rejected(
                            f"{where}: column {col!r} value {v!r} is not bool")
                ops.append(("replace", rid, dict(vals)))
            else:
                raise _rejected(
                    f"{where}: op must be 'delete' or 'replace', got {op!r}")
        return ops

    # ------------------------------------------------------------------ #
    # join deltas
    # ------------------------------------------------------------------ #
    def _join_deltas(
        self,
        join_id: str,
        ops: List[Tuple[str, int, Optional[Dict[str, Any]]]],
        old_rows: Dict[int, Dict[str, Any]],
    ) -> Tuple[List[Tuple[str, int]], List[Tuple[str, Dict[str, Any]]]]:
        jn = self.nodes[join_id]
        kind = jn["kind"]
        on = jn["on"]
        prefix = jn.get("right_prefix", "r_")
        residual = jn.get("residual")
        right_scan = jn["right"]
        left_node = jn["left"]
        right_src = self.sources[self.nodes[right_scan]["source"]]
        right_schema = {F.ROW_ID_COL: "int64", **right_src["schema"]}

        def key_of(row: Dict[str, Any]) -> tuple:
            return tuple(row.get(p["right"]) for p in on)

        corrected_ids = {rid for _op, rid, _v in ops}
        repl_by_key: Dict[tuple, List[Dict[str, Any]]] = {}
        affected: Set[tuple] = set()
        for op, rid, vals in ops:
            old_key = key_of(old_rows[rid])
            if not any(v is None for v in old_key):
                affected.add(old_key)
            if op == "replace":
                new_row = {F.ROW_ID_COL: rid, **vals}
                new_key = key_of(new_row)
                if not any(v is None for v in new_key):
                    affected.add(new_key)
                    repl_by_key.setdefault(new_key, []).append(new_row)
        if not affected:
            return [], []

        # current right rows at the affected keys (indexed point reads)
        kj_of = {k: key_json(k) for k in affected}
        right_refs: Dict[tuple, List[int]] = {k: [] for k in affected}
        for keyj, oid in self._q_in_kv(
            join_id, "right", [kj_of[k] for k in sorted(affected, key=repr)]
        ):
            right_refs.setdefault(_decode_key_json(keyj), []).append(oid)
        right_data = self._fetch_occ(
            [(right_scan, o) for k in affected for o in right_refs.get(k, [])])

        left_refs: Dict[tuple, List[int]] = {k: [] for k in affected}
        for keyj, oid in self._q_in_kv(
            join_id, "left", [kj_of[k] for k in sorted(affected, key=repr)]
        ):
            left_refs.setdefault(_decode_key_json(keyj), []).append(oid)
        left_data = self._fetch_occ(
            [(left_node, o) for k in affected for o in left_refs.get(k, [])])

        removed: List[Tuple[str, int]] = []
        added: List[Tuple[str, Dict[str, Any]]] = []
        for key in sorted(affected, key=repr):
            olds = [right_data[(right_scan, o)] for o in right_refs.get(key, [])]
            news = ([r for r in olds if r[F.ROW_ID_COL] not in corrected_ids]
                    + repl_by_key.get(key, []))
            for l_occ in left_refs.get(key, []):
                l_row = left_data[(left_node, l_occ)]
                old_m = [r for r in olds
                         if self._residual_true(residual, l_row, r, right_schema, prefix)]
                new_m = [r for r in news
                         if self._residual_true(residual, l_row, r, right_schema, prefix)]
                if kind == "left":
                    old_out = ([self._combine(l_row, r, right_schema, prefix, False)
                                for r in old_m]
                               or [self._combine(l_row, None, right_schema, prefix, True)])
                    new_out = ([self._combine(l_row, r, right_schema, prefix, False)
                                for r in new_m]
                               or [self._combine(l_row, None, right_schema, prefix, True)])
                else:  # anti: released only when no valid match remains
                    old_out = [] if old_m else [dict(l_row)]
                    new_out = [] if new_m else [dict(l_row)]
                if _bag(old_out) == _bag(new_out):
                    continue
                # evidence outputs of this left occurrence at the join, so the
                # removed rows can be tied to real occurrence ids for the walk
                ev = self._q(
                    "SELECT to_node, to_occ FROM preview_edges WHERE k = ?",
                    [jkey(left_node, l_occ)],
                )
                out_occs = [to for tn, to in ev if tn == join_id]
                ev_rows = self._fetch_occ([(join_id, o) for o in out_occs])
                if _bag(list(ev_rows.values())) != _bag(old_out):
                    raise _unsupported(
                        f"preview indexes are inconsistent with the stored join "
                        f"evidence at join {join_id!r}; rebuild the package")
                pool = [(oid, row) for (nid, oid), row in ev_rows.items()]
                for row, cnt in _bag_diff(old_out, new_out):
                    for _ in range(cnt):
                        for idx, (oid, erow) in enumerate(pool):
                            if _canon(erow) == _canon(row):
                                removed.append((join_id, oid))
                                pool.pop(idx)
                                break
                        else:  # pragma: no cover - guarded by the bag check above
                            raise _unsupported(
                                f"cannot map a removed join output at {join_id!r} "
                                f"to its occurrence; rebuild the package")
                for row, cnt in _bag_diff(new_out, old_out):
                    added.extend([(join_id, row)] * cnt)
        return removed, added

    def _q_in_kv(self, join_id: str, side: str, key_jsons: List[str]) -> List[Tuple[str, int]]:
        if not key_jsons:
            return []
        keys = [jkey(join_id, side, kj) for kj in key_jsons]
        return [
            (kj, oid)
            for _k, kj, oid in self._q_in(
                "SELECT k, key_json, occ_id FROM preview_join_keys WHERE k IN ({ph})",
                keys,
            )
        ]

    @staticmethod
    def _residual_true(
        residual: Optional[Dict[str, Any]],
        l_row: Dict[str, Any],
        r_row: Dict[str, Any],
        right_schema: Dict[str, str],
        prefix: str,
    ) -> bool:
        if residual is None:
            return True
        combined = dict(l_row)
        for c in right_schema:
            combined[prefix + c] = r_row[c]
        return E.eval_predicate(residual, combined) is True

    @staticmethod
    def _combine(
        l_row: Dict[str, Any],
        r_row: Optional[Dict[str, Any]],
        right_schema: Dict[str, str],
        prefix: str,
        padded: bool,
    ) -> Dict[str, Any]:
        out = dict(l_row)
        for c in right_schema:
            out[prefix + c] = None if padded else r_row[c]
        return out

    # ------------------------------------------------------------------ #
    # from join outputs to top_k candidates
    # ------------------------------------------------------------------ #
    def _forward_reach(self, seeds: List[Tuple[str, int]], stop_node: str) -> Set[int]:
        """Occ ids at `stop_node` reachable from seeds inside the region."""
        found: Set[int] = set()
        visited = set(seeds)
        frontier = list(seeds)
        while frontier:
            expand = []
            for nid, oid in frontier:
                if nid == stop_node:
                    found.add(oid)
                else:
                    expand.append((nid, oid))
            if not expand:
                break
            keys = [jkey(n, o) for n, o in expand]
            nxt: List[Tuple[str, int]] = []
            for _k, _fn, _fo, tn, to in self._q_in(
                "SELECT k, from_node, from_occ, to_node, to_occ FROM preview_edges "
                "WHERE k IN ({ph})",
                keys,
            ):
                if tn not in self.region or (tn, to) in visited:
                    continue
                visited.add((tn, to))
                nxt.append((tn, to))
            frontier = nxt
        return found

    def _propagate(
        self,
        added_outputs: List[Tuple[str, Dict[str, Any]]],
        topk_input: str,
        sink_id: str,
    ) -> List[Dict[str, Any]]:
        """Push new join outputs through filter/project/union_all to the top_k
        input. Occurrences are kept as a multiset: identical rows arriving via
        re-merged branches stay separate occurrences."""
        out: List[Dict[str, Any]] = []
        work: deque = deque(added_outputs)
        while work:
            nid, row = work.popleft()
            if nid == topk_input:
                out.append(row)
                continue
            for c in self.children.get(nid, []):
                if c not in self.region or c == sink_id:
                    continue
                cn = self.nodes[c]
                op = cn["op"]
                if op == "filter":
                    if E.eval_predicate(cn["pred"], row) is True:
                        work.append((c, row))
                elif op == "project":
                    out_row = {}
                    for col in cn["columns"]:
                        out_row[col["name"]] = (
                            row[col["keep"]] if "keep" in col
                            else E.eval_expr(col["expr"], row)
                        )
                    work.append((c, out_row))
                elif op == "union_all":
                    work.append((c, row))
                else:  # pragma: no cover - the cone check rejects these earlier
                    raise _unsupported(
                        f"node {c!r} ({op}) cannot be propagated through; only "
                        f"filter, project and union_all are supported")
        return out

    # ------------------------------------------------------------------ #
    # ranking (exact executor semantics)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _rank(rows: List[Dict[str, Any]], terms: List[Dict[str, Any]]):
        return sorted(rows, key=lambda r: Executor._sort_key(r, terms))

    @staticmethod
    def _check_decisive(rows: List[Dict[str, Any]], terms: List[Dict[str, Any]], pk: tuple):
        decisive = [t for t in terms if t.get("decisive")]
        part_desc = json.dumps(public_part_key(pk), ensure_ascii=False)
        seen: Set[str] = set()
        for row in rows:
            vals = [Executor._order_value(row, t) for t in decisive]
            if any(isinstance(v, dict) and v.get("__null__") for v in vals):
                raise _rejected(
                    f"corrections would leave a NULL decisive tie-break value "
                    f"in partition {part_desc}; decisive tuples must be "
                    f"non-empty and unique — the whole batch is rejected")
            dk = json.dumps(vals, ensure_ascii=False, sort_keys=True,
                            separators=(",", ":"))
            if dk in seen:
                raise _rejected(
                    f"corrections would make decisive tie-break tuple {dk} "
                    f"collide for two candidates in partition {part_desc} "
                    f"(even outside the kept top-k); the whole batch is rejected")
            seen.add(dk)


def _decode_key_json(keyj: str) -> tuple:
    return tuple(json.loads(keyj))


def preview(
    package_path: str,
    sink: str,
    source: str,
    corrections: Any,
    sql_log: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """One-call preview entry (see module docstring for the contract)."""
    eng = PreviewEngine(package_path)
    try:
        eng.sql_log = sql_log
        return eng.run(sink, source, corrections)
    finally:
        eng.close()
