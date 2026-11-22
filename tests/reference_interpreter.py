"""Independent reference interpreter for acceptance testing.

This is a deliberately small, self-contained re-implementation of the flow
semantics. It does NOT import the production lineage_auditor package and does
NOT touch any evidence builder: it recomputes both the business bag result of
every node and the expected audit edges/rejects from scratch, with explicit
occurrence ids, so the production evidence can be checked against it.

The only shared contract is the declarative flow JSON format.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


# --------------------------------------------------------------------------- #
# 3VL expression semantics (independent copy of the spec, not of expr.py)
# --------------------------------------------------------------------------- #
def eval_expr(expr: Dict[str, Any], row: Dict[str, Any]) -> Any:
    k = expr["kind"]
    if k == "col":
        return row.get(expr["name"])
    if k == "lit":
        return expr["value"]
    if k == "cmp":
        a, b = eval_expr(expr["left"], row), eval_expr(expr["right"], row)
        if a is None or b is None:
            return None
        op = expr["op"]
        return {"=": a == b, "!=": a != b, "<": a < b, "<=": a <= b,
                ">": a > b, ">=": a >= b}[op]
    if k == "and":
        saw_unknown = False
        for sub in expr["args"]:
            t = eval_bool(sub, row)
            if t is False:
                return False
            if t is None:
                saw_unknown = True
        return None if saw_unknown else True
    if k == "or":
        saw_unknown = False
        for sub in expr["args"]:
            t = eval_bool(sub, row)
            if t is True:
                return True
            if t is None:
                saw_unknown = True
        return None if saw_unknown else False
    if k == "not":
        t = eval_bool(expr["arg"], row)
        return None if t is None else not t
    if k == "is_null":
        return eval_expr(expr["arg"], row) is None
    if k == "is_not_null":
        return eval_expr(expr["arg"], row) is not None
    raise AssertionError(f"bad expr kind {k}")


def eval_bool(expr: Dict[str, Any], row: Dict[str, Any]) -> Optional[bool]:
    v = eval_expr(expr, row)
    assert v is None or isinstance(v, bool)
    return v


# --------------------------------------------------------------------------- #
# occurrences
# --------------------------------------------------------------------------- #
@dataclass
class Occ:
    occ_id: int
    data: Dict[str, Any]
    padded: bool = False
    # parents: list of (node_id, occ_id, role)
    parents: List[Tuple[str, int, str]] = field(default_factory=list)


@dataclass
class Reject:
    node_id: str
    input_node: str
    input_occ: int
    reason: str
    detail: Dict[str, Any]


@dataclass
class Part:
    node_id: str
    group_occ: int
    member_node: str
    member_occ: int
    agg_name: str
    in_count: bool
    in_sum: bool
    contribution: Optional[int]


class ReferenceInterpreter:
    def __init__(self, flow: Dict[str, Any]):
        self.flow = flow
        self.sources = {s["name"]: s for s in flow["sources"]}
        self.nodes = {n["id"]: n for n in flow["nodes"]}
        self.occurrences: Dict[str, List[Occ]] = {}
        self.rejects: List[Reject] = []
        self.participation: List[Part] = []

    def run(self) -> None:
        for nid in self._topo():
            self.occurrences.setdefault(nid, [])  # empty scans still produce a node
            node = self.nodes[nid]
            getattr(self, f"_op_{node['op']}")(node)

    # ------------------------------------------------------------------ #
    def _topo(self) -> List[str]:
        order: List[str] = []
        done: set = set()

        def visit(nid: str) -> None:
            if nid in done:
                return
            node = self.nodes[nid]
            for p in self._parent_ids(node):
                visit(p)
            done.add(nid)
            order.append(nid)

        for nid in self.nodes:
            visit(nid)
        return order

    @staticmethod
    def _parent_ids(node: Dict[str, Any]) -> List[str]:
        op = node["op"]
        if op == "scan":
            return []
        if op in ("filter", "project", "group", "distinct", "top_k"):
            return [node["input"]]
        if op == "union_all":
            return list(node["inputs"])
        return [node["left"], node["right"]]

    def _new(self, node_id: str, data: Dict[str, Any], padded: bool = False) -> Occ:
        bucket = self.occurrences.setdefault(node_id, [])
        occ = Occ(occ_id=len(bucket), data=data, padded=padded)
        bucket.append(occ)
        return occ

    # ------------------------------------------------------------------ #
    def _op_scan(self, node: Dict[str, Any]) -> None:
        src = self.sources[node["source"]]
        if src.get("type", "inline") == "inline":
            for r in src["rows"]:
                row = {"__row_id": r["row_id"], **r["values"]}
                self._new(node["id"], row)
        else:
            cols = src["schema"]
            for rid in range(src["count"]):
                row = {"__row_id": rid}
                for c in cols:
                    spec = src.get("generate", {}).get(c, {"kind": "lit", "value": None})
                    row[c] = rid if spec == "row_id" else eval_expr(spec, {"__row_id": rid})
                self._new(node["id"], row)

    def _op_filter(self, node: Dict[str, Any]) -> None:
        for in_occ in self.occurrences[node["input"]]:
            tv = eval_bool(node["pred"], in_occ.data)
            if tv is True:
                out = self._new(node["id"], dict(in_occ.data), in_occ.padded)
                out.parents.append((node["input"], in_occ.occ_id, "pass"))
            else:
                self.rejects.append(Reject(
                    node["id"], node["input"], in_occ.occ_id,
                    "filter_false" if tv is False else "filter_unknown",
                    {"truth": "false" if tv is False else "unknown"},
                ))

    def _op_project(self, node: Dict[str, Any]) -> None:
        for in_occ in self.occurrences[node["input"]]:
            out_row = {}
            for c in node["columns"]:
                out_row[c["name"]] = (
                    in_occ.data[c["keep"]] if "keep" in c
                    else eval_expr(c["expr"], in_occ.data)
                )
            out = self._new(node["id"], out_row, in_occ.padded)
            out.parents.append((node["input"], in_occ.occ_id, "pass"))

    def _op_union_all(self, node: Dict[str, Any]) -> None:
        for in_id in node["inputs"]:
            for in_occ in self.occurrences[in_id]:
                out = self._new(node["id"], dict(in_occ.data), in_occ.padded)
                out.parents.append((in_id, in_occ.occ_id, "branch"))

    def _op_join(self, node: Dict[str, Any]) -> None:
        kind, on = node["kind"], node["on"]
        prefix = node.get("right_prefix", "r_")
        left = self.occurrences[node["left"]]
        right = self.occurrences[node["right"]]
        right_cols = [c for c in self.occurrences[node["right"]][0].data] if right else []
        index: Dict[tuple, List[Occ]] = {}
        for ro in right:
            key = tuple(ro.data[p["right"]] for p in on)
            if any(v is None for v in key):
                continue
            index.setdefault(key, []).append(ro)

        for lo in left:
            lkey = tuple(lo.data[p["left"]] for p in on)
            cands = [] if any(v is None for v in lkey) else index.get(lkey, [])
            passing = [ro for ro in cands if self._residual(node, lo.data, ro.data, prefix)]

            if kind == "anti":
                if passing:
                    ro = passing[0]
                    self.rejects.append(Reject(
                        node["id"], node["left"], lo.occ_id,
                        "anti_join_matched_witness",
                        {"witness_right_occ": ro.occ_id, "witness_row": dict(ro.data)},
                    ))
                else:
                    out = self._new(node["id"], dict(lo.data), lo.padded)
                    out.parents.append((node["left"], lo.occ_id, "pass"))
                continue

            if not passing:
                if kind == "inner":
                    reason = ("inner_join_no_equal_key_candidate" if not cands
                              else "inner_join_residual_failed")
                    self.rejects.append(Reject(
                        node["id"], node["left"], lo.occ_id, reason,
                        {"equal_key_candidate_count": len(cands)},
                    ))
                else:
                    combined = dict(lo.data)
                    for c in right_cols:
                        combined[prefix + c] = None
                    out = self._new(node["id"], combined, True)
                    out.parents.append((node["left"], lo.occ_id, "left_padded"))
                continue

            for ro in passing:
                combined = dict(lo.data)
                for c in right_cols:
                    combined[prefix + c] = ro.data[c]
                out = self._new(node["id"], combined, False)
                out.parents.append((node["left"], lo.occ_id, "branch"))
                out.parents.append((node["right"], ro.occ_id, "branch"))

    @staticmethod
    def _residual(node: Dict[str, Any], lrow: Dict[str, Any],
                  rrow: Dict[str, Any], prefix: str) -> bool:
        residual = node.get("residual")
        if residual is None:
            return True
        combined = dict(lrow)
        for c, v in rrow.items():
            combined[prefix + c] = v
        return eval_bool(residual, combined) is True

    def _op_group(self, node: Dict[str, Any]) -> None:
        keys = node.get("keys", [])
        rows = self.occurrences[node["input"]]
        if not rows:
            out_row = {k: None for k in keys}
            for a in node["aggs"]:
                out_row[a["name"]] = 0 if a["func"] == "count" else None
            self._new(node["id"], out_row)
            return
        groups: Dict[tuple, List[Occ]] = {}
        for occ in rows:
            groups.setdefault(tuple(occ.data[k] for k in keys), []).append(occ)
        for gk, members in groups.items():
            out_row = dict(zip(keys, gk))
            for a in node["aggs"]:
                name, func, arg = a["name"], a["func"], a.get("arg")
                if func == "count":
                    out_row[name] = (
                        len(members) if arg is None
                        else sum(1 for m in members if m.data[arg] is not None)
                    )
                else:
                    out_row[name] = sum(
                        m.data[arg] for m in members if m.data[arg] is not None
                    )
            gout = self._new(node["id"], out_row)
            for member in members:
                gout.parents.append((node["input"], member.occ_id, "group_member"))
                for a in node["aggs"]:
                    name, func, arg = a["name"], a["func"], a.get("arg")
                    in_count = func == "count" and (
                        arg is None or member.data[arg] is not None)
                    in_sum = func == "sum" and member.data[arg] is not None
                    self.participation.append(Part(
                        node["id"], gout.occ_id, node["input"], member.occ_id, name,
                        in_count, in_sum, member.data[arg] if in_sum else None,
                    ))

    def _op_distinct(self, node: Dict[str, Any]) -> None:
        keys = node["keys"]
        groups: Dict[tuple, List[Occ]] = {}
        for occ in self.occurrences[node["input"]]:
            groups.setdefault(tuple(occ.data[k] for k in keys), []).append(occ)
        for dk, members in groups.items():
            out = self._new(node["id"], dict(members[0].data))
            for member in members:
                out.parents.append((node["input"], member.occ_id, "merge"))

    def _op_top_k(self, node: Dict[str, Any]) -> None:
        k, terms = node["k"], node["order_by"]
        parts = node.get("partition_by", [])
        buckets: Dict[tuple, List[Occ]] = {}
        for occ in self.occurrences[node["input"]]:
            buckets.setdefault(tuple(occ.data[p] for p in parts), []).append(occ)
        for pk, members in buckets.items():
            ranked = sorted(members, key=lambda o: self._sort_key(o.data, terms))
            for kept in ranked[:k]:
                out = self._new(node["id"], dict(kept.data))
                out.parents.append((node["input"], kept.occ_id, "pass"))
            for dropped in ranked[k:]:
                self.rejects.append(Reject(
                    node["id"], node["input"], dropped.occ_id,
                    "top_k_below_cutoff",
                    {"k": k, "cutoff_last_kept": ranked[k - 1].occ_id},
                ))

    @staticmethod
    def _sort_key(row: Dict[str, Any], terms: List[Dict[str, Any]]) -> tuple:
        key = []
        for t in terms:
            v = eval_expr(t["expr"], row)
            null_flag = v is None
            if null_flag:
                key.append((1, 0))
                continue
            if t.get("dir", "asc") == "desc":
                v = -v if isinstance(v, (int, float)) and not isinstance(v, bool) else (
                    not v if isinstance(v, bool) else _Rev(v))
            key.append((0, v))
        return tuple(key)

    # ------------------------------------------------------------------ #
    # extracted expected results
    # ------------------------------------------------------------------ #
    def business_bags(self) -> Dict[str, List[Dict[str, Any]]]:
        """Multiset of business rows (identity columns kept for comparison)."""
        return {nid: [dict(o.data) for o in occs]
                for nid, occs in self.occurrences.items()}

    def expected_edges(self) -> List[Tuple[str, int, str, int, str]]:
        edges = []
        for nid, occs in self.occurrences.items():
            for o in occs:
                for pn, po, role in o.parents:
                    edges.append((pn, po, nid, o.occ_id, role))
        return sorted(edges)

    def expected_rejects(self) -> List[Tuple[str, str, int, str]]:
        return sorted((r.node_id, r.input_node, r.input_occ, r.reason)
                      for r in self.rejects)

    def expected_participation(self) -> List[Tuple[str, int, str, int, str, bool, bool]]:
        return sorted(
            (p.node_id, p.group_occ, p.member_node, p.member_occ, p.agg_name,
             p.in_count, p.in_sum)
            for p in self.participation
        )


class _Rev:
    def __init__(self, v: Any):
        self.v = v

    def __lt__(self, other: "_Rev") -> bool:
        return self.v > other.v

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Rev) and self.v == other.v
