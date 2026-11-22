"""Run-time preparation of preview-support structures.

When `run` builds an audit package it also writes, alongside the evidence, a
set of narrow index tables plus a row-less copy of the flow structure. They
let `lineage_auditor.preview` answer "what would this batch of lookup
corrections do to that top_k list" with *point lookups only*:

  preview_occ          (node,occ) -> row, for every node a preview may read
  preview_edges        out-edges of nodes in the walk domain (affected join
                       down to the top_k input), for the bounded forward walk
  preview_source_rows  (scan,row_id) -> row, for correctable inline sources
  preview_join_keys    (join,side,equi-key) -> occurrence, for left/anti joins
                       whose right side is a correctable inline-source scan
  preview_topk_parts   (top_k sink, partition) -> candidate occurrence at the
                       top_k input

Every table is keyed by one synthetic text column (a JSON-array encoding of
the logical key) because the bundled DuckDB only uses *single-column* indexes
for point lookups; composite indexes are never selected for scans (verified
with EXPLAIN ANALYZE). Building these tables is a one-time, data-proportional
step of `run`; the preview command itself never re-scans full tables.

Old packages simply lack preview.json / these tables: preview refuses them
with an explanation instead of silently falling back to a full re-run.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Set

from . import flow as F

PREVIEW_VERSION = 1

PREVIEW_DDL = """
CREATE TABLE preview_occ (
    k         VARCHAR NOT NULL,
    node_id   VARCHAR NOT NULL,
    occ_id    BIGINT NOT NULL,
    data_json VARCHAR NOT NULL
);
CREATE TABLE preview_edges (
    k         VARCHAR NOT NULL,
    from_node VARCHAR NOT NULL,
    from_occ  BIGINT NOT NULL,
    to_node   VARCHAR NOT NULL,
    to_occ    BIGINT NOT NULL
);
CREATE TABLE preview_source_rows (
    k         VARCHAR NOT NULL,
    scan_node VARCHAR NOT NULL,
    row_id    BIGINT NOT NULL,
    data_json VARCHAR NOT NULL
);
CREATE TABLE preview_join_keys (
    k         VARCHAR NOT NULL,
    join_node VARCHAR NOT NULL,
    side      VARCHAR NOT NULL,
    key_json  VARCHAR NOT NULL,
    occ_id    BIGINT NOT NULL
);
CREATE TABLE preview_topk_parts (
    k         VARCHAR NOT NULL,
    topk_node VARCHAR NOT NULL,
    part_json VARCHAR NOT NULL,
    occ_id    BIGINT NOT NULL
);
"""

PREVIEW_INDEXES = """
CREATE INDEX idx_pocc_k ON preview_occ(k);
CREATE INDEX idx_pedges_k ON preview_edges(k);
CREATE INDEX idx_psr_k ON preview_source_rows(k);
CREATE INDEX idx_pjk_k ON preview_join_keys(k);
CREATE INDEX idx_ptp_k ON preview_topk_parts(k);
"""

PREVIEW_TABLES = (
    "preview_occ",
    "preview_edges",
    "preview_source_rows",
    "preview_join_keys",
    "preview_topk_parts",
)

_NULL_MARK = {"__null__": True}


# --------------------------------------------------------------------------- #
# key encodings (single source of truth shared with preview.py)
# --------------------------------------------------------------------------- #
def jkey(*parts: Any) -> str:
    """Encode a logical lookup key as one deterministic JSON-array string."""
    return json.dumps(list(parts), ensure_ascii=False, separators=(",", ":"))


def key_json(key_tuple: tuple) -> str:
    """Encode a non-NULL equi-key tuple (component order = the join's `on`)."""
    return json.dumps(list(key_tuple), ensure_ascii=False, separators=(",", ":"))


def part_json(part_tuple: tuple) -> str:
    """Encode a partition key tuple, marking NULLs like the executor does."""
    return json.dumps(
        [_NULL_MARK if v is None else v for v in part_tuple],
        ensure_ascii=False, separators=(",", ":"),
    )


def public_part_key(part_tuple: tuple) -> List[Any]:
    """JSON-ready partition key for output (same null marking as executor)."""
    return [dict(_NULL_MARK) if v is None else v for v in part_tuple]


# --------------------------------------------------------------------------- #
# batched inserts (same multi-row VALUES style as the executor)
# --------------------------------------------------------------------------- #
def _sql_value(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _insert_rows(con, table: str, cols: str, rows: List[tuple]) -> None:
    step = 5000
    for i in range(0, len(rows), step):
        chunk = rows[i:i + step]
        body = ",".join(
            "(" + ",".join(_sql_value(v) for v in row) + ")" for row in chunk
        )
        con.execute(f"INSERT INTO {table} {cols} VALUES {body}")


# --------------------------------------------------------------------------- #
# graph helpers over the flow structure
# --------------------------------------------------------------------------- #
def _children_map(nodes: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    children: Dict[str, List[str]] = {n["id"]: [] for n in nodes}
    for nid, ps in F._parents(nodes).items():
        for p in ps:
            children[p].append(nid)
    return children


def _ancestors_incl(parents: Dict[str, List[str]], nid: str) -> Set[str]:
    out: Set[str] = set()
    stack = [nid]
    while stack:
        cur = stack.pop()
        if cur in out:
            continue
        out.add(cur)
        stack.extend(parents[cur])
    return out


def _descendants_in(children: Dict[str, List[str]], nid: str, region: Set[str]) -> Set[str]:
    out: Set[str] = set()
    stack = [nid]
    while stack:
        cur = stack.pop()
        for c in children.get(cur, []):
            if c in region and c not in out:
                out.add(c)
                stack.append(c)
    return out


def scope_of(flow: Dict[str, Any]) -> Dict[str, Any]:
    """Compute the preview-relevant scope of a flow (also used by preview.py).

    Returns the top_k sinks, the union of their ancestor regions, the
    potentially-affected joins (region left/anti joins whose right side is a
    direct scan of an inline source), and the derived node sets to index.
    """
    nodes = F.node_map(flow)
    sources = F.source_map(flow)
    parents = F._parents(flow["nodes"])
    children = _children_map(flow["nodes"])

    topk_sinks = [s for s in flow["sinks"] if nodes[s]["op"] == "top_k"]
    region: Set[str] = set()
    for s in topk_sinks:
        region |= _ancestors_incl(parents, s)

    pa_joins: List[str] = []
    for nid in sorted(region):
        n = nodes[nid]
        if n["op"] != "join" or n.get("kind") not in ("left", "anti"):
            continue
        right = nodes[n["right"]]
        if right["op"] != "scan":
            continue
        if sources[right["source"]].get("type", "inline") != "inline":
            continue
        pa_joins.append(nid)

    # out-edges are indexed for: the joins' left inputs (so a left occurrence's
    # outputs at the join can be found), the joins themselves, and everything
    # downstream of them inside the region (the bounded forward-walk domain)
    walk_nodes: Set[str] = set(pa_joins)
    for j in pa_joins:
        walk_nodes.add(nodes[j]["left"])
        walk_nodes |= _descendants_in(children, j, region)

    occ_nodes: Set[str] = set()
    for s in topk_sinks:
        occ_nodes.add(nodes[s]["input"])
    for j in pa_joins:
        occ_nodes.add(nodes[j]["left"])
        occ_nodes.add(nodes[j]["right"])
        occ_nodes.add(j)

    src_scans = sorted({nodes[j]["right"] for j in pa_joins})
    return {
        "topk_sinks": topk_sinks,
        "region": region,
        "pa_joins": pa_joins,
        "walk_nodes": walk_nodes,
        "occ_nodes": occ_nodes,
        "src_scans": src_scans,
    }


# --------------------------------------------------------------------------- #
# build
# --------------------------------------------------------------------------- #
def build(flow: Dict[str, Any], pkg, con) -> Dict[str, Any]:
    """Build preview-support structures for a freshly executed package.

    Runs inside the caller's write connection (its own transaction) after the
    evidence tables and their indexes exist. Reads only the evidence tables of
    the relevant region; this is the one-time, data-proportional preparation
    that keeps every later preview lookup an indexed point read.
    """
    nodes = F.node_map(flow)
    scope = scope_of(flow)

    con.execute("BEGIN TRANSACTION")
    con.execute(PREVIEW_DDL)

    # (node,occ) -> row for every node a preview may point at
    rows: List[tuple] = []
    for nid in sorted(scope["occ_nodes"]):
        for occ_id, data_json in con.execute(
            "SELECT occ_id, data_json FROM occ WHERE node_id=? ORDER BY occ_id",
            [nid],
        ).fetchall():
            rows.append((jkey(nid, occ_id), nid, occ_id, data_json))
    _insert_rows(con, "preview_occ", "(k, node_id, occ_id, data_json)", rows)

    # out-edges of the walk domain (affected join -> ... -> top_k input)
    rows = []
    for nid in sorted(scope["walk_nodes"]):
        for from_occ, to_node, to_occ in con.execute(
            "SELECT from_occ, to_node, to_occ FROM edges WHERE from_node=? "
            "ORDER BY from_occ, to_node, to_occ",
            [nid],
        ).fetchall():
            rows.append((jkey(nid, from_occ), nid, from_occ, to_node, to_occ))
    _insert_rows(
        con, "preview_edges", "(k, from_node, from_occ, to_node, to_occ)", rows)

    # (scan,row_id) -> row for correctable inline sources
    rows = []
    for scan_node in scope["src_scans"]:
        for occ_id, data_json in con.execute(
            "SELECT occ_id, data_json FROM occ WHERE node_id=? ORDER BY occ_id",
            [scan_node],
        ).fetchall():
            row_id = json.loads(data_json)[F.ROW_ID_COL]
            rows.append((jkey(scan_node, row_id), scan_node, row_id, data_json))
    _insert_rows(
        con, "preview_source_rows", "(k, scan_node, row_id, data_json)", rows)

    # (join,side,equi-key) -> occurrence; NULL-keyed rows never match, so they
    # are deliberately absent from the index
    rows = []
    for j in scope["pa_joins"]:
        node = nodes[j]
        on = node["on"]
        for side, input_node in (("left", node["left"]), ("right", node["right"])):
            for occ_id, data_json in con.execute(
                "SELECT occ_id, data_json FROM occ WHERE node_id=? ORDER BY occ_id",
                [input_node],
            ).fetchall():
                row = json.loads(data_json)
                key = tuple(row[p[side]] for p in on)
                if any(v is None for v in key):
                    continue
                kj = key_json(key)
                rows.append((jkey(j, side, kj), j, side, kj, occ_id))
    _insert_rows(
        con, "preview_join_keys", "(k, join_node, side, key_json, occ_id)", rows)

    # (top_k sink, partition) -> candidate occurrence at the top_k input
    rows = []
    for s in scope["topk_sinks"]:
        node = nodes[s]
        part_cols = node.get("partition_by", [])
        input_node = node["input"]
        for occ_id, data_json in con.execute(
            "SELECT occ_id, data_json FROM occ WHERE node_id=? ORDER BY occ_id",
            [input_node],
        ).fetchall():
            row = json.loads(data_json)
            pk = tuple(row.get(p) for p in part_cols)
            pj = part_json(pk)
            rows.append((jkey(s, pj), s, pj, occ_id))
    _insert_rows(
        con, "preview_topk_parts", "(k, topk_node, part_json, occ_id)", rows)

    con.execute(PREVIEW_INDEXES)
    con.execute("COMMIT")

    _write_structure(flow, pkg)
    return {
        "version": PREVIEW_VERSION,
        "topk_sinks": sorted(scope["topk_sinks"]),
        "previewable_joins": scope["pa_joins"],
    }


def _write_structure(flow: Dict[str, Any], pkg) -> None:
    """Write preview.json: the exact flow structure with source rows stripped.

    Preview must never re-parse flow.json (it embeds every source row); this
    row-less copy carries everything preview needs: schemas, node definitions,
    expressions and sink declarations.
    """
    sources = []
    for s in flow["sources"]:
        entry = {
            "name": s["name"],
            "type": s.get("type", "inline"),
            "schema": dict(s["schema"]),
        }
        if entry["type"] == "generate":
            entry["count"] = s["count"]
            if "generate" in s:
                entry["generate"] = s["generate"]
        sources.append(entry)
    meta = {
        "preview_version": PREVIEW_VERSION,
        "structure": {
            "sources": sources,
            "nodes": flow["nodes"],
            "sinks": flow["sinks"],
        },
    }
    tmp = pkg.preview_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    os.replace(tmp, pkg.preview_path)
