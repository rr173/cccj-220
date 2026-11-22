"""Declarative flow model, schema resolution and pre-execution validation.

A flow is a finite acyclic graph::

    {
      "sources": [ <source declaration>, ... ],
      "nodes":   [ <node declaration>, ... ],
      "sinks":   ["node_id", ...]
    }

Every problem that can be detected before touching data is rejected here with a
FlowValidationError: dangling columns, schema ambiguity, cycles, joins/union on
incompatible schemas, and top_k without a fully decisive ordering.
"""
from __future__ import annotations

from typing import Any, Dict, List, Set

from . import expr as E

ROW_ID_COL = "__row_id"  # identity column attached by every scan


class FlowValidationError(ValueError):
    pass


NODE_OPS = {
    "scan", "filter", "project", "union_all", "join",
    "group", "distinct", "top_k",
}
JOIN_KINDS = ("inner", "left", "anti")
AGG_FUNCS = ("count", "sum")


# --------------------------------------------------------------------------- #
# validation
# --------------------------------------------------------------------------- #
def validate(flow: Dict[str, Any]) -> None:
    sources = flow.get("sources", [])
    nodes = flow.get("nodes", [])
    sinks = flow.get("sinks", [])

    _validate_sources(sources)
    source_names = {s["name"] for s in sources}

    seen: Set[str] = set()
    for n in nodes:
        nid = n.get("id")
        if not nid or not isinstance(nid, str):
            raise FlowValidationError("every node needs a string id")
        if nid in seen:
            raise FlowValidationError(f"duplicate node id {nid!r}")
        seen.add(nid)
        if n.get("op") not in NODE_OPS:
            raise FlowValidationError(f"node {nid!r} has unknown op {n.get('op')!r}")
    if not sinks:
        raise FlowValidationError("flow must declare at least one sink")
    for s in sinks:
        if s not in seen:
            raise FlowValidationError(f"sink {s!r} is not a declared node")

    parents = _parents(nodes)
    topo = topological_order(nodes)  # raises on cycles / missing refs

    schemas: Dict[str, Dict[str, str]] = {}
    for nid in topo:
        node = next(n for n in nodes if n["id"] == nid)
        try:
            schemas[nid] = _resolve_node(node, schemas, source_names, sources)
        except E.ExpressionError as exc:
            raise FlowValidationError(f"node {nid!r}: {exc}") from exc
    # every sink must be reachable from at least one scan (topo already guarantees
    # connectivity); additionally reject sinks whose ancestor set has no scan
    for s in sinks:
        if not _has_scan_ancestor(s, parents):
            raise FlowValidationError(f"sink {s!r} has no scan ancestor")


def _validate_sources(sources: List[Dict[str, Any]]) -> None:
    names: Set[str] = set()
    for s in sources:
        name = s.get("name")
        if not name:
            raise FlowValidationError("every source needs a name")
        if name in names:
            raise FlowValidationError(f"duplicate source name {name!r}")
        names.add(name)
        schema = s.get("schema")
        if not schema or ROW_ID_COL in schema:
            raise FlowValidationError(
                f"source {name!r} schema must be non-empty and must not declare "
                f"the reserved column {ROW_ID_COL!r}"
            )
        for c, t in schema.items():
            if t not in E.VALUE_TYPES or t == "null":
                raise FlowValidationError(
                    f"source {name!r} column {c!r} has invalid stored type {t!r}"
                )
        if s.get("type", "inline") == "inline":
            rows = s.get("rows", [])
            ids: Set[Any] = set()
            for r in rows:
                if "row_id" not in r or "values" not in r:
                    raise FlowValidationError(
                        f"source {name!r} rows need row_id and values"
                    )
                rid = r["row_id"]
                if not isinstance(rid, int):
                    raise FlowValidationError(
                        f"source {name!r} row_id must be int64, got {rid!r}"
                    )
                if rid in ids:
                    raise FlowValidationError(
                        f"source {name!r} has duplicate row_id {rid}"
                    )
                ids.add(rid)
                vals = r["values"]
                if set(vals) != set(schema):
                    raise FlowValidationError(
                        f"source {name!r} row {rid} value columns "
                        f"{sorted(vals)} do not match schema {sorted(schema)}"
                    )
                for c, v in vals.items():
                    _check_value_type(name, c, v, schema[c])
        elif s["type"] == "generate":
            if not isinstance(s.get("count"), int) or s["count"] < 0:
                raise FlowValidationError(
                    f"generated source {name!r} needs a non-negative count"
                )
        else:
            raise FlowValidationError(
                f"source {name!r} has unknown type {s.get('type')!r}"
            )


def _check_value_type(source: str, col: str, v: Any, t: str) -> None:
    if v is None:
        return
    if t == "int64" and (isinstance(v, bool) or not isinstance(v, int)):
        raise FlowValidationError(f"{source}.{col} value {v!r} is not int64")
    if t == "text" and not isinstance(v, str):
        raise FlowValidationError(f"{source}.{col} value {v!r} is not text")
    if t == "bool" and not isinstance(v, bool):
        raise FlowValidationError(f"{source}.{col} value {v!r} is not bool")


def _parents(nodes: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    ids = {n["id"] for n in nodes}
    for n in nodes:
        nid = n["id"]
        op = n["op"]
        if op == "scan":
            out[nid] = []
        elif op in ("filter", "project", "group", "distinct", "top_k"):
            ref = n.get("input")
            if ref not in ids:
                raise FlowValidationError(f"node {nid!r} references missing input {ref!r}")
            out[nid] = [ref]
        elif op == "union_all":
            refs = n.get("inputs", [])
            if not refs:
                raise FlowValidationError(f"union_all {nid!r} needs inputs")
            for r in refs:
                if r not in ids:
                    raise FlowValidationError(f"node {nid!r} references missing input {r!r}")
            out[nid] = list(refs)
        elif op == "join":
            for side in ("left", "right"):
                ref = n.get(side)
                if ref not in ids:
                    raise FlowValidationError(f"node {nid!r} references missing {side} {ref!r}")
            out[nid] = [n["left"], n["right"]]
    return out


def topological_order(nodes: List[Dict[str, Any]]) -> List[str]:
    parents = _parents(nodes)
    visited: Dict[str, int] = {}  # 0=visiting, 1=done
    order: List[str] = []

    def visit(nid: str, stack: List[str]) -> None:
        state = visited.get(nid)
        if state == 1:
            return
        if state == 0:
            cycle = " -> ".join(stack[stack.index(nid):] + [nid])
            raise FlowValidationError(f"cycle detected: {cycle}")
        visited[nid] = 0
        for p in parents[nid]:
            visit(p, stack + [nid])
        visited[nid] = 1
        order.append(nid)

    for n in nodes:
        visit(n["id"], [])
    return order


def _has_scan_ancestor(nid: str, parents: Dict[str, List[str]]) -> bool:
    # scan nodes are exactly the nodes with no parents
    stack = [nid]
    seen: Set[str] = set()
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        if not parents[cur]:
            return True  # reached a scan node
        stack.extend(parents[cur])
    return False


# --------------------------------------------------------------------------- #
# schema resolution per op
# --------------------------------------------------------------------------- #
def _resolve_node(
    node: Dict[str, Any],
    schemas: Dict[str, Dict[str, str]],
    source_names: Set[str],
    sources: List[Dict[str, Any]],
) -> Dict[str, str]:
    nid = node["id"]
    op = node["op"]

    if op == "scan":
        src = node.get("source")
        if src not in source_names:
            raise FlowValidationError(f"scan {nid!r} references missing source {src!r}")
        source = next(s for s in sources if s["name"] == src)
        return {ROW_ID_COL: "int64", **dict(source["schema"])}

    if op == "filter":
        schema = dict(schemas[node["input"]])
        t = E.infer_type(node["pred"], schema)
        if t not in ("bool", "null"):
            raise FlowValidationError(f"filter {nid!r} predicate must be boolean, got {t}")
        return schema

    if op == "project":
        in_schema = schemas[node["input"]]
        out: Dict[str, str] = {}
        columns = node.get("columns", [])
        if not columns:
            raise FlowValidationError(f"project {nid!r} needs at least one output column")
        for c in columns:
            name = c["name"]
            if name in out:
                raise FlowValidationError(
                    f"project {nid!r} produces ambiguous duplicate column {name!r}"
                )
            if "keep" in c:
                if c["keep"] not in in_schema:
                    raise FlowValidationError(
                        f"project {nid!r} keeps dangling column {c['keep']!r}"
                    )
                out[name] = in_schema[c["keep"]]
            else:
                out[name] = E.infer_type(c["expr"], in_schema)
        return out

    if op == "union_all":
        ref_schemas = [schemas[r] for r in node["inputs"]]
        base = ref_schemas[0]
        for other in ref_schemas[1:]:
            if other != base:
                raise FlowValidationError(
                    f"union_all {nid!r} inputs have mismatched schemas: {base} vs {other}"
                )
        return dict(base)

    if op == "join":
        return _resolve_join(node, schemas)

    if op == "group":
        return _resolve_group(node, schemas)

    if op == "distinct":
        schema = schemas[node["input"]]
        keys = node.get("keys", [])
        if not keys:
            raise FlowValidationError(f"distinct {nid!r} needs explicit keys")
        for k in keys:
            if k not in schema:
                raise FlowValidationError(f"distinct {nid!r} key {k!r} is dangling")
        return dict(schema)

    if op == "top_k":
        schema = dict(schemas[node["input"]])
        if not isinstance(node.get("k"), int) or node["k"] <= 0:
            raise FlowValidationError(f"top_k {nid!r} needs a positive k")
        for p in node.get("partition_by", []):
            if p not in schema:
                raise FlowValidationError(f"top_k {nid!r} partition column {p!r} is dangling")
        order = node.get("order_by", [])
        if not order:
            raise FlowValidationError(f"top_k {nid!r} needs order_by terms")
        decisive = 0
        names: Set[str] = set()
        for term in order:
            t = E.infer_type(term["expr"], schema)
            if t not in ("int64", "text", "bool", "null"):
                raise FlowValidationError(f"top_k {nid!r} order term has bad type {t}")
            if term.get("dir", "asc") not in ("asc", "desc"):
                raise FlowValidationError(f"top_k {nid!r} order direction must be asc/desc")
            if term.get("decisive"):
                decisive += 1
            names.add(str(term["expr"]))
        if decisive == 0:
            raise FlowValidationError(
                f"top_k {nid!r} rejected: ordering must declare at least one fully "
                f"decisive tie-break field (decisive=true)"
            )
        return schema

    raise FlowValidationError(f"unhandled op {op!r}")


def _resolve_join(
    node: Dict[str, Any], schemas: Dict[str, Dict[str, str]]
) -> Dict[str, str]:
    nid = node["id"]
    kind = node.get("kind")
    if kind not in JOIN_KINDS:
        raise FlowValidationError(f"join {nid!r} kind must be one of {JOIN_KINDS}")
    left_schema = schemas[node["left"]]
    right_schema = schemas[node["right"]]
    on = node.get("on", [])
    if not on:
        raise FlowValidationError(f"join {nid!r} needs at least one equi-key in on")
    for pair in on:
        if pair["left"] not in left_schema:
            raise FlowValidationError(
                f"join {nid!r} left key {pair['left']!r} is dangling"
            )
        if pair["right"] not in right_schema:
            raise FlowValidationError(
                f"join {nid!r} right key {pair['right']!r} is dangling"
            )
        lt = left_schema[pair["left"]]
        rt = right_schema[pair["right"]]
        if lt != rt:
            raise FlowValidationError(
                f"join {nid!r} equi-key type mismatch {lt} vs {rt}"
            )
    prefix = node.get("right_prefix", "r_")
    out: Dict[str, str] = {}
    for c, t in left_schema.items():
        if c in out:
            raise FlowValidationError(f"join {nid!r} ambiguous left output column {c!r}")
        out[c] = t
    combined = dict(left_schema)
    combined.update({prefix + c: t for c, t in right_schema.items()})
    # Cross-side output names are allowed to coincide (a genuine left column may
    # share a name with a prefixed right column): each side keeps its own value
    # and downstream references disambiguate by the prefixed name. We only reject
    # a collision that would overwrite inside the prefixed namespace itself.
    prefixed_seen: Set[str] = set()
    for c, t in right_schema.items():
        rc = prefix + c
        if rc in prefixed_seen:
            raise FlowValidationError(
                f"join {nid!r} rejected: prefixed column {rc!r} produced twice"
            )
        prefixed_seen.add(rc)
        if rc in out:
            raise FlowValidationError(
                f"join {nid!r} rejected: output column {rc!r} is ambiguous "
                f"(a left column and a right column map to the same output name; "
                f"choose a non-empty right_prefix)"
            )
        out[rc] = t
    residual = node.get("residual")
    if residual is not None and E.infer_type(residual, combined) not in ("bool", "null"):
        raise FlowValidationError(f"join {nid!r} residual must be boolean")
    if kind == "anti":
        # anti join only returns the left side
        return dict(left_schema)
    return out


def _resolve_group(
    node: Dict[str, Any], schemas: Dict[str, Dict[str, str]]
) -> Dict[str, str]:
    nid = node["id"]
    in_schema = schemas[node["input"]]
    keys = node.get("keys", [])
    out: Dict[str, str] = {}
    for k in keys:
        if k not in in_schema:
            raise FlowValidationError(f"group {nid!r} key {k!r} is dangling")
        if k in out:
            raise FlowValidationError(f"group {nid!r} duplicate output column {k!r}")
        out[k] = in_schema[k]
    aggs = node.get("aggs", [])
    if not aggs:
        raise FlowValidationError(f"group {nid!r} needs at least one aggregate")
    for a in aggs:
        name = a["name"]
        func = a.get("func")
        if func not in AGG_FUNCS:
            raise FlowValidationError(f"group {nid!r} aggregate {name!r} has bad func {func!r}")
        if name in out:
            raise FlowValidationError(f"group {nid!r} duplicate output column {name!r}")
        arg = a.get("arg")
        if func == "count":
            if arg is not None and arg not in in_schema:
                raise FlowValidationError(f"group {nid!r} count arg {arg!r} is dangling")
            out[name] = "int64"
        else:
            if arg is None or arg not in in_schema:
                raise FlowValidationError(
                    f"group {nid!r} sum {name!r} needs an existing column argument"
                )
            if in_schema[arg] != "int64":
                raise FlowValidationError(
                    f"group {nid!r} sum {name!r} argument must be int64, "
                    f"got {in_schema[arg]}"
                )
            out[name] = "int64"
    return out


# --------------------------------------------------------------------------- #
# helpers used by the executor
# --------------------------------------------------------------------------- #
def node_map(flow: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {n["id"]: n for n in flow["nodes"]}


def source_map(flow: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {s["name"]: s for s in flow["sources"]}


def output_schemas(flow: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Re-run resolution and return every node's output schema (after validate)."""
    schemas: Dict[str, Dict[str, str]] = {}
    sources = flow["sources"]
    source_names = {s["name"] for s in sources}
    for nid in topological_order(flow["nodes"]):
        node = next(n for n in flow["nodes"] if n["id"] == nid)
        schemas[nid] = _resolve_node(node, schemas, source_names, sources)
    return schemas


def duckdb_type(t: str) -> str:
    return {"int64": "BIGINT", "text": "VARCHAR", "bool": "BOOLEAN", "null": "VARCHAR"}[t]
