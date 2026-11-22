#!/usr/bin/env python3
"""DuckDB-SQL cross-check.

Renders the declarative flow to *native DuckDB SQL*, executes it in a fresh
in-memory DuckDB connection, and compares every node's business bag against the
occurrence evidence the auditor materialized. This independently shows that the
audit package matches a real DuckDB execution rather than static SQL guessing.

Supported ops: scan, filter, project, union_all, inner/left/anti join,
group(count/sum), distinct (compared on its declared keys). top_k is reported
as skipped by this cross-check (it is covered by the reference interpreter).

Run:  python3 -I scripts/crosscheck_duckdb.py <flow.json> <package_dir>
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "vendor", "duckdb_pkg"))
sys.path.insert(0, ROOT)

import duckdb  # noqa: E402

from lineage_auditor import expr as E  # noqa: E402
from lineage_auditor import flow as F  # noqa: E402


def lit_scan_sql(source):
    rows = source.get("rows", [])
    cols = ["__row_id", *source["schema"].keys()]

    def val(v):
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, int):
            return str(v)
        return "'" + str(v).replace("'", "''") + "'"

    if not rows:
        types = ["BIGINT", *[F.duckdb_type(t) for t in source["schema"].values()]]
        # one single SELECT so every typed NULL keeps its intended column name
        select_list = ", ".join(
            f"NULL::{ty} AS \"{c}\"" for ty, c in zip(types, cols)
        )
        return f"SELECT * FROM (SELECT {select_list}) WHERE FALSE"
    tuples = []
    for r in rows:
        vals = [str(r["row_id"])] + [val(r["values"][c]) for c in source["schema"]]
        tuples.append("(" + ",".join(vals) + ")")
    return f"SELECT * FROM (VALUES {','.join(tuples)}) AS t({','.join(cols)})"


def gen_scan_sql(source):
    if source.get("type", "inline") == "inline":
        return lit_scan_sql(source)
    n = source["count"]
    cols = ["__row_id", *source["schema"].keys()]
    selects = ["range AS __row_id"]
    for c in source["schema"]:
        spec = source.get("generate", {}).get(c, {"kind": "lit", "value": None})
        if spec == "row_id":
            selects.append(f"range AS {c}")
        else:
            selects.append(f"{E.to_sql(spec)} AS {c}")
    return f"SELECT {', '.join(selects)} FROM range({n})"


def _qualified_sql(expr, left_cols, right_cols, prefix):
    """Render a residual against l.<left col> / r.<right col> names.

    The flow residual references right columns by their *prefixed* output name;
    inside the join's ON clause those must be translated back to r.<original>.
    """
    k = expr["kind"]
    if k == "col":
        name = expr["name"]
        if name.startswith(prefix) and name[len(prefix):] in right_cols:
            return f'r."{name[len(prefix):]}"'
        if name in left_cols:
            return f'l."{name}"'
        # already-prefixed left-named column fallback
        return f'l."{name}"'
    if k == "lit":
        return E.to_sql(expr)
    if k == "cmp":
        return (f"({_qualified_sql(expr['left'], left_cols, right_cols, prefix)} "
                f"{expr['op']} "
                f"{_qualified_sql(expr['right'], left_cols, right_cols, prefix)})")
    if k in ("and", "or"):
        joiner = " AND " if k == "and" else " OR "
        return "(" + joiner.join(
            _qualified_sql(a, left_cols, right_cols, prefix) for a in expr["args"]) + ")"
    if k == "not":
        return f"(NOT {_qualified_sql(expr['arg'], left_cols, right_cols, prefix)})"
    if k in ("is_null", "is_not_null"):
        op = "IS NULL" if k == "is_null" else "IS NOT NULL"
        return f"({_qualified_sql(expr['arg'], left_cols, right_cols, prefix)} {op})"
    raise ValueError(f"unsupported expr {k}")


def render(node, sources, schemas):
    op = node["op"]
    if op == "scan":
        return gen_scan_sql(sources[node["source"]])
    if op == "filter":
        return (f"SELECT * FROM ({render_ref(node['input'])}) "
                f"WHERE {E.to_sql(node['pred'])}")
    if op == "project":
        parts = []
        for c in node["columns"]:
            if "keep" in c:
                parts.append(f'"{c["keep"]}" AS "{c["name"]}"')
            else:
                parts.append(f'{E.to_sql(c["expr"])} AS "{c["name"]}"')
        return f"SELECT {', '.join(parts)} FROM ({render_ref(node['input'])})"
    if op == "union_all":
        return " UNION ALL ".join(f"SELECT * FROM ({render_ref(i)})"
                                  for i in node["inputs"])
    if op == "join":
        kind, prefix = node["kind"], node.get("right_prefix", "r_")
        l, r = node["left"], node["right"]
        on = " AND ".join(f'l."{p["left"]}" = r."{p["right"]}"' for p in node["on"])
        join_kw = {"inner": "JOIN", "left": "LEFT JOIN",
                   "anti": "ANTI JOIN"}[kind]
        rcols = schemas[r]
        lcols = schemas[l]
        rselect = ", ".join(f'r."{c}" AS "{prefix}{c}"' for c in rcols)
        if node.get("residual"):
            extra = " AND " + _qualified_sql(
                node["residual"], set(lcols), set(rcols), prefix)
        else:
            extra = ""
        if kind == "anti":
            sql = (f"SELECT l.* FROM ({render_ref(l)}) l {join_kw} "
                   f"({render_ref(r)}) r ON {on}{extra}")
        else:
            sql = (f"SELECT l.*, {rselect} FROM ({render_ref(l)}) l {join_kw} "
                   f"({render_ref(r)}) r ON {on}{extra}")
        return sql
    if op == "group":
        keys = node.get("keys", [])
        ksel = ", ".join(f'"{k}"' for k in keys)
        aggs = []
        for a in node["aggs"]:
            if a["func"] == "count":
                expr = "COUNT(*)" if a.get("arg") is None else f'COUNT("{a["arg"]}")'
            else:
                expr = f'SUM("{a["arg"]}")'
            aggs.append(f'{expr} AS "{a["name"]}"')
        cols = ", ".join(filter(None, [ksel, *aggs]))
        in_sql = render_ref(node["input"])
        group = (" GROUP BY " + ", ".join(f'"{k}"' for k in keys)) if keys else ""
        main = f"SELECT {cols} FROM ({in_sql}){group}"
        # empty-input semantics: exactly one all-NULL-key group, count=0, sum=NULL
        key_nulls = ", ".join("NULL" for _ in keys)
        tail_nulls = []
        for a in node["aggs"]:
            tail_nulls.append("0" if a["func"] == "count" else "NULL")
        fallback_cols = ", ".join(filter(None, [key_nulls, *tail_nulls]))
        fallback = (f" SELECT {fallback_cols} WHERE NOT EXISTS "
                    f"(SELECT 1 FROM ({in_sql}))")
        return f"SELECT * FROM ({main} UNION ALL {fallback})"
    if op == "distinct":
        keys = ", ".join(f'"{k}"' for k in node["keys"])
        return (f"SELECT DISTINCT {keys} FROM ({render_ref(node['input'])}) "
                f"ORDER BY {keys}")
    return None  # top_k etc.


REFS = {}


def render_ref(nid):
    return REFS[nid]


def bag(rows):
    return sorted(json.dumps(r, sort_keys=True, default=str) for r in rows)


def main(flow_path, pkg_path):
    flow = json.load(open(flow_path, encoding="utf-8"))
    F.validate(flow)
    sources = F.source_map(flow)
    schemas = F.output_schemas(flow)
    con = duckdb.connect()
    failures, skipped = [], []
    pkg = duckdb.connect(os.path.join(pkg_path, "evidence.duckdb"), read_only=True)
    for nid in F.topological_order(flow["nodes"]):
        node = next(n for n in flow["nodes"] if n["id"] == nid)
        sql = render(node, sources, schemas)
        if sql is None:
            skipped.append(nid)
            continue
        REFS[nid] = sql
        native = con.execute(sql).fetchall()
        cols = [d[0] for d in con.description]
        native_bag = bag([dict(zip(cols, row)) for row in native])
        if node["op"] == "distinct":
            evidence = [
                {k: json.loads(dj)[k] for k in node["keys"]}
                for dj, in pkg.execute(
                    "SELECT data_json FROM occ WHERE node_id=? ORDER BY occ_id",
                    [nid]).fetchall()
            ]
        else:
            evidence = [
                json.loads(dj) for dj, in pkg.execute(
                    "SELECT data_json FROM occ WHERE node_id=? ORDER BY occ_id",
                    [nid]).fetchall()
            ]
        ev_bag = bag(evidence)
        status = "MATCH" if native_bag == ev_bag else "MISMATCH"
        if status == "MISMATCH":
            failures.append(nid)
        print(f"{status:8} {nid:18} native={len(native_bag):5} evidence={len(ev_bag):5}")
    if skipped:
        print("skipped (covered by reference interpreter):", ", ".join(skipped))
    pkg.close()
    con.close()
    if failures:
        print("MISMATCH at:", failures)
        return 1
    print("all rendered nodes match native DuckDB execution")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], sys.argv[2]))
