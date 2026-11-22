#!/usr/bin/env python3
"""Re-runnable drill for `slice-flow` (the delivery hand-over rehearsal).

This is exactly the sequence the downstream team can repeat on the real flow:

1. run the **full** flow and keep the row bag obtained by projecting the chosen
   sink onto the delivery columns (types, names, multiset);
2. `slice-flow` the same sink, inspect that the reduced source schema is the
   new supply contract (unused fields are physically gone from schema *and*
   inline values);
3. feed the sliced flow **from only the fields its own source declarations
   list**, run it, and compare the delivered bag with the full-flow projection;
4. hunt for the deliberately recognisable "must be gone" markers in the new
   definition and in the actual evidence of the new package;
5. repeat the whole comparison on a second batch of values with the same
   schema, proving the slice is driven by the declaration, not by the first
   batch's values.

Usage:  python3 -I scripts/slice_drill.py [--keep]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "vendor", "duckdb_pkg"))
sys.path.insert(0, ROOT)

import duckdb  # noqa: E402

from lineage_auditor.executor import Executor  # noqa: E402
from lineage_auditor.package import AuditPackage  # noqa: E402
from lineage_auditor.slice import slice_flow  # noqa: E402

FIXTURE = os.path.join(ROOT, "tests", "fixtures", "slice_demo.json")
# The recognisable markers are only valid on the first batch; they are checked
# against the definition, not against the data.
MARKERS = ["DROP-LABEL-ALPHA", "DROP-LABEL-BETA", "DROP-LABEL-GAMMA",
           "DROP-LABEL-DELTA"]

# sink -> delivery columns used by the drill
DELIVERY = {
    "dist_ab": ["name"],
    "join_dup": ["name"],
    "grp_cnt": ["cnt"],
    "top_pick": ["grp"],
    "empty_rows": ["rows"],
}


def run_flow(flow, path):
    pkg = AuditPackage.create(path, flow)
    Executor(flow, pkg).run()
    return pkg


def occ_rows(pkg_path, node_id):
    con = duckdb.connect(os.path.join(pkg_path, "evidence.duckdb"), read_only=True)
    try:
        rows = [json.loads(dj) for dj, in con.execute(
            "SELECT data_json FROM occ WHERE node_id=? ORDER BY occ_id",
            [node_id]).fetchall()]
    finally:
        con.close()
    return rows


def projection_bag(rows, columns):
    """Types, names and multiset of the delivered projection."""
    packed = [json.dumps({c: r.get(c) for c in columns}, sort_keys=True,
                         ensure_ascii=False) for r in rows]
    return sorted(packed)


def types_of(rows, columns):
    seen = {}
    for r in rows:
        for c in columns:
            v = r.get(c)
            seen.setdefault(c, set()).add(
                "null" if v is None else
                "bool" if isinstance(v, bool) else
                "int64" if isinstance(v, int) else
                "text" if isinstance(v, str) else type(v).__name__)
    return seen


def supply_from_declaration(reduced, full_flow):
    """Rebuild the reduced flow's inputs from the *full* flow's rows.

    Only the fields the reduced source declarations list (plus row_id) are
    taken; every dropped field is materially absent.  This is what a new
    supplier looking at the generated definition would hand over, and it proves
    the reduced flow does not secretly read a field it stopped declaring.
    """
    supplied = json.loads(json.dumps(reduced))
    by_name = {s["name"]: s for s in full_flow["sources"]}
    for s in supplied["sources"]:
        original = by_name[s["name"]]
        if s.get("type", "inline") != "inline":
            continue
        rows = []
        for r in original.get("rows", []):
            values = {}
            for c in s["schema"]:
                if c not in r["values"]:
                    raise SystemExit(
                        f"declared field {s['name']}.{c} is absent from the input")
                values[c] = r["values"][c]
            rows.append({"row_id": r["row_id"], "values": values})
        s["rows"] = rows
    return supplied


def check_no_markers(reduced, pkg_path):
    blob = json.dumps(reduced, ensure_ascii=False)
    problems = [m for m in MARKERS if m in blob]
    con = duckdb.connect(os.path.join(pkg_path, "evidence.duckdb"), read_only=True)
    try:
        evidence = "\n".join(
            r[0] for r in con.execute("SELECT data_json FROM occ").fetchall())
        evidence += "\n".join(
            r[0] for r in con.execute("SELECT detail_json FROM rejects").fetchall())
    finally:
        con.close()
    problems += [m for m in MARKERS if m in evidence]
    return problems


def drop_check(flow, reduced):
    """Unused source fields must be absent from BOTH schema and inline values."""
    orig = {s["name"]: s for s in flow["sources"]}
    problems = []
    for s in reduced["sources"]:
        kept = set(s["schema"])
        if s.get("type", "inline") == "inline":
            for r in s.get("rows", []):
                if set(r["values"]) != kept:
                    problems.append(
                        f"{s['name']} row {r['row_id']} still carries "
                        f"{sorted(set(r['values']) - kept)}")
        for c in orig[s["name"]]["schema"]:
            if c not in kept:
                # the marker value of the dropped column must not survive
                vals = {r["values"][c] for r in orig[s["name"]].get("rows", [])}
                if any(isinstance(v, str) and v in MARKERS for v in vals):
                    marker = next(v for v in vals if v in MARKERS)
                    if marker in json.dumps(reduced, ensure_ascii=False):
                        problems.append(f"dropped field {s['name']}.{c} marker leaked")
    return problems


def drill_once(flow, label, tmp, keep=False):
    print(f"\n=== batch: {label}")
    full_pkg = run_flow(flow, os.path.join(tmp, f"{label}_full"))
    failures = 0
    for sink, columns in DELIVERY.items():
        full_rows = occ_rows(full_pkg.path, sink)
        expected = projection_bag(full_rows, columns)

        reduced = slice_flow(flow, sink, columns)
        # the downstream supplier receives only the fields the reduced
        # definition declares; running *that* flow is the real acceptance check
        supplied = supply_from_declaration(reduced, flow)
        self_pkg = run_flow(supplied, os.path.join(tmp, f"{label}_{sink}"))
        new_rows = occ_rows(self_pkg.path, sink)
        got = projection_bag(new_rows, columns)

        # the delivered projection must have identical column names and types
        exp_types = types_of(full_rows, columns)
        got_types = types_of(new_rows, columns)
        same_types = all(exp_types.get(c) == got_types.get(c) for c in columns)

        problems = drop_check(flow, reduced) + check_no_markers(reduced,
                                                                self_pkg.path)

        ok = expected == got and same_types and not problems
        failures += 0 if ok else 1
        print(f"  {'OK  ' if ok else 'FAIL'} {sink} <- {columns} "
              f"({len(expected)} rows)")
        for s in reduced["sources"]:
            print(f"       supply {s['name']}: {list(s['schema'])}")
        if problems:
            for p in problems:
                print(f"       PROBLEM {p}")
        if expected != got:
            print(f"       expected {expected}")
            print(f"       got      {got}")
        if not same_types:
            print(f"       types full {exp_types} new {got_types}")
        if not ok:
            # keep the failing artefacts for inspection
            print(f"       packages: {full_pkg.path} / {self_pkg.path}")
    print(f"  -> {len(DELIVERY) - failures}/{len(DELIVERY)} sinks matched")
    return failures


def second_batch(flow):
    """Same schema, different values: no marker, different duplicates/keys."""
    other = json.loads(json.dumps(flow))
    by_name = {s["name"]: s for s in other["sources"]}
    by_name["src_flow"]["rows"] = [
        {"row_id": 1, "values": {"k": 1, "grp": 10, "hidden_a": 1, "hidden_b": 1,
                                 "label": "x1", "amount": 11, "score": 5, "tie": 1}},
        {"row_id": 2, "values": {"k": 1, "grp": 10, "hidden_a": -1, "hidden_b": 2,
                                 "label": "x2", "amount": 13, "score": 9, "tie": 2}},
        {"row_id": 3, "values": {"k": 2, "grp": 20, "hidden_a": 3, "hidden_b": -1,
                                 "label": "x3", "amount": 17, "score": 1, "tie": 3}},
        {"row_id": 4, "values": {"k": 4, "grp": 20, "hidden_a": 4, "hidden_b": 4,
                                 "label": "x4", "amount": 19, "score": 2, "tie": 4}},
    ]
    by_name["src_lookup"]["rows"] = [
        {"row_id": 30, "values": {"k": 1, "rv": 1, "rflag": True}},
        {"row_id": 31, "values": {"k": 2, "rv": 2, "rflag": True}},
        {"row_id": 32, "values": {"k": 2, "rv": 3, "rflag": True}},
        {"row_id": 33, "values": {"k": 4, "rv": 4, "rflag": False}},
    ]
    return other


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="keep packages for review")
    args = ap.parse_args()
    with open(FIXTURE, encoding="utf-8") as f:
        flow = json.load(f)

    tmp = tempfile.mkdtemp(prefix="slice_drill_")
    print(f"drill workspace: {tmp}")
    failed = drill_once(flow, "batch1", tmp)
    failed += drill_once(second_batch(flow), "batch2", tmp)

    if failed:
        print(f"\nFAILED: {failed} sink comparison(s) did not match")
        return 1
    print("\nAll delivery bags, source declarations and evidence checks matched.")
    if not args.keep:
        shutil.rmtree(tmp, ignore_errors=True)
    else:
        print(f"packages kept under {tmp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
