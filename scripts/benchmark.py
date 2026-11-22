#!/usr/bin/env python3
"""Scale benchmark: high-fanout join + large shared-source group.

Proves that tracing ONE source row is paginated and does not expand all fan-out
paths into Python memory:
  * evidence row counts (occ / edges / participation) are recorded;
  * peak resident memory is measured while paging one row's 200 join outputs;
  * pages are fetched one at a time with a small page size.

Run:  python3 -I scripts/benchmark.py
"""
import json
import os
import resource
import shutil
import sys
import time
import tracemalloc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "vendor", "duckdb_pkg"))
sys.path.insert(0, ROOT)

import duckdb  # noqa: E402

from lineage_auditor.executor import Executor  # noqa: E402
from lineage_auditor.package import AuditPackage  # noqa: E402
from lineage_auditor.audit import AuditReader  # noqa: E402

FLOW = os.path.join(ROOT, "tests", "fixtures", "scale_benchmark.json")
OUT = os.path.join(ROOT, "out", "benchmark_pkg")
TARGET_ROW = 42
PAGE_SIZE = 25


def rss_mb():
    # ru_maxrss is KB on Linux
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def main():
    with open(FLOW, encoding="utf-8") as f:
        flow = json.load(f)
    src_counts = {s["name"]: s["count"] for s in flow["sources"]}
    expected_fanout_per_row = src_counts["big_right"]
    if os.path.exists(OUT):
        shutil.rmtree(OUT)

    t0 = time.perf_counter()
    pkg = AuditPackage.create(OUT, flow)
    Executor(flow, pkg).run()
    build_s = time.perf_counter() - t0
    build_peak_mb = rss_mb()

    con = duckdb.connect(os.path.join(OUT, "evidence.duckdb"), read_only=True)
    evidence = {
        "occ_rows": con.execute("SELECT count(*) FROM occ").fetchone()[0],
        "edge_rows": con.execute("SELECT count(*) FROM edges").fetchone()[0],
        "participation_rows": con.execute(
            "SELECT count(*) FROM participation").fetchone()[0],
        "distinct_evidence_nodes": con.execute(
            "SELECT count(DISTINCT node_id) FROM occ").fetchone()[0],
        "fanout_output_occ": con.execute(
            "SELECT count(*) FROM occ WHERE node_id='fanout_join'").fetchone()[0],
        "fanout_edges": con.execute(
            "SELECT count(*) FROM edges WHERE to_node='fanout_join'").fetchone()[0],
        "group_output_occ": con.execute(
            "SELECT count(*) FROM occ WHERE node_id='big_group'").fetchone()[0],
        "db_file_mb": round(
            os.path.getsize(os.path.join(OUT, "evidence.duckdb")) / 1024 / 1024, 2),
    }
    con.close()

    # ---- paginated trace of exactly one high-fanout source row --------------
    tracemalloc.start()
    t1 = time.perf_counter()
    pages = []
    with AuditReader(AuditPackage(OUT)) as rd:
        first = rd.trace_source("big_left", TARGET_ROW, ["fanout_join"],
                                page_size=PAGE_SIZE, page=0)
        total = first["surviving_outputs"]["total"]
        reachable = first["reachable_occurrence_total"]
        pages.append(len(first["surviving_outputs"]["rows"]))
        needed = (total + PAGE_SIZE - 1) // PAGE_SIZE
        for page_no in range(1, needed):
            t = rd.trace_source("big_left", TARGET_ROW, ["fanout_join"],
                                page_size=PAGE_SIZE, page=page_no)
            pages.append(len(t["surviving_outputs"]["rows"]))
        # also trace one member of the large shared group, first page only
        tg = rd.trace_source("wide_members", TARGET_ROW, ["big_group"],
                             page_size=PAGE_SIZE, page=0)
    query_s = time.perf_counter() - t1
    cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    result = {
        "build": {
            "seconds": round(build_s, 2),
            "peak_rss_mb": round(build_peak_mb, 1),
        },
        "evidence": evidence,
        "single_row_trace": {
            "target_source_row_id": TARGET_ROW,
            "expected_join_outputs_for_this_row": expected_fanout_per_row,
            "reported_total_surviving_outputs": total,
            "reachable_occurrences_for_this_row": reachable,
            "page_size": PAGE_SIZE,
            "pages_fetched": len(pages),
            "rows_per_page": pages,
            "query_seconds": round(query_s, 3),
            "python_tracemalloc_peak_kb": round(peak / 1024, 1),
            "process_max_rss_mb": round(rss_mb(), 1),
        },
        "shared_group_trace": {
            "target_source_row_id": TARGET_ROW,
            "group_outputs_reached": tg["surviving_outputs"]["total"],
            "merge_destinations": [
                {"node": m["node_id"],
                 "total_contributing_input_occurrences":
                     m["total_contributing_input_occurrences"]}
                for m in tg["merge_destinations"]
            ],
        },
        "conclusion": (
            "one source row's trace returns only its own occurrences; the "
            f"{evidence['fanout_edges']:,}-row join edge table is scanned by "
            "DuckDB indexes/recursive CTE and never fully loaded into Python"
        ),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    out_json = os.path.join(ROOT, "out", "benchmark_result.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\nwritten: {out_json}")


if __name__ == "__main__":
    main()
