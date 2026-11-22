#!/usr/bin/env python3
"""Rerunnable drill for the archive/restore history feature.

Everything here is generated into a scratch directory; nothing large is
committed. The drill:

  1. Runs two real flows (tests/fixtures/archive_demo_v{1,2}.json) whose
     packages keep the same node ids and occurrence numbering but change
     contents and relations: same-value/different-occurrence rows, on-disk
     duplicate edges (self-join of one scan), rejects and group participation.
  2. Captures the five-table full-field multiset of each original package by
     reading the package directly, then archives both versions into one shared
     history library.
  3. Moves the original packages away and restores each version by name only;
     asserts the restored five-table multisets equal the direct-read
     references, and that trace-source/trace-output/node-balance/participation
     open against the restored package (preview is refused).
  4. Checks same-name idempotent retry (no duplicate evidence writes),
     same-name different-content conflict (explicit refusal), two version
     names registering identical evidence, and refusal of unfinished packages.
  5. Kills an archive in the pre-publication write window, reopens the library
     and proves the old version still restores, the new one is invisible, and
     the retry succeeds.
  6. Generates a pair of near-twin runtime flows whose differing rows carry
     long texts ordinary compression cannot collapse, archives v2 alone into a
     control library and appends it to the shared library, and reports the
     measurement basis plus the ratio of added evidence payload (must be <=1/4
     of the control) and the whole-library delta (manifests/indexes included).

Run:  python3 -I scripts/archive_drill.py [--workdir DIR]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUDITOR = os.path.join(ROOT, "bin", "auditor")
FIX = os.path.join(ROOT, "tests", "fixtures")
sys.path.insert(0, os.path.join(ROOT, "vendor", "duckdb_pkg"))
sys.path.insert(0, ROOT)

import duckdb  # noqa: E402

from lineage_auditor.archive import EVIDENCE_TABLES, HistoryArchive  # noqa: E402
from lineage_auditor.package import AuditPackage  # noqa: E402

FIVE_TABLES = tuple(EVIDENCE_TABLES)


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #
def auditor(*args, env=None, check=True):
    full_env = dict(os.environ)
    full_env["PYTHONPATH"] = os.path.join(ROOT, "vendor", "duckdb_pkg")
    if env:
        full_env.update(env)
    p = subprocess.run(
        [sys.executable, "-I", AUDITOR, *args],
        capture_output=True, text=True, env=full_env, cwd=ROOT,
    )
    if check and p.returncode != 0:
        raise AssertionError(
            f"auditor {' '.join(args)} failed ({p.returncode}):\n{p.stderr}"
        )
    return p


def evidence_multiset(db_path):
    """Five-table full-field multiset read directly from a package database."""
    con = duckdb.connect(db_path, read_only=True)
    try:
        out = {}
        for table, cols in EVIDENCE_TABLES.items():
            rows = con.execute(
                f"SELECT {', '.join(cols)} FROM {table}"
            ).fetchall()
            out[table] = sorted(map(_hashable, rows))
        return out
    finally:
        con.close()


def _hashable(row):
    return tuple(row)


def assert_equal_multiset(a, b, label):
    if a != b:
        for t in FIVE_TABLES:
            if a.get(t) != b.get(t):
                raise AssertionError(
                    f"{label}: multiset mismatch on {t}: "
                    f"{len(a.get(t, []))} vs {len(b.get(t, []))} rows"
                )
        raise AssertionError(f"{label}: multiset mismatch")


def dir_bytes(path):
    total = 0
    for base, _dirs, files in os.walk(path):
        for name in files:
            total += os.path.getsize(os.path.join(base, name))
    return total


def chunk_bytes(library):
    return dir_bytes(os.path.join(library, "chunks"))


def version_bytes(library):
    return dir_bytes(os.path.join(library, "versions"))


# --------------------------------------------------------------------------- #
# part 1-4: semantic fixtures, restore fidelity, retry, conflicts
# --------------------------------------------------------------------------- #
def drill_semantics(work):
    report = {}
    pkg1 = os.path.join(work, "pkg_v1")
    pkg2 = os.path.join(work, "pkg_v2")
    library = os.path.join(work, "library")
    auditor("run", "--flow", os.path.join(FIX, "archive_demo_v1.json"),
            "--package", pkg1)
    auditor("run", "--flow", os.path.join(FIX, "archive_demo_v2.json"),
            "--package", pkg2)

    # direct-read references, captured while the originals still exist
    ref1 = evidence_multiset(os.path.join(pkg1, "evidence.duckdb"))
    ref2 = evidence_multiset(os.path.join(pkg2, "evidence.duckdb"))

    # required evidence shapes exist in the references
    n_edges_1 = len(ref1["edges"])
    n_edges_2 = len(ref2["edges"])
    assert n_edges_1 != len(set(ref1["edges"])) or \
        n_edges_2 != len(set(ref2["edges"])), "drill expects on-disk duplicate edges"
    assert ref1["rejects"] and ref2["rejects"], "drill expects reject evidence"
    assert ref1["participation"] and ref2["participation"], \
        "drill expects participation evidence"
    # same-value different occurrence: scan occ 0 and 1 have identical business
    # values and differ only by their __row_id identity (they stay two occs)
    scan_business = {}
    for r in ref1["occ"]:
        if r[0] == "scan_pairs":
            values = json.loads(r[2])
            values.pop("__row_id", None)
            scan_business[r[1]] = json.dumps(values, sort_keys=True)
    assert scan_business[0] == scan_business[1], \
        "fixture must contain a same-value/different-occ pair"    # versions genuinely differ in evidence
    assert ref1 != ref2, "v1 and v2 evidence must differ"
    report["reference_rows"] = {
        "v1": {t: len(ref1[t]) for t in FIVE_TABLES},
        "v2": {t: len(ref2[t]) for t in FIVE_TABLES},
    }
    report["duplicate_edge_rows"] = {
        "v1": n_edges_1 - len(set(ref1["edges"])),
        "v2": n_edges_2 - len(set(ref2["edges"])),
    }

    r1 = json.loads(auditor("archive", "--package", pkg1, "--library",
                            library, "--version", "rel-1").stdout)
    r2 = json.loads(auditor("archive", "--package", pkg2, "--library",
                            library, "--version", "rel-2").stdout)
    report["archive"] = {"rel-1": r1, "rel-2": r2}

    # same-name idempotent retry: same evidence fingerprint, zero new chunks
    retry = json.loads(auditor("archive", "--package", pkg1, "--library",
                               library, "--version", "rel-1").stdout)
    assert retry["status"] == "already_archived"
    assert retry["evidence_sha256"] == r1["evidence_sha256"]
    chunks_after_retry = set(os.listdir(os.path.join(library, "chunks")))
    assert len(chunks_after_retry) == r1["chunks_written"] + r2["chunks_written"]
    report["idempotent_retry"] = retry

    # same name, different content -> explicit refusal, exit 2
    conflict = auditor("archive", "--package", pkg2, "--library", library,
                       "--version", "rel-1", check=False)
    assert conflict.returncode == 2, conflict.stderr
    err = json.loads(conflict.stderr)
    assert err["status"] == "rejected" and "immutable" in err["error"]
    report["same_name_conflict"] = err

    # different name may register identical evidence
    alias = json.loads(auditor("archive", "--package", pkg1, "--library",
                               library, "--version", "rel-1-alias").stdout)
    assert alias["evidence_sha256"] == r1["evidence_sha256"]
    assert alias["chunks_written"] == 0
    report["alias_registration"] = alias

    # unfinished packages are refused
    running_pkg = os.path.join(work, "running_pkg")
    shutil.copytree(pkg1, running_pkg)
    manifest_path = os.path.join(running_pkg, "manifest.json")
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)
    manifest["status"] = "running"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f)
    refused = auditor("archive", "--package", running_pkg, "--library",
                      os.path.join(work, "lib2"), "--version", "x", check=False)
    assert refused.returncode == 2, refused.stderr
    assert "RUNNING" in refused.stderr
    report["unfinished_refused"] = json.loads(refused.stderr)

    # missing evidence table is refused
    broken_pkg = os.path.join(work, "broken_pkg")
    shutil.copytree(pkg1, broken_pkg)
    con = duckdb.connect(os.path.join(broken_pkg, "evidence.duckdb"))
    con.execute("DROP TABLE participation")
    con.close()
    refused2 = auditor("archive", "--package", broken_pkg, "--library",
                       os.path.join(work, "lib3"), "--version", "x", check=False)
    assert refused2.returncode == 2 and "participation" in refused2.stderr
    report["missing_table_refused"] = json.loads(refused2.stderr)

    # source packages must be untouched: rehash their bytes
    assert evidence_multiset(os.path.join(pkg1, "evidence.duckdb")) == ref1
    assert evidence_multiset(os.path.join(pkg2, "evidence.duckdb")) == ref2

    # move originals away: restore must work by version name alone
    hidden = os.path.join(work, "originals_hidden")
    os.makedirs(hidden)
    shutil.move(pkg1, os.path.join(hidden, "pkg_v1"))
    shutil.move(pkg2, os.path.join(hidden, "pkg_v2"))
    assert not os.path.exists(pkg1) and not os.path.exists(pkg2)

    rest1 = os.path.join(work, "restored_v1")
    rest2 = os.path.join(work, "restored_v2")
    auditor("restore", "--library", library, "--version", "rel-1",
            "--dest", rest1)
    auditor("restore", "--library", library, "--version", "rel-2",
            "--dest", rest2)
    got1 = evidence_multiset(os.path.join(rest1, "evidence.duckdb"))
    got2 = evidence_multiset(os.path.join(rest2, "evidence.duckdb"))
    assert_equal_multiset(got1, ref1, "rel-1 restore")
    assert_equal_multiset(got2, ref2, "rel-2 restore")
    # flow + manifest bytes preserved per version
    with open(os.path.join(hidden, "pkg_v1", "flow.json"), "rb") as f:
        assert f.read() == open(os.path.join(rest1, "flow.json"), "rb").read()
    report["restore_multiset_match"] = {"rel-1": True, "rel-2": True}

    # restore never overwrites and leaves no completed-looking failure
    clash = auditor("restore", "--library", library, "--version", "rel-1",
                    "--dest", rest1, check=False)
    assert clash.returncode == 2 and "already exists" in clash.stderr

    # all four existing query families open against restored packages
    query_checks = [
        ("trace-source", "--package", rest1, "--source", "src_pairs",
         "--row-id", "0"),
        ("trace-output", "--package", rest2, "--sink", "self_join",
         "--occ-id", "0"),
        ("node-balance", "--package", rest1),
        ("participation", "--package", rest2, "--node", "grp_members",
         "--occ-id", "0"),
    ]
    for q in query_checks:
        p = auditor(*q)
        assert json.loads(p.stdout), q
    # semantic spot checks through the restored packages
    nb = json.loads(auditor("node-balance", "--package", rest2).stdout)
    keep = next(n for n in nb["node_balance"] if n["node_id"] == "keep_positive")
    assert keep["rejected_occurrences"] == 3
    part = json.loads(auditor("participation", "--package", rest2,
                              "--node", "grp_members", "--occ-id", "0").stdout)
    sum_members = part["aggregates"]["amt_sum"]["members"]
    contributions = {m["member_occ"]: m["sum_contribution"]
                     for m in sum_members}
    assert contributions == {0: 10, 1: 15}
    report["trace_queries_on_restore"] = "all four query families succeed"

    # restored package cannot claim preview support
    corrections = os.path.join(work, "corrections.json")
    with open(corrections, "w", encoding="utf-8") as f:
        json.dump({"source": "src_pairs", "corrections": []}, f)
    pv = auditor("preview", "--package", rest1, "--sink", "self_join",
                 "--corrections", corrections, check=False)
    assert pv.returncode == 2 and "preview" in pv.stderr
    report["preview_refused_on_restore"] = True

    return library, report


# --------------------------------------------------------------------------- #
# part 5: interruption in the pre-publication write window
# --------------------------------------------------------------------------- #
def drill_interruption(work, library):
    # archive a stable old version first
    flow_v1 = os.path.join(FIX, "archive_demo_v1.json")
    old_pkg = os.path.join(work, "int_old_pkg")
    auditor("run", "--flow", flow_v1, "--package", old_pkg)
    auditor("archive", "--package", old_pkg, "--library", library,
            "--version", "int-old")

    new_pkg = os.path.join(work, "int_new_pkg")
    auditor("run", "--flow", os.path.join(FIX, "archive_demo_v2.json"),
            "--package", new_pkg)
    ref_new = evidence_multiset(os.path.join(new_pkg, "evidence.duckdb"))

    # hard-kill the archive after evidence chunks are written but before the
    # version directory rename (the visibility point)
    killed = auditor(
        "archive", "--package", new_pkg, "--library", library,
        "--version", "int-new",
        env={"LINEAGE_AUDITOR_ARCHIVE_KILL": "after_chunks"}, check=False,
    )
    assert killed.returncode == 2, killed.stderr

    archive = HistoryArchive(library)
    visible = archive.list_versions()
    assert "int-old" in visible and "int-new" not in visible, visible
    # no staging directory leaked into versions/
    leftovers = [n for n in os.listdir(os.path.join(library, "versions"))
                 if n.startswith(".")]
    assert not leftovers, leftovers

    # old version still fully restorable after the crash
    old_dest = os.path.join(work, "int_old_restored")
    auditor("restore", "--library", library, "--version", "int-old",
            "--dest", old_dest)
    old_ref = evidence_multiset(os.path.join(old_pkg, "evidence.duckdb"))
    assert_equal_multiset(
        evidence_multiset(os.path.join(old_dest, "evidence.duckdb")),
        old_ref, "int-old restore after crash",
    )

    # the invisible version can be retried safely and then restores completely
    retry = json.loads(auditor("archive", "--package", new_pkg,
                               "--library", library,
                               "--version", "int-new").stdout)
    assert retry["status"] == "archived"
    new_dest = os.path.join(work, "int_new_restored")
    auditor("restore", "--library", library, "--version", "int-new",
            "--dest", new_dest)
    assert_equal_multiset(
        evidence_multiset(os.path.join(new_dest, "evidence.duckdb")),
        ref_new, "int-new restore after retry",
    )
    return {
        "killed_version_invisible": True,
        "old_version_restored_after_kill": True,
        "retry_archived_and_restored": True,
        "retry": retry,
    }


# --------------------------------------------------------------------------- #
# part 6: sharing measurement with long-text near twins
# --------------------------------------------------------------------------- #
LONG_TEXT_LEN = 900
N_ROWS = 240
CHANGE_IDS = (3, 47, 150)


def _long_text(seed):
    # deterministic, high-entropy text: repeated prefixes would compress away
    h = hashlib.sha256(f"long-record-{seed}".encode()).hexdigest()
    return (f"record-{seed:04d}-" + h) * (LONG_TEXT_LEN // len(h) + 1)


def build_near_twin_flows():
    def flow(version_tag):
        rows = []
        for rid in range(N_ROWS):
            seed = rid + (10000 if version_tag == 2 and rid in CHANGE_IDS else 0)
            rows.append({
                "row_id": rid,
                "values": {
                    "bucket": rid % 8,
                    "note": _long_text(seed)[:LONG_TEXT_LEN],
                },
            })
        return {
            "sources": [{
                "name": "src_big",
                "schema": {"bucket": "int64", "note": "text"},
                "rows": rows,
            }],
            "nodes": [
                {"id": "scan_big", "op": "scan", "source": "src_big"},
                {"id": "f_bucket", "op": "filter", "input": "scan_big",
                 "pred": {"kind": "cmp", "op": ">=",
                          "left": {"kind": "col", "name": "bucket"},
                          "right": {"kind": "lit", "value": 1}}},
                {"id": "self_big", "op": "join", "kind": "inner",
                 "left": "scan_big", "right": "scan_big",
                 "on": [{"left": "bucket", "right": "bucket"}],
                 "right_prefix": "r_"},
                {"id": "grp_big", "op": "group", "input": "scan_big",
                 "keys": ["bucket"],
                 "aggs": [{"name": "n", "func": "count", "arg": None},
                          {"name": "note_sum", "func": "sum",
                           "arg": "bucket"}]},
            ],
            "sinks": ["f_bucket", "self_big", "grp_big"],
        }

    return flow(1), flow(2)


def drill_sharing(work):
    flow1, flow2 = build_near_twin_flows()
    f1 = os.path.join(work, "near_v1.json")
    f2 = os.path.join(work, "near_v2.json")
    with open(f1, "w", encoding="utf-8") as f:
        json.dump(flow1, f)
    with open(f2, "w", encoding="utf-8") as f:
        json.dump(flow2, f)

    big1 = os.path.join(work, "near_pkg_v1")
    big2 = os.path.join(work, "near_pkg_v2")
    auditor("run", "--flow", f1, "--package", big1)
    auditor("run", "--flow", f2, "--package", big2)
    ref1 = evidence_multiset(os.path.join(big1, "evidence.duckdb"))
    ref2 = evidence_multiset(os.path.join(big2, "evidence.duckdb"))
    # the three changed source rows fan out through the high-fanout self join,
    # so many downstream occ payloads change; edge/reject/participation rows
    # (identities and relations, not text payloads) stay exactly identical
    assert ref1["edges"] == ref2["edges"]
    assert ref1["rejects"] == ref2["rejects"]
    assert ref1["participation"] == ref2["participation"]
    assert len(ref1["occ"]) == len(ref2["occ"])
    changed_scan = sorted({
        r[1] for r in (set(ref1["occ"]) ^ set(ref2["occ"]))
        if r[0] == "scan_big"
    })
    assert changed_scan == list(CHANGE_IDS), changed_scan
    assert set(ref1["node_stats"]) == set(ref2["node_stats"])

    # control: archive v2, with the same settings, into an empty library
    control_lib = os.path.join(work, "control_library")
    control = json.loads(auditor(
        "archive", "--package", big2, "--library", control_lib,
        "--version", "control-v2").stdout)
    control_payload = chunk_bytes(control_lib)
    control_total = dir_bytes(control_lib)

    # shared: v1 then v2 appended; measure v2's marginal evidence payload and
    # the whole-library delta (chunks + version manifests/indexes)
    shared_lib = os.path.join(work, "shared_library")
    json.loads(auditor("archive", "--package", big1, "--library", shared_lib,
                      "--version", "near-v1").stdout)
    chunks_before = chunk_bytes(shared_lib)
    total_before = dir_bytes(shared_lib)
    append = json.loads(auditor("archive", "--package", big2,
                                "--library", shared_lib,
                                "--version", "near-v2").stdout)
    chunks_after = chunk_bytes(shared_lib)
    total_after = dir_bytes(shared_lib)

    added_payload = chunks_after - chunks_before
    total_delta = total_after - total_before
    ratio = added_payload / control_payload
    assert added_payload == append["evidence_payload_bytes_added"]
    assert ratio <= 0.25, (
        f"shared append added {added_payload} payload bytes vs control "
        f"{control_payload} (ratio {ratio:.3f} > 0.25)"
    )

    # restored near-twins still match direct reads
    for ver, pkg, ref in (("near-v1", big1, ref1), ("near-v2", big2, ref2)):
        dest = os.path.join(work, f"restored_{ver}")
        auditor("restore", "--library", shared_lib, "--version", ver,
                "--dest", dest)
        assert_equal_multiset(
            evidence_multiset(os.path.join(dest, "evidence.duckdb")),
            ref, f"{ver} restore",
        )

    return {
        "measurement_basis": (
            "evidence payload = bytes under library/chunks only (content-"
            "addressed row chunks, plain canonical bytes, no compression); "
            "version manifests/registry hold hashes and counts, never row "
            "payload; whole-library delta additionally counts versions/*"
        ),
        "rows_per_package": {t: len(ref2[t]) for t in FIVE_TABLES},
        "changed_long_text_rows": len(CHANGE_IDS),
        "control_archive": {
            "evidence_payload_bytes": control_payload,
            "whole_library_bytes": control_total,
            "chunks_written": control["chunks_written"],
        },
        "shared_append_v2": {
            "evidence_payload_bytes_added": added_payload,
            "whole_library_delta_bytes": total_delta,
            "chunks_written": append["chunks_written"],
            "payload_ratio_vs_control": round(ratio, 4),
            "within_one_quarter": ratio <= 0.25,
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", help="scratch directory (default: tempdir)")
    args = ap.parse_args()
    work = args.workdir or tempfile.mkdtemp(prefix="archive-drill-")
    os.makedirs(work, exist_ok=True)
    report = {"workdir": work}
    try:
        library, semantic = drill_semantics(work)
        report["semantic_drill"] = semantic
        report["interruption_drill"] = drill_interruption(work, library)
        report["sharing_drill"] = drill_sharing(work)
        report["status"] = "all checks passed"
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    except AssertionError as exc:
        report["status"] = "FAILED"
        report["error"] = str(exc) or repr(exc)
        print(json.dumps(report, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
