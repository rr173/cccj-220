"""Tests for the content-addressed evidence history (archive/restore).

Run:  python3 -I tests/test_archive.py

The tests build real packages with the production executor and then prove:

  * five-table, full-field multiset fidelity through archive + restore,
    including duplicate rows (same value, distinct occurrence; repeated edge),
  * equality that ignores DuckDB physical row order and page layout,
  * immutability: idempotent same-content retry, explicit same-name conflict,
    alias names for identical evidence, refusal of unfinished/incomplete
    packages, read-only handling of the source package,
  * atomic visibility under a simulated hard kill in the write window,
  * restored packages serving the four trace query families while not
    claiming preview support.
"""
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "vendor", "duckdb_pkg"))
sys.path.insert(0, ROOT)

import duckdb  # noqa: E402

from lineage_auditor.archive import (  # noqa: E402
    ARCHIVE_FORMAT,
    ArchiveError,
    HistoryArchive,
    _bucket_scheme,
    encode_row,
    row_hash,
)
from lineage_auditor.executor import Executor  # noqa: E402
from lineage_auditor.package import AuditPackage  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures")
AUDITOR = os.path.join(ROOT, "bin", "auditor")
TABLES = ("occ", "edges", "rejects", "participation", "node_stats")
COLS = {
    "occ": ("node_id", "occ_id", "data_json", "padded"),
    "edges": ("from_node", "from_occ", "to_node", "to_occ", "role"),
    "rejects": ("reject_id", "node_id", "input_node", "input_occ",
                "reason_code", "detail_json"),
    "participation": ("node_id", "group_occ", "member_node", "member_occ",
                      "agg_name", "in_count", "in_sum", "contribution"),
    "node_stats": ("node_id", "op", "input_occ", "output_occ",
                   "rejected", "expanded", "merged"),
}


def load_fixture(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


def build_package(flow, path):
    pkg = AuditPackage.create(path, flow)
    Executor(flow, pkg).run()
    return pkg


def five_table_multiset(db_path):
    con = duckdb.connect(db_path, read_only=True)
    try:
        out = {}
        for table in TABLES:
            rows = con.execute(
                f"SELECT {', '.join(COLS[table])} FROM {table}"
            ).fetchall()
            out[table] = sorted(rows)
        return out
    finally:
        con.close()


class TestCanonicalEncoding(unittest.TestCase):
    def test_typed_encoding_distinguishes_null_empty_and_types(self):
        self.assertNotEqual(
            encode_row("occ", ("n", 1, None, False)),
            encode_row("occ", ("n", 1, "", False)),
        )
        # bool vs int never collide
        self.assertNotEqual(row_hash("t", (True,)), row_hash("t", (1,)))
        # the same scalar tuple in two tables never aliases
        self.assertNotEqual(row_hash("occ", ("n", 1)),
                            row_hash("edges", ("n", 1)))

    def test_encoding_is_stable_and_unicode_safe(self):
        row = ("sink/0", 7, '{"v":"é\\u00e9"}', True)
        h1 = row_hash("occ", row)
        h2 = row_hash("occ", tuple(list(row)))
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 64)

    def test_bucket_scheme_is_pow2_and_bounded(self):
        for n in (0, 1, 100, 1000, 100000):
            b = _bucket_scheme(n)
            self.assertEqual(b & (b - 1), 0)
            self.assertGreaterEqual(b, 256)
            self.assertLessEqual(b, 1 << 16)
        # larger tables get strictly more buckets
        self.assertGreater(_bucket_scheme(100000), _bucket_scheme(1000))


class TestArchiveRoundTrip(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.flow1 = load_fixture("archive_demo_v1.json")
        self.flow2 = load_fixture("archive_demo_v2.json")
        self.pkg1 = os.path.join(self.tmp, "pkg1")
        self.pkg2 = os.path.join(self.tmp, "pkg2")
        build_package(self.flow1, self.pkg1)
        build_package(self.flow2, self.pkg2)
        self.lib = os.path.join(self.tmp, "library")
        self.ref1 = five_table_multiset(os.path.join(self.pkg1, "evidence.duckdb"))
        self.ref2 = five_table_multiset(os.path.join(self.pkg2, "evidence.duckdb"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_duplicate_rows_are_a_multiset_not_a_set(self):
        # repeated on-disk edge and same-value/different-occ scan rows exist
        edges = self.ref1["edges"]
        self.assertLess(len(set(edges)), len(edges),
                        "fixture must contain fully duplicated edge rows")
        scan = [r for r in self.ref1["occ"] if r[0] == "scan_pairs"]
        business = {}
        for _nid, occ, dj, _pad in scan:
            values = json.loads(dj)
            values.pop("__row_id", None)
            business[occ] = json.dumps(values, sort_keys=True)
        self.assertEqual(business[0], business[1])

    def test_archive_then_restore_is_lossless_five_table_multiset(self):
        r1 = HistoryArchive(self.lib).archive(self.pkg1, "v1")
        r2 = HistoryArchive(self.lib).archive(self.pkg2, "v2")
        self.assertEqual(r1["status"], "archived")
        self.assertEqual(r2["status"], "archived")
        for ver, ref in (("v1", self.ref1), ("v2", self.ref2)):
            dest = os.path.join(self.tmp, f"restored-{ver}")
            HistoryArchive(self.lib).restore(ver, dest)
            got = five_table_multiset(os.path.join(dest, "evidence.duckdb"))
            self.assertEqual(got, ref, ver)
            # field order/types preserved, not just row values
            con = duckdb.connect(os.path.join(dest, "evidence.duckdb"),
                                  read_only=True)
            types = con.execute(
                "SELECT table_name, column_name, data_type "
                "FROM information_schema.columns ORDER BY table_name, "
                "ordinal_position"
            ).fetchall()
            con.close()
            self.assertTrue(all(t[2] in ("BIGINT", "VARCHAR", "BOOLEAN")
                                for t in types))

    def test_equality_ignores_physical_row_order(self):
        # Repack the same logical rows into a second DB in shuffled order;
        # archiving both under different names must share every chunk and
        # produce the same evidence fingerprint.
        shuffled_pkg = os.path.join(self.tmp, "shuffled")
        shutil.copytree(self.pkg1, shuffled_pkg)
        db = os.path.join(shuffled_pkg, "evidence.duckdb")
        con = duckdb.connect(db)
        for table in TABLES:
            rows = con.execute(
                f"SELECT {', '.join(COLS[table])} FROM {table}"
            ).fetchall()
            rev = list(reversed(rows))
            con.execute(f"DELETE FROM {table}")
            placeholders = ", ".join("?" for _ in COLS[table])
            con.executemany(
                f"INSERT INTO {table} VALUES ({placeholders})", rev)
        con.execute("CHECKPOINT")
        con.close()

        a = HistoryArchive(self.lib)
        r_orig = a.archive(self.pkg1, "ordered")
        r_shuf = a.archive(shuffled_pkg, "shuffled")
        self.assertEqual(r_orig["evidence_sha256"], r_shuf["evidence_sha256"])
        self.assertEqual(r_shuf["chunks_written"], 0)
        dest = os.path.join(self.tmp, "from-shuffled")
        HistoryArchive(self.lib).restore("shuffled", dest)
        self.assertEqual(
            five_table_multiset(os.path.join(dest, "evidence.duckdb")),
            self.ref1,
        )

    def test_same_name_retry_is_idempotent_and_conflict_is_refused(self):
        a = HistoryArchive(self.lib)
        first = a.archive(self.pkg1, "weekly")
        again = a.archive(self.pkg1, "weekly")
        self.assertEqual(again["status"], "already_archived")
        self.assertEqual(again["evidence_sha256"], first["evidence_sha256"])
        chunks_on_disk = set(os.listdir(os.path.join(self.lib, "chunks")))
        a.archive(self.pkg1, "weekly")  # another retry
        self.assertEqual(
            chunks_on_disk, set(os.listdir(os.path.join(self.lib, "chunks"))))
        with self.assertRaises(ArchiveError) as ctx:
            a.archive(self.pkg2, "weekly")
        self.assertIn("immutable", str(ctx.exception))

    def test_different_names_can_register_identical_evidence(self):
        a = HistoryArchive(self.lib)
        r1 = a.archive(self.pkg1, "name-a")
        r2 = a.archive(self.pkg1, "name-b")
        self.assertEqual(r1["evidence_sha256"], r2["evidence_sha256"])
        self.assertEqual(r2["chunks_written"], 0)
        self.assertEqual(sorted(a.list_versions()), ["name-a", "name-b"])

    def test_unfinished_and_incomplete_packages_are_refused(self):
        running = os.path.join(self.tmp, "running")
        shutil.copytree(self.pkg1, running)
        mp = os.path.join(running, "manifest.json")
        with open(mp, encoding="utf-8") as f:
            manifest = json.load(f)
        manifest["status"] = "running"
        with open(mp, "w", encoding="utf-8") as f:
            json.dump(manifest, f)
        with self.assertRaises(ArchiveError):
            HistoryArchive(self.lib).archive(running, "x")

        broken = os.path.join(self.tmp, "broken")
        shutil.copytree(self.pkg1, broken)
        con = duckdb.connect(os.path.join(broken, "evidence.duckdb"))
        con.execute("DROP TABLE rejects")
        con.close()
        with self.assertRaises(ArchiveError) as ctx:
            HistoryArchive(self.lib).archive(broken, "x")
        self.assertIn("rejects", str(ctx.exception))

    def test_source_package_is_never_modified(self):
        before = {}
        for name in ("manifest.json", "flow.json", "evidence.duckdb"):
            p = os.path.join(self.pkg1, name)
            with open(p, "rb") as f:
                before[name] = hashlib.sha256(f.read()).hexdigest()
        HistoryArchive(self.lib).archive(self.pkg1, "v1")
        HistoryArchive(self.lib).archive(self.pkg1, "v1")
        for name, h in before.items():
            with open(os.path.join(self.pkg1, name), "rb") as f:
                self.assertEqual(hashlib.sha256(f.read()).hexdigest(), h, name)

    def test_restore_refuses_existing_destination_and_unknown_version(self):
        HistoryArchive(self.lib).archive(self.pkg1, "v1")
        dest = os.path.join(self.tmp, "taken")
        os.makedirs(dest)
        with self.assertRaises(ArchiveError):
            HistoryArchive(self.lib).restore("v1", dest)
        with self.assertRaises(ArchiveError):
            HistoryArchive(self.lib).restore("nope",
                                             os.path.join(self.tmp, "z"))

    def test_restored_package_serves_traces_but_not_preview(self):
        HistoryArchive(self.lib).archive(self.pkg2, "v2")
        dest = os.path.join(self.tmp, "restored")
        HistoryArchive(self.lib).restore("v2", dest)
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(ROOT, "vendor", "duckdb_pkg")

        def run(*args):
            return subprocess.run(
                [sys.executable, "-I", AUDITOR, *args],
                capture_output=True, text=True, env=env, cwd=ROOT)

        self.assertEqual(run("node-balance", "--package", dest).returncode, 0)
        self.assertEqual(run("trace-source", "--package", dest,
                             "--source", "src_pairs", "--row-id", "0").returncode, 0)
        self.assertEqual(run("trace-output", "--package", dest,
                             "--sink", "self_join", "--occ-id", "0").returncode, 0)
        self.assertEqual(run("participation", "--package", dest,
                             "--node", "grp_members", "--occ-id", "0").returncode, 0)
        corrections = os.path.join(self.tmp, "c.json")
        with open(corrections, "w", encoding="utf-8") as f:
            json.dump({"source": "src_pairs", "corrections": []}, f)
        pv = run("preview", "--package", dest, "--sink", "self_join",
                 "--corrections", corrections)
        self.assertEqual(pv.returncode, 2)
        self.assertIn("preview", pv.stderr)

    def test_restored_manifest_marks_completed_and_keeps_flow(self):
        HistoryArchive(self.lib).archive(self.pkg1, "v1")
        dest = os.path.join(self.tmp, "restored")
        HistoryArchive(self.lib).restore("v1", dest)
        with open(os.path.join(dest, "manifest.json"), encoding="utf-8") as f:
            self.assertEqual(json.load(f)["status"], "completed")
        with open(os.path.join(dest, "flow.json"), "rb") as f:
            restored_flow = f.read()
        with open(os.path.join(self.pkg1, "flow.json"), "rb") as f:
            self.assertEqual(restored_flow, f.read())
        # no preview support files are fabricated
        self.assertFalse(os.path.exists(os.path.join(dest, "preview.json")))

    def test_library_marker_and_format_guards(self):
        lib = HistoryArchive(self.lib)
        lib.archive(self.pkg1, "v1")
        with open(os.path.join(self.lib, "archive.json"), encoding="utf-8") as f:
            marker = json.load(f)
        self.assertEqual(marker["format"], ARCHIVE_FORMAT)
        self.assertEqual(marker["tables"], list(TABLES))
        with self.assertRaises(ArchiveError):
            HistoryArchive(self.tmp).list_versions()


class TestAtomicVisibility(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.flow1 = load_fixture("archive_demo_v1.json")
        self.flow2 = load_fixture("archive_demo_v2.json")
        self.old_pkg = os.path.join(self.tmp, "old")
        self.new_pkg = os.path.join(self.tmp, "new")
        build_package(self.flow1, self.old_pkg)
        build_package(self.flow2, self.new_pkg)
        self.lib = os.path.join(self.tmp, "library")
        HistoryArchive(self.lib).archive(self.old_pkg, "old")
        self.new_ref = five_table_multiset(
            os.path.join(self.new_pkg, "evidence.duckdb"))
        self.old_ref = five_table_multiset(
            os.path.join(self.old_pkg, "evidence.duckdb"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _kill_archive(self, hook):
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(ROOT, "vendor", "duckdb_pkg")
        env["LINEAGE_AUDITOR_ARCHIVE_KILL"] = hook
        return subprocess.run(
            [sys.executable, "-I", AUDITOR, "archive", "--package",
             self.new_pkg, "--library", self.lib, "--version", "new"],
            capture_output=True, text=True, env=env, cwd=ROOT)

    def test_kill_before_visibility_leaves_old_version_intact_and_retryable(self):
        killed = self._kill_archive("after_chunks")
        self.assertEqual(killed.returncode, 2)
        lib = HistoryArchive(self.lib)
        self.assertEqual(lib.list_versions(), ["old"])
        # no staging directory leaks
        self.assertEqual(
            [n for n in os.listdir(os.path.join(self.lib, "versions"))
             if n.startswith(".")], [])
        # old version still restores fully
        old_dest = os.path.join(self.tmp, "old-restored")
        lib.restore("old", old_dest)
        self.assertEqual(
            five_table_multiset(os.path.join(old_dest, "evidence.duckdb")),
            self.old_ref)
        # safe retry makes the new version visible and complete
        retry = lib.archive(self.new_pkg, "new")
        self.assertEqual(retry["status"], "archived")
        new_dest = os.path.join(self.tmp, "new-restored")
        lib.restore("new", new_dest)
        self.assertEqual(
            five_table_multiset(os.path.join(new_dest, "evidence.duckdb")),
            self.new_ref)

    def test_kill_after_visibility_leaves_a_complete_version(self):
        killed = self._kill_archive("after_publish")
        self.assertEqual(killed.returncode, 2)
        lib = HistoryArchive(self.lib)
        self.assertIn("new", lib.list_versions())
        dest = os.path.join(self.tmp, "new-restored")
        lib.restore("new", dest)
        self.assertEqual(
            five_table_multiset(os.path.join(dest, "evidence.duckdb")),
            self.new_ref)
        # retrying the now-visible identical version is a no-op
        again = lib.archive(self.new_pkg, "new")
        self.assertEqual(again["status"], "already_archived")


if __name__ == "__main__":
    unittest.main(verbosity=2)
