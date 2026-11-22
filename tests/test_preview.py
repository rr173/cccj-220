"""Acceptance tests for the `preview` command (correction rehearsal).

Run:  python3 -I tests/test_preview.py

Expectations are anchored two ways:
  * hand-computed diffs for the main reproduction fixture;
  * an independent full recomputation with tests/reference_interpreter.py
    (which shares no code with the production package): apply the corrections
    to the flow, re-run the whole flow, and diff the sink's top-k bags per
    partition. Preview must agree with that oracle on every scenario.

Scale behaviour is proven with actual read/execution observations: the
preview's own query/row counters must stay flat while unrelated lookup keys
and ranking partitions grow 10x, and every preview SQL statement must plan as
an Index Scan (EXPLAIN ANALYZE), never a Sequential Scan of the evidence or
preview tables.
"""
import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "vendor", "duckdb_pkg"))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import duckdb  # noqa: E402

from lineage_auditor.executor import Executor  # noqa: E402
from lineage_auditor.package import AuditPackage, PackageError  # noqa: E402
from lineage_auditor.preview import PreviewError, preview  # noqa: E402
from reference_interpreter import ReferenceInterpreter  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures")
AUDITOR = os.path.join(ROOT, "bin", "auditor")


def load_fixture(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


def canon(row):
    return json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def bag_diff(a, b):
    ca, cb = Counter(canon(r) for r in a), Counter(canon(r) for r in b)
    out = []
    for k in ca:
        d = ca[k] - cb.get(k, 0)
        if d > 0:
            out.append((json.loads(k), d))
    return sorted(out, key=lambda t: canon(t[0]))


def apply_corrections(flow, source_name, corrections):
    """Independent correction application on the raw flow (the oracle path)."""
    new_flow = copy.deepcopy(flow)
    src = next(s for s in new_flow["sources"] if s["name"] == source_name)
    rows = {r["row_id"]: r for r in src["rows"]}
    for c in corrections:
        if c["op"] == "delete":
            del rows[c["row_id"]]
        else:
            rows[c["row_id"]] = {"row_id": c["row_id"], "values": c["values"]}
    src["rows"] = [rows[k] for k in sorted(rows)]
    return new_flow


def oracle_diff(flow, source_name, sink, corrections):
    """Full independent recomputation: original vs corrected flow, diff of the
    sink's per-partition top-k bags. Returns {partition_tuple: {"added": ...,
    "removed": ...}} with (row, count) entries."""
    node = next(n for n in flow["nodes"] if n["id"] == sink)
    part_cols = node.get("partition_by", [])
    ref_old = ReferenceInterpreter(flow)
    ref_old.run()
    ref_new = ReferenceInterpreter(apply_corrections(flow, source_name, corrections))
    ref_new.run()

    def bags(interp):
        out = {}
        for occ in interp.occurrences[sink]:
            pk = tuple(occ.data.get(p) for p in part_cols)
            out.setdefault(pk, []).append(occ.data)
        return out

    old_b, new_b = bags(ref_old), bags(ref_new)
    result = {}
    for pk in set(old_b) | set(new_b):
        added = bag_diff(new_b.get(pk, []), old_b.get(pk, []))
        removed = bag_diff(old_b.get(pk, []), new_b.get(pk, []))
        if added or removed:
            result[pk] = {"added": added, "removed": removed}
    return result


def preview_diff_shape(result):
    """Normalize a preview result into the oracle's shape for comparison."""
    out = {}
    for p in result["partitions"]:
        pk = tuple(
            None if (isinstance(v, dict) and v.get("__null__")) else v
            for v in p["partition"]
        )
        out[pk] = {
            "added": [(e["row"], e["count"]) for e in p["added"]],
            "removed": [(e["row"], e["count"]) for e in p["removed"]],
        }
    return out


def normalize(diff):
    return {
        pk: {
            side: sorted(((canon(r), c) for r, c in entries))
            for side, entries in (("added", d["added"]), ("removed", d["removed"]))
        }
        for pk, d in diff.items()
    }


def pkg_fingerprint(path):
    out = {}
    for name in sorted(os.listdir(path)):
        p = os.path.join(path, name)
        if os.path.isfile(p):
            with open(p, "rb") as f:
                out[name] = hashlib.sha256(f.read()).hexdigest()
    return out


class PreviewCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def build_pkg(self, flow, name="pkg"):
        path = os.path.join(self.tmp, name)
        pkg = AuditPackage.create(path, flow)
        Executor(flow, pkg).run()
        return path

    def run_preview(self, pkg_path, sink, source, corrections, sql_log=None):
        """Run a preview and assert the package is byte-identical afterwards."""
        before = pkg_fingerprint(pkg_path)
        result = preview(pkg_path, sink, source, corrections, sql_log=sql_log)
        after = pkg_fingerprint(pkg_path)
        self.assertEqual(before, after,
                         "preview must not modify the package in any way")
        return result

    def assert_matches_oracle(self, flow, source, sink, corrections, result):
        expected = normalize(oracle_diff(flow, source, sink, corrections))
        got = normalize(preview_diff_shape(result))
        self.assertEqual(expected, got,
                         "preview diff disagrees with the independent full "
                         "recomputation")


# --------------------------------------------------------------------------- #
# main reproduction fixture: one cross-key replacement, two partitions
# --------------------------------------------------------------------------- #
class TestMainRepro(PreviewCase):
    @classmethod
    def setUpClass(cls):
        cls.flow = load_fixture("preview_demo.json")

    def setUp(self):
        super().setUp()
        self.pkg = self.build_pkg(self.flow)

    def corrections(self):
        return [
            {"op": "replace", "row_id": 1,
             "values": {"k": 200, "flag": 1, "note": "moved"}},
            {"op": "delete", "row_id": 3},
        ]

    def test_cross_key_replacement_two_partitions(self):
        res = self.run_preview(self.pkg, "top1", "src_lookup", self.corrections())
        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["affected_joins"], ["anti_j"])
        # hand-computed expectation:
        #  partition A: rec3 (lk 100) loses its last true match (row 2 keeps
        #    the key but its residual is UNKNOWN and must not block); rec3 is
        #    released and promoted from outside the old list, knocking out
        #    the uncorrected rec2.
        #  partition B: rec11 (lk 200) gains a true match and is blocked;
        #    below-cutoff rec13 backfills.
        #  partition C: control — rec21 had two true matches (rows 3,4);
        #    deleting row 3 does not release it, so C shows no change.
        self.assertEqual(preview_diff_shape(res), {
            ("A",): {
                "added": [({"part": "A", "rec": 3, "score": 45, "tie": 3}, 1)],
                "removed": [({"part": "A", "rec": 2, "score": 40, "tie": 2}, 1)],
            },
            ("B",): {
                "added": [({"part": "B", "rec": 13, "score": 30, "tie": 3}, 1)],
                "removed": [({"part": "B", "rec": 11, "score": 60, "tie": 1}, 1)],
            },
        })
        self.assert_matches_oracle(self.flow, "src_lookup", "top1",
                                   self.corrections(), res)

    def test_deterministic_repeat(self):
        r1 = self.run_preview(self.pkg, "top1", "src_lookup", self.corrections())
        r2 = self.run_preview(self.pkg, "top1", "src_lookup", self.corrections())
        self.assertEqual(json.dumps(r1, sort_keys=True), json.dumps(r2, sort_keys=True))

    def test_empty_batch_and_noop_batches_give_empty_result(self):
        for corrections in (
            [],
            [{"op": "replace", "row_id": 1,
              "values": {"k": 100, "flag": 1, "note": "will-move"}}],  # identical
            [{"op": "delete", "row_id": 5}],  # key 999 matches no left row
            [{"op": "replace", "row_id": 2,   # still NULL flag, same key
              "values": {"k": 100, "flag": None, "note": "renamed"}}],
        ):
            res = self.run_preview(self.pkg, "top1", "src_lookup", corrections)
            self.assertEqual(res["status"], "ok")
            self.assertEqual(res["partitions"], [],
                             f"expected empty diff for {corrections}")
            self.assert_matches_oracle(self.flow, "src_lookup", "top1",
                                       corrections, res)

    def test_delete_one_of_two_matches_keeps_anti_blocked(self):
        # the control half of the main batch, isolated: rec21 stays blocked
        res = self.run_preview(self.pkg, "top1", "src_lookup",
                               [{"op": "delete", "row_id": 3}])
        self.assertEqual(res["partitions"], [])
        self.assert_matches_oracle(self.flow, "src_lookup", "top1",
                                   [{"op": "delete", "row_id": 3}], res)


class TestCorrectionRejection(PreviewCase):
    @classmethod
    def setUpClass(cls):
        cls.flow = load_fixture("preview_demo.json")

    def setUp(self):
        super().setUp()
        self.pkg = self.build_pkg(self.flow)

    def assert_rejected(self, corrections, needle):
        before = pkg_fingerprint(self.pkg)
        with self.assertRaises(PreviewError) as cm:
            preview(self.pkg, "top1", "src_lookup", corrections)
        self.assertEqual(cm.exception.status, "rejected")
        self.assertIn(needle, str(cm.exception))
        self.assertEqual(before, pkg_fingerprint(self.pkg))

    def test_nonexistent_row_id(self):
        self.assert_rejected([{"op": "delete", "row_id": 777}], "does not exist")

    def test_duplicate_operations(self):
        self.assert_rejected(
            [{"op": "delete", "row_id": 1},
             {"op": "replace", "row_id": 1,
              "values": {"k": 5, "flag": 1, "note": "x"}}],
            "duplicate operation")

    def test_type_errors(self):
        self.assert_rejected(
            [{"op": "replace", "row_id": 1,
              "values": {"k": "not-an-int", "flag": 1, "note": "x"}}], "int64")
        self.assert_rejected(
            [{"op": "replace", "row_id": 1,
              "values": {"k": True, "flag": 1, "note": "x"}}], "int64")
        self.assert_rejected(
            [{"op": "replace", "row_id": 1,
              "values": {"k": 5, "flag": 1, "note": 9}}], "text")

    def test_schema_mismatch(self):
        self.assert_rejected(
            [{"op": "replace", "row_id": 1, "values": {"k": 5, "flag": 1}}],
            "do not match")
        self.assert_rejected(
            [{"op": "replace", "row_id": 1,
              "values": {"k": 5, "flag": 1, "note": "x", "extra": 1}}],
            "do not match")
        self.assert_rejected(
            [{"op": "replace", "row_id": 1,
              "values": {"__row_id": 1, "k": 5, "flag": 1, "note": "x"}}],
            "do not match")

    def test_bad_shapes(self):
        self.assert_rejected("not-a-list", "must be a JSON array")
        self.assert_rejected([{"op": "move", "row_id": 1}], "delete' or 'replace")
        self.assert_rejected([{"op": "delete", "row_id": "1"}], "int64")
        self.assert_rejected([{"op": "replace", "row_id": 1}], "values")


# --------------------------------------------------------------------------- #
# left join semantics (generated fixture, two sub-flows sharing one lookup)
# --------------------------------------------------------------------------- #
def left_join_flow():
    return {
        "sources": [
            {"name": "src_lk", "schema": {"x": "int64", "y": "int64",
                                          "flag": "int64", "tag": "text"},
             "rows": [
                 {"row_id": 1, "values": {"x": 1, "y": 1, "flag": 1, "tag": "x"}},
                 {"row_id": 2, "values": {"x": 1, "y": 1, "flag": 1, "tag": "y"}},
                 {"row_id": 3, "values": {"x": 2, "y": 2, "flag": 1, "tag": "m"}},
                 {"row_id": 4, "values": {"x": 2, "y": 2, "flag": 0, "tag": "z"}},
                 {"row_id": 5, "values": {"x": 4, "y": 4, "flag": 1, "tag": "w"}},
                 {"row_id": 6, "values": {"x": 7, "y": 7, "flag": 1, "tag": "e"}},
                 {"row_id": 7, "values": {"x": None, "y": 8, "flag": 1, "tag": "u"}},
                 {"row_id": 8, "values": {"x": 6, "y": 6, "flag": 1, "tag": "aaa"}},
                 {"row_id": 9, "values": {"x": 6, "y": 6, "flag": 1, "tag": "bbb"}},
                 {"row_id": 10, "values": {"x": 6, "y": 6, "flag": 1, "tag": "ccc"}},
             ]},
            {"name": "src_la", "schema": {"rec": "int64", "part": "text",
                                          "ka": "int64", "kb": "int64",
                                          "score": "int64", "tie": "int64"},
             "rows": [
                 {"row_id": 101, "values": {"rec": 101, "part": "P", "ka": 1, "kb": 1, "score": 10, "tie": 1}},
                 {"row_id": 102, "values": {"rec": 102, "part": "P", "ka": 2, "kb": 2, "score": 20, "tie": 2}},
                 {"row_id": 103, "values": {"rec": 103, "part": "P", "ka": 3, "kb": 3, "score": 5, "tie": 3}},
                 {"row_id": 104, "values": {"rec": 104, "part": "P", "ka": 4, "kb": 4, "score": 8, "tie": 4}},
                 {"row_id": 105, "values": {"rec": 105, "part": "R", "ka": 6, "kb": 6, "score": 50, "tie": 5}},
             ]},
            {"name": "src_lb", "schema": {"rec": "int64", "part": "text",
                                          "ka": "int64", "kb": "int64",
                                          "score": "int64", "tie": "int64"},
             "rows": [
                 {"row_id": 201, "values": {"rec": 201, "part": "Q", "ka": 7, "kb": 7, "score": 30, "tie": 1}},
                 {"row_id": 202, "values": {"rec": 202, "part": "Q", "ka": 8, "kb": 8, "score": 25, "tie": 2}},
                 {"row_id": 203, "values": {"rec": 203, "part": "Q", "ka": 9, "kb": None, "score": 15, "tie": 3}},
             ]},
        ],
        "nodes": [
            {"id": "scan_la", "op": "scan", "source": "src_la"},
            {"id": "scan_lb", "op": "scan", "source": "src_lb"},
            {"id": "scan_lk", "op": "scan", "source": "src_lk"},

            {"id": "joinA", "op": "join", "kind": "left",
             "left": "scan_la", "right": "scan_lk",
             "on": [{"left": "ka", "right": "x"}, {"left": "kb", "right": "y"}],
             "residual": {"kind": "cmp", "op": ">",
                          "left": {"kind": "col", "name": "r_flag"},
                          "right": {"kind": "lit", "value": 0}},
             "right_prefix": "r_"},
            {"id": "fA", "op": "filter", "input": "joinA",
             "pred": {"kind": "is_not_null", "arg": {"kind": "col", "name": "r_tag"}}},
            {"id": "projA", "op": "project", "input": "fA",
             "columns": [{"name": "part", "keep": "part"},
                         {"name": "score", "keep": "score"},
                         {"name": "tie", "keep": "tie"},
                         {"name": "r_tag", "keep": "r_tag"}]},
            {"id": "topA", "op": "top_k", "input": "projA", "k": 2,
             "partition_by": ["part"],
             "order_by": [
                 {"expr": {"kind": "col", "name": "score"}, "dir": "desc"},
                 {"expr": {"kind": "col", "name": "r_tag"}, "dir": "asc",
                  "decisive": True}]},

            {"id": "joinB", "op": "join", "kind": "left",
             "left": "scan_lb", "right": "scan_lk",
             "on": [{"left": "ka", "right": "x"}, {"left": "kb", "right": "y"}],
             "residual": {"kind": "cmp", "op": ">",
                          "left": {"kind": "col", "name": "r_flag"},
                          "right": {"kind": "lit", "value": 0}},
             "right_prefix": "r_"},
            {"id": "projB", "op": "project", "input": "joinB",
             "columns": [{"name": "part", "keep": "part"},
                         {"name": "score", "keep": "score"},
                         {"name": "tie", "keep": "tie"},
                         {"name": "r_tag", "keep": "r_tag"}]},
            {"id": "topB", "op": "top_k", "input": "projB", "k": 2,
             "partition_by": ["part"],
             "order_by": [
                 {"expr": {"kind": "col", "name": "score"}, "dir": "desc"},
                 {"expr": {"kind": "col", "name": "tie"}, "dir": "asc",
                  "decisive": True}]},
        ],
        "sinks": ["topA", "topB"],
    }


class TestLeftJoinSemantics(PreviewCase):
    def setUp(self):
        super().setUp()
        self.flow = left_join_flow()
        self.pkg = self.build_pkg(self.flow)

    def check(self, sink, corrections, expected):
        res = self.run_preview(self.pkg, sink, "src_lk", corrections)
        self.assertEqual(preview_diff_shape(res), expected)
        self.assert_matches_oracle(self.flow, "src_lk", sink, corrections, res)
        return res

    def test_every_valid_match_is_an_occurrence(self):
        # P1 has two true matches (tags x, y); deleting x leaves y, which moves
        # into the kept top-2 — occurrences are never collapsed by value
        self.check("topA", [{"op": "delete", "row_id": 1}], {
            ("P",): {
                "added": [({"part": "P", "score": 10, "tie": 1, "r_tag": "y"}, 1)],
                "removed": [({"part": "P", "score": 10, "tie": 1, "r_tag": "x"}, 1)],
            },
        })

    def test_last_match_loss_pads_and_residual_false_does_not_match(self):
        # deleting row 3 leaves only row 4 (residual FALSE) at key (2,2):
        # P2 is padded, the padded row is filtered out downstream, and the
        # below-cutoff candidate backfills
        self.check("topA", [{"op": "delete", "row_id": 3}], {
            ("P",): {
                "added": [({"part": "P", "score": 10, "tie": 1, "r_tag": "y"}, 1)],
                "removed": [({"part": "P", "score": 20, "tie": 2, "r_tag": "m"}, 1)],
            },
        })

    def test_below_cutoff_change_is_invisible(self):
        self.check("topA", [{"op": "delete", "row_id": 5}], {})

    def test_same_row_value_in_and_out_cancels(self):
        # row 8 (tag aaa) deleted while row 10 is replaced to produce tag aaa:
        # the underlying matches change, the projected rows do not
        self.check("topA", [
            {"op": "delete", "row_id": 8},
            {"op": "replace", "row_id": 10,
             "values": {"x": 6, "y": 6, "flag": 1, "tag": "aaa"}},
        ], {})

    def test_composite_key_with_null_component_never_matches(self):
        # replacement moves the key to (7, NULL): Q1 loses its match and its
        # padded row (r_tag NULL) enters the list
        self.check("topB", [
            {"op": "replace", "row_id": 6,
             "values": {"x": 7, "y": None, "flag": 1, "tag": "e"}},
        ], {
            ("Q",): {
                "added": [({"part": "Q", "score": 30, "tie": 1, "r_tag": None}, 1)],
                "removed": [({"part": "Q", "score": 30, "tie": 1, "r_tag": "e"}, 1)],
            },
        })

    def test_null_key_row_gains_a_real_key(self):
        # row 7 had a NULL key component (matched nothing); giving it key
        # (8, 8) makes Q2's padded row leave and the combined row enter
        self.check("topB", [
            {"op": "replace", "row_id": 7,
             "values": {"x": 8, "y": 8, "flag": 1, "tag": "u2"}},
        ], {
            ("Q",): {
                "added": [({"part": "Q", "score": 25, "tie": 2, "r_tag": "u2"}, 1)],
                "removed": [({"part": "Q", "score": 25, "tie": 2, "r_tag": None}, 1)],
            },
        })


    def test_same_key_value_change_updates_combined_row(self):
        # same key, same flag, new tag: the match persists but the combined
        # row's value changes, so the listed row changes with it
        self.check("topA", [
            {"op": "replace", "row_id": 1,
             "values": {"x": 1, "y": 1, "flag": 1, "tag": "x2"}},
        ], {
            ("P",): {
                "added": [({"part": "P", "score": 10, "tie": 1, "r_tag": "x2"}, 1)],
                "removed": [({"part": "P", "score": 10, "tie": 1, "r_tag": "x"}, 1)],
            },
        })

    def test_null_decisive_value_rejected(self):
        # the correction would leave a padded candidate whose decisive value
        # (r_tag) is NULL: decisive tuples must be non-empty -> batch rejected
        flow = {
            "sources": [
                {"name": "l", "schema": {"rec": "int64", "part": "text",
                                         "lk": "int64", "score": "int64",
                                         "tie": "int64"},
                 "rows": [{"row_id": 1, "values": {"rec": 1, "part": "Z", "lk": 10,
                                                   "score": 50, "tie": 1}}]},
                {"name": "lk", "schema": {"k": "int64", "flag": "int64", "tag": "text"},
                 "rows": [{"row_id": 1, "values": {"k": 10, "flag": 1, "tag": "m"}}]},
            ],
            "nodes": [
                {"id": "sl", "op": "scan", "source": "l"},
                {"id": "slk", "op": "scan", "source": "lk"},
                {"id": "lj", "op": "join", "kind": "left", "left": "sl", "right": "slk",
                 "on": [{"left": "lk", "right": "k"}],
                 "residual": {"kind": "cmp", "op": ">",
                              "left": {"kind": "col", "name": "r_flag"},
                              "right": {"kind": "lit", "value": 0}}},
                {"id": "pj", "op": "project", "input": "lj",
                 "columns": [{"name": "part", "keep": "part"},
                             {"name": "score", "keep": "score"},
                             {"name": "tie", "keep": "tie"},
                             {"name": "r_tag", "keep": "r_tag"}]},
                {"id": "t", "op": "top_k", "input": "pj", "k": 1,
                 "partition_by": ["part"],
                 "order_by": [
                     {"expr": {"kind": "col", "name": "score"}, "dir": "desc"},
                     {"expr": {"kind": "col", "name": "r_tag"}, "dir": "asc",
                      "decisive": True}]},
            ],
            "sinks": ["t"],
        }
        pkg = self.build_pkg(flow, "nulldec")
        before = pkg_fingerprint(pkg)
        with self.assertRaises(PreviewError) as cm:
            preview(pkg, "t", "lk", [{"op": "delete", "row_id": 1}])
        self.assertEqual(cm.exception.status, "rejected")
        self.assertIn("non-empty", str(cm.exception))
        self.assertEqual(before, pkg_fingerprint(pkg))


# --------------------------------------------------------------------------- #
# diamond re-merge (generated fixture)
# --------------------------------------------------------------------------- #
def diamond_flow():
    proj = lambda branch: [
        {"name": "part", "keep": "part"},
        {"name": "score", "keep": "score"},
        {"name": "tie", "keep": "tie"},
        {"name": "rec", "keep": "rec"},
        {"name": "branch", "expr": {"kind": "lit", "value": branch}},
    ]
    return {
        "sources": [
            {"name": "src_ld", "schema": {"rec": "int64", "part": "text",
                                          "lk": "int64", "score": "int64",
                                          "tie": "int64"},
             "rows": [
                 {"row_id": 301, "values": {"rec": 301, "part": "D", "lk": 8801, "score": 40, "tie": 1}},
                 {"row_id": 302, "values": {"rec": 302, "part": "D", "lk": 8802, "score": 30, "tie": 2}},
                 {"row_id": 303, "values": {"rec": 303, "part": "D", "lk": 500, "score": 50, "tie": 3}},
                 {"row_id": 304, "values": {"rec": 304, "part": "D", "lk": 600, "score": 10, "tie": 1}},
             ]},
            {"name": "src_lkd", "schema": {"k": "int64", "flag": "int64"},
             "rows": [
                 {"row_id": 1, "values": {"k": 500, "flag": 1}},
                 {"row_id": 2, "values": {"k": 600, "flag": 1}},
             ]},
        ],
        "nodes": [
            {"id": "scan_ld", "op": "scan", "source": "src_ld"},
            {"id": "scan_lkd", "op": "scan", "source": "src_lkd"},
            {"id": "anti_d", "op": "join", "kind": "anti",
             "left": "scan_ld", "right": "scan_lkd",
             "on": [{"left": "lk", "right": "k"}],
             "residual": {"kind": "cmp", "op": ">",
                          "left": {"kind": "col", "name": "r_flag"},
                          "right": {"kind": "lit", "value": 0}},
             "right_prefix": "r_"},
            {"id": "p1", "op": "project", "input": "anti_d", "columns": proj("b1")},
            {"id": "p2", "op": "project", "input": "anti_d", "columns": proj("b2")},
            {"id": "u", "op": "union_all", "inputs": ["p1", "p2"]},
            {"id": "topD", "op": "top_k", "input": "u", "k": 2,
             "partition_by": ["part"],
             "order_by": [
                 {"expr": {"kind": "col", "name": "score"}, "dir": "desc"},
                 {"expr": {"kind": "col", "name": "tie"}, "dir": "asc",
                  "decisive": True},
                 {"expr": {"kind": "col", "name": "branch"}, "dir": "asc",
                  "decisive": True}]},
        ],
        "sinks": ["topD"],
    }


class TestDiamond(PreviewCase):
    def setUp(self):
        super().setUp()
        self.flow = diamond_flow()
        self.pkg = self.build_pkg(self.flow)

    def test_release_fans_out_through_both_branches(self):
        res = self.run_preview(self.pkg, "topD", "src_lkd",
                               [{"op": "delete", "row_id": 1}])
        self.assertEqual(preview_diff_shape(res), {
            ("D",): {
                "added": [
                    ({"part": "D", "score": 50, "tie": 3, "rec": 303, "branch": "b1"}, 1),
                    ({"part": "D", "score": 50, "tie": 3, "rec": 303, "branch": "b2"}, 1),
                ],
                "removed": [
                    ({"part": "D", "score": 40, "tie": 1, "rec": 301, "branch": "b1"}, 1),
                    ({"part": "D", "score": 40, "tie": 1, "rec": 301, "branch": "b2"}, 1),
                ],
            },
        })
        self.assert_matches_oracle(self.flow, "src_lkd", "topD",
                                   [{"op": "delete", "row_id": 1}], res)

    def test_one_replace_reblocks_and_releases_across_the_diamond(self):
        # row 1 moves 500 -> 8801: rec303 is released, rec301 becomes blocked;
        # the bounded walk must find both branch occurrences of rec301
        corr = [{"op": "replace", "row_id": 1, "values": {"k": 8801, "flag": 1}}]
        res = self.run_preview(self.pkg, "topD", "src_lkd", corr)
        self.assertEqual(preview_diff_shape(res), {
            ("D",): {
                "added": [
                    ({"part": "D", "score": 50, "tie": 3, "rec": 303, "branch": "b1"}, 1),
                    ({"part": "D", "score": 50, "tie": 3, "rec": 303, "branch": "b2"}, 1),
                ],
                "removed": [
                    ({"part": "D", "score": 40, "tie": 1, "rec": 301, "branch": "b1"}, 1),
                    ({"part": "D", "score": 40, "tie": 1, "rec": 301, "branch": "b2"}, 1),
                ],
            },
        })
        self.assert_matches_oracle(self.flow, "src_lkd", "topD", corr, res)

    def test_decisive_conflict_at_ranking_end_rejects_whole_batch(self):
        # rec304 (tie 1) would be released and collide with rec301's decisive
        # tuple (1, b1) — rec304 scores last, far outside the kept top-2, yet
        # the whole batch (including the innocent delete of row 1) is rejected
        corr = [{"op": "delete", "row_id": 1}, {"op": "delete", "row_id": 2}]
        before = pkg_fingerprint(self.pkg)
        with self.assertRaises(PreviewError) as cm:
            preview(self.pkg, "topD", "src_lkd", corr)
        self.assertEqual(cm.exception.status, "rejected")
        self.assertIn("decisive", str(cm.exception))
        self.assertEqual(before, pkg_fingerprint(self.pkg))
        # the package still serves the valid preview afterwards (no partial
        # state was committed by the rejected batch)
        ok = self.run_preview(self.pkg, "topD", "src_lkd",
                              [{"op": "delete", "row_id": 1}])
        self.assertEqual(len(ok["partitions"]), 1)


# --------------------------------------------------------------------------- #
# unsupported packages / paths: explicit reasons, never a silent full re-run
# --------------------------------------------------------------------------- #
class TestUnsupported(PreviewCase):
    @classmethod
    def setUpClass(cls):
        cls.flow = load_fixture("preview_demo.json")

    def setUp(self):
        super().setUp()
        self.pkg = self.build_pkg(self.flow)

    def assert_unsupported(self, pkg, sink, source, needle):
        with self.assertRaises(PreviewError) as cm:
            preview(pkg, sink, source, [])
        self.assertEqual(cm.exception.status, "unsupported")
        self.assertIn(needle, str(cm.exception))

    def test_sink_not_top_k(self):
        self.assert_unsupported(self.pkg, "grp_other", "src_lookup", "top_k")

    def test_unknown_sink(self):
        self.assert_unsupported(self.pkg, "nope", "src_lookup", "unknown sink")

    def test_source_not_upstream(self):
        self.assert_unsupported(self.pkg, "top1", "src_other", "does not contribute")

    def test_source_used_as_left_input(self):
        self.assert_unsupported(self.pkg, "top1", "src_records",
                                "direct right-side scan")

    def test_unknown_source(self):
        self.assert_unsupported(self.pkg, "top1", "ghost", "unknown source")

    def test_inner_join_right_side(self):
        flow = {
            "sources": [
                {"name": "l", "schema": {"lk": "int64", "score": "int64", "tie": "int64"},
                 "rows": [{"row_id": 1, "values": {"lk": 1, "score": 1, "tie": 1}}]},
                {"name": "r", "schema": {"k": "int64"},
                 "rows": [{"row_id": 1, "values": {"k": 1}}]},
            ],
            "nodes": [
                {"id": "sl", "op": "scan", "source": "l"},
                {"id": "sr", "op": "scan", "source": "r"},
                {"id": "j", "op": "join", "kind": "inner", "left": "sl", "right": "sr",
                 "on": [{"left": "lk", "right": "k"}]},
                {"id": "t", "op": "top_k", "input": "j", "k": 1,
                 "order_by": [{"expr": {"kind": "col", "name": "tie"}, "dir": "asc",
                               "decisive": True}]},
            ],
            "sinks": ["t"],
        }
        pkg = self.build_pkg(flow, "inner_pkg")
        self.assert_unsupported(pkg, "t", "r", "left/anti joins")

    def test_group_between_join_and_sink(self):
        flow = {
            "sources": [
                {"name": "l", "schema": {"lk": "int64", "score": "int64", "tie": "int64"},
                 "rows": [{"row_id": 1, "values": {"lk": 1, "score": 1, "tie": 1}}]},
                {"name": "r", "schema": {"k": "int64"},
                 "rows": [{"row_id": 1, "values": {"k": 2}}]},
            ],
            "nodes": [
                {"id": "sl", "op": "scan", "source": "l"},
                {"id": "sr", "op": "scan", "source": "r"},
                {"id": "j", "op": "join", "kind": "anti", "left": "sl", "right": "sr",
                 "on": [{"left": "lk", "right": "k"}]},
                {"id": "g", "op": "group", "input": "j", "keys": ["tie"],
                 "aggs": [{"name": "n", "func": "count", "arg": None}]},
                {"id": "t", "op": "top_k", "input": "g", "k": 1,
                 "order_by": [{"expr": {"kind": "col", "name": "tie"}, "dir": "asc",
                               "decisive": True}]},
            ],
            "sinks": ["t"],
        }
        pkg = self.build_pkg(flow, "group_pkg")
        self.assert_unsupported(pkg, "t", "r", "'g' (group)")

    def test_generated_source(self):
        flow = {
            "sources": [
                {"name": "l", "schema": {"lk": "int64", "tie": "int64"},
                 "rows": [{"row_id": 1, "values": {"lk": 1, "tie": 1}}]},
                {"name": "r", "type": "generate", "count": 3,
                 "schema": {"k": "int64"}, "generate": {"k": "row_id"}},
            ],
            "nodes": [
                {"id": "sl", "op": "scan", "source": "l"},
                {"id": "sr", "op": "scan", "source": "r"},
                {"id": "j", "op": "join", "kind": "left", "left": "sl", "right": "sr",
                 "on": [{"left": "lk", "right": "k"}]},
                {"id": "t", "op": "top_k", "input": "j", "k": 1,
                 "order_by": [{"expr": {"kind": "col", "name": "tie"}, "dir": "asc",
                               "decisive": True}]},
            ],
            "sinks": ["t"],
        }
        pkg = self.build_pkg(flow, "gen_pkg")
        self.assert_unsupported(pkg, "t", "r", "generated source")

    def test_old_package_without_preview_support(self):
        os.remove(os.path.join(self.pkg, "preview.json"))
        self.assert_unsupported(self.pkg, "top1", "src_lookup", "no preview support")

    def test_missing_preview_tables(self):
        con = duckdb.connect(os.path.join(self.pkg, "evidence.duckdb"))
        for t in ("preview_occ", "preview_edges", "preview_source_rows",
                  "preview_join_keys", "preview_topk_parts"):
            con.execute(f"DROP TABLE {t}")
        con.close()
        self.assert_unsupported(self.pkg, "top1", "src_lookup",
                                "missing preview index tables")

    def test_failed_package_refused(self):
        # decisive conflict at run time -> failed package -> require_completed
        flow = {
            "sources": [{"name": "s", "schema": {"tie": "int64"},
                         "rows": [{"row_id": 1, "values": {"tie": 1}},
                                  {"row_id": 2, "values": {"tie": 1}}]}],
            "nodes": [
                {"id": "sc", "op": "scan", "source": "s"},
                {"id": "t", "op": "top_k", "input": "sc", "k": 1,
                 "order_by": [{"expr": {"kind": "col", "name": "tie"}, "dir": "asc",
                               "decisive": True}]},
            ],
            "sinks": ["t"],
        }
        path = os.path.join(self.tmp, "failed_pkg")
        pkg = AuditPackage.create(path, flow)
        with self.assertRaises(Exception):
            Executor(flow, pkg).run()
        with self.assertRaises(PackageError):
            preview(path, "t", "s", [])


    def test_preview_never_parses_flow_json(self):
        # flow.json embeds every source row; preview must read only the
        # row-less preview.json structure. Deleting flow.json entirely must
        # not affect the preview.
        flow = load_fixture("preview_demo.json")
        pkg = self.build_pkg(flow, "noflowjson")
        os.remove(os.path.join(pkg, "flow.json"))
        corr = [
            {"op": "replace", "row_id": 1,
             "values": {"k": 200, "flag": 1, "note": "moved"}},
            {"op": "delete", "row_id": 3},
        ]
        res = preview(pkg, "top1", "src_lookup", corr)
        self.assertEqual([pt["partition"] for pt in res["partitions"]],
                         [["A"], ["B"]])


# --------------------------------------------------------------------------- #
# scale: fixed corrections, growing unrelated keys and partitions
# --------------------------------------------------------------------------- #
def scale_flow(n_extra):
    flow = load_fixture("preview_demo.json")
    records = next(s for s in flow["sources"] if s["name"] == "src_records")
    lookup = next(s for s in flow["sources"] if s["name"] == "src_lookup")
    for i in range(n_extra):
        # unrelated lookup keys that match nothing
        lookup["rows"].append({"row_id": 1000 + i,
                               "values": {"k": 70000 + i, "flag": 1, "note": "noise"}})
        # extra ranking partitions with a single released row
        records["rows"].append({"row_id": 5000 + i,
                                "values": {"rec": 5000 + i, "part": f"PX{i}",
                                           "lk": 80000 + i, "score": i, "tie": 1}})
        # extra ranking partitions with one blocked and one released row
        records["rows"].append({"row_id": 6000 + i,
                                "values": {"rec": 6000 + i, "part": f"PY{i}",
                                           "lk": 81000 + i, "score": 5, "tie": 1}})
        records["rows"].append({"row_id": 7000 + i,
                                "values": {"rec": 7000 + i, "part": f"PY{i}",
                                           "lk": 90000 + i, "score": 9, "tie": 2}})
        lookup["rows"].append({"row_id": 2000 + i,
                               "values": {"k": 90000 + i, "flag": 1, "note": "blocker"}})
    return flow


class TestScale(PreviewCase):
    CORRECTIONS = [
        {"op": "replace", "row_id": 1, "values": {"k": 200, "flag": 1, "note": "moved"}},
        {"op": "delete", "row_id": 3},
    ]

    def test_reads_stay_flat_as_unrelated_data_grows(self):
        pkg1 = self.build_pkg(scale_flow(50), "scale1")
        pkg2 = self.build_pkg(scale_flow(500), "scale2")

        # the packages really did grow: more partitions, more indexed keys
        con1 = duckdb.connect(os.path.join(pkg1, "evidence.duckdb"), read_only=True)
        con2 = duckdb.connect(os.path.join(pkg2, "evidence.duckdb"), read_only=True)
        size1 = con1.execute("SELECT (SELECT count(*) FROM preview_topk_parts), "
                             "(SELECT count(*) FROM preview_join_keys)").fetchone()
        size2 = con2.execute("SELECT (SELECT count(*) FROM preview_topk_parts), "
                             "(SELECT count(*) FROM preview_join_keys)").fetchone()
        self.assertGreater(size2[0], size1[0] * 5)
        self.assertGreater(size2[1], size1[1] * 5)

        log1, log2 = [], []
        r1 = self.run_preview(pkg1, "top1", "src_lookup", self.CORRECTIONS, sql_log=log1)
        r2 = self.run_preview(pkg2, "top1", "src_lookup", self.CORRECTIONS, sql_log=log2)

        # identical diffs at both scales
        self.assertEqual(preview_diff_shape(r1), preview_diff_shape(r2))
        self.assertEqual(len(r1["partitions"]), 2)
        # identical read/execution footprint: corrections and affected
        # partitions are fixed, so no read may grow with unrelated data
        self.assertEqual(r1["read_stats"], r2["read_stats"])
        self.assertEqual(len(log1), len(log2))
        self.assertEqual([e["rows"] for e in log1], [e["rows"] for e in log2])

        # plan-level proof: every preview read is an Index Scan, never a
        # Sequential Scan of a (growing) table
        self.assertTrue(log1)
        for entry in log1:
            plan = con2.execute("EXPLAIN ANALYZE " + entry["sql"],
                                entry["params"]).fetchall()[0][1]
            self.assertIn("Index Scan", plan, entry["sql"])
            self.assertNotIn("Sequential Scan", plan, entry["sql"])
        con1.close()
        con2.close()


# --------------------------------------------------------------------------- #
# additional shapes: multi-scan source, direct join->top_k, no partition_by,
# NULL partition keys, batch interaction, same-key value change, IN-list chunks
# --------------------------------------------------------------------------- #
def _anti_topk_flow(records, lookup, *, partition_by=("part",)):
    """Small anti-join -> project -> top_k flow generator."""
    rec_schema = {"rec": "int64", "part": "text", "lk": "int64",
                  "score": "int64", "tie": "int64"}
    cols = [{"name": "rec", "keep": "rec"},
            {"name": "part", "keep": "part"},
            {"name": "score", "keep": "score"},
            {"name": "tie", "keep": "tie"}]
    nodes = [
        {"id": "scan_rec", "op": "scan", "source": "rec_src"},
        {"id": "scan_lk", "op": "scan", "source": "lk_src"},
        {"id": "anti_j", "op": "join", "kind": "anti",
         "left": "scan_rec", "right": "scan_lk",
         "on": [{"left": "lk", "right": "k"}],
         "residual": {"kind": "cmp", "op": ">",
                      "left": {"kind": "col", "name": "r_flag"},
                      "right": {"kind": "lit", "value": 0}},
         "right_prefix": "r_"},
        {"id": "proj", "op": "project", "input": "anti_j", "columns": cols},
        {"id": "top", "op": "top_k", "input": "proj", "k": 2,
         "partition_by": list(partition_by),
         "order_by": [
             {"expr": {"kind": "col", "name": "score"}, "dir": "desc"},
             {"expr": {"kind": "col", "name": "tie"}, "dir": "asc",
              "decisive": True}]},
    ]
    return {
        "sources": [
            {"name": "rec_src", "schema": rec_schema, "rows": records},
            {"name": "lk_src", "schema": {"k": "int64", "flag": "int64"},
             "rows": lookup},
        ],
        "nodes": nodes,
        "sinks": ["top"],
    }


class TestExtraShapes(PreviewCase):
    def test_same_key_value_change_releases_left_row(self):
        # fixture-1 batch variant: row 1 keeps its key 100 but its flag drops
        # to 0, so its residual turns FALSE and rec3 is released
        flow = load_fixture("preview_demo.json")
        pkg = self.build_pkg(flow)
        corr = [{"op": "replace", "row_id": 1,
                 "values": {"k": 100, "flag": 0, "note": "demoted"}}]
        res = self.run_preview(pkg, "top1", "src_lookup", corr)
        self.assertEqual(preview_diff_shape(res), {
            ("A",): {
                "added": [({"part": "A", "rec": 3, "score": 45, "tie": 3}, 1)],
                "removed": [({"part": "A", "rec": 2, "score": 40, "tie": 2}, 1)],
            },
        })
        self.assert_matches_oracle(flow, "src_lookup", "top1", corr, res)

    def test_residual_may_reference_left_columns(self):
        # residual compares the lookup flag against the LEFT row's own score:
        # flag 30 > 50 is false (rec1 released); raising it to 60 blocks rec1
        flow = _anti_topk_flow(
            [{"row_id": 1, "values": {"rec": 1, "part": "R", "lk": 10, "score": 50, "tie": 1}},
             {"row_id": 2, "values": {"rec": 2, "part": "R", "lk": 9001, "score": 40, "tie": 2}}],
            [{"row_id": 1, "values": {"k": 10, "flag": 30}}],
        )
        next(n for n in flow["nodes"] if n["id"] == "anti_j")["residual"] = {
            "kind": "cmp", "op": ">",
            "left": {"kind": "col", "name": "r_flag"},
            "right": {"kind": "col", "name": "score"},
        }
        next(n for n in flow["nodes"] if n["id"] == "top")["k"] = 1
        pkg = self.build_pkg(flow)
        corr = [{"op": "replace", "row_id": 1, "values": {"k": 10, "flag": 60}}]
        res = self.run_preview(pkg, "top", "lk_src", corr)
        self.assertEqual(preview_diff_shape(res), {
            ("R",): {
                "added": [({"part": "R", "rec": 2, "score": 40, "tie": 2}, 1)],
                "removed": [({"part": "R", "rec": 1, "score": 50, "tie": 1}, 1)],
            },
        })
        self.assert_matches_oracle(flow, "lk_src", "top", corr, res)

    def test_batch_applies_jointly_swap_case(self):
        # row 1 moves onto row 2's key while row 2 is deleted: rec2 stays
        # blocked (by the moved row), rec1 is released — the batch is computed
        # jointly, not as sequential steps
        flow = _anti_topk_flow(
            [{"row_id": 1, "values": {"rec": 1, "part": "S", "lk": 10, "score": 50, "tie": 1}},
             {"row_id": 2, "values": {"rec": 2, "part": "S", "lk": 20, "score": 40, "tie": 2}}],
            [{"row_id": 1, "values": {"k": 10, "flag": 1}},
             {"row_id": 2, "values": {"k": 20, "flag": 1}}],
        )
        pkg = self.build_pkg(flow)
        corr = [{"op": "replace", "row_id": 1, "values": {"k": 20, "flag": 1}},
                {"op": "delete", "row_id": 2}]
        res = self.run_preview(pkg, "top", "lk_src", corr)
        self.assertEqual(preview_diff_shape(res), {
            ("S",): {
                "added": [({"part": "S", "rec": 1, "score": 50, "tie": 1}, 1)],
                "removed": [],
            },
        })
        self.assert_matches_oracle(flow, "lk_src", "top", corr, res)

    def test_join_directly_feeds_top_k_without_partitions(self):
        # no nodes between the join and the sink, and no partition_by:
        # the whole sink is a single partition, reported as []
        flow = _anti_topk_flow(
            [{"row_id": 1, "values": {"rec": 1, "part": "N", "lk": 10, "score": 50, "tie": 1}},
             {"row_id": 2, "values": {"rec": 2, "part": "N", "lk": 9001, "score": 40, "tie": 2}}],
            [{"row_id": 1, "values": {"k": 10, "flag": 1}}],
            partition_by=(),
        )
        # rewire: the anti join feeds the top_k directly (no project between)
        flow["nodes"] = [n for n in flow["nodes"] if n["id"] != "proj"]
        top = next(n for n in flow["nodes"] if n["id"] == "top")
        top["input"] = "anti_j"
        top["k"] = 1
        pkg = self.build_pkg(flow)
        corr = [{"op": "delete", "row_id": 1}]
        res = self.run_preview(pkg, "top", "lk_src", corr)
        self.assertEqual(preview_diff_shape(res), {
            (): {
                "added": [({"__row_id": 1, "rec": 1, "part": "N", "lk": 10,
                            "score": 50, "tie": 1}, 1)],
                "removed": [({"__row_id": 2, "rec": 2, "part": "N", "lk": 9001,
                              "score": 40, "tie": 2}, 1)],
            },
        })
        self.assertEqual(res["partitions"][0]["partition"], [])
        self.assert_matches_oracle(flow, "lk_src", "top", corr, res)

    def test_null_partition_key(self):
        flow = _anti_topk_flow(
            [{"row_id": 1, "values": {"rec": 1, "part": None, "lk": 10, "score": 50, "tie": 1}},
             {"row_id": 2, "values": {"rec": 2, "part": None, "lk": 9001, "score": 40, "tie": 2}}],
            [{"row_id": 1, "values": {"k": 10, "flag": 1}}],
        )
        next(n for n in flow["nodes"] if n["id"] == "top")["k"] = 1
        pkg = self.build_pkg(flow)
        corr = [{"op": "delete", "row_id": 1}]
        res = self.run_preview(pkg, "top", "lk_src", corr)
        self.assertEqual(preview_diff_shape(res), {
            (None,): {
                "added": [({"part": None, "rec": 1, "score": 50, "tie": 1}, 1)],
                "removed": [({"part": None, "rec": 2, "score": 40, "tie": 2}, 1)],
            },
        })
        self.assertEqual(res["partitions"][0]["partition"], [{"__null__": True}])
        self.assert_matches_oracle(flow, "lk_src", "top", corr, res)

    def test_source_scanned_twice_both_joins_affected(self):
        proj = lambda branch: [
            {"name": "part", "keep": "part"},
            {"name": "score", "keep": "score"},
            {"name": "tie", "keep": "tie"},
            {"name": "branch", "expr": {"kind": "lit", "value": branch}},
        ]
        flow = {
            "sources": [
                {"name": "rec_src", "schema": {"rec": "int64", "part": "text",
                                               "lk": "int64", "score": "int64",
                                               "tie": "int64"},
                 "rows": [
                     {"row_id": 1, "values": {"rec": 1, "part": "M", "lk": 10, "score": 50, "tie": 1}},
                     {"row_id": 2, "values": {"rec": 2, "part": "M", "lk": 20, "score": 40, "tie": 2}},
                     {"row_id": 3, "values": {"rec": 3, "part": "M", "lk": 9001, "score": 30, "tie": 3}},
                 ]},
                {"name": "lk_src", "schema": {"k": "int64", "flag": "int64"},
                 "rows": [
                     {"row_id": 1, "values": {"k": 10, "flag": 1}},
                     {"row_id": 2, "values": {"k": 20, "flag": 1}},
                 ]},
            ],
            "nodes": [
                {"id": "scan_rec", "op": "scan", "source": "rec_src"},
                {"id": "scan_lk_a", "op": "scan", "source": "lk_src"},
                {"id": "scan_lk_b", "op": "scan", "source": "lk_src"},
                {"id": "anti1", "op": "join", "kind": "anti",
                 "left": "scan_rec", "right": "scan_lk_a",
                 "on": [{"left": "lk", "right": "k"}],
                 "residual": {"kind": "cmp", "op": ">",
                              "left": {"kind": "col", "name": "r_flag"},
                              "right": {"kind": "lit", "value": 0}}},
                {"id": "anti2", "op": "join", "kind": "anti",
                 "left": "scan_rec", "right": "scan_lk_b",
                 "on": [{"left": "lk", "right": "k"}],
                 "residual": {"kind": "cmp", "op": ">",
                              "left": {"kind": "col", "name": "r_flag"},
                              "right": {"kind": "lit", "value": 0}}},
                {"id": "proj1", "op": "project", "input": "anti1", "columns": proj("b1")},
                {"id": "proj2", "op": "project", "input": "anti2", "columns": proj("b2")},
                {"id": "u", "op": "union_all", "inputs": ["proj1", "proj2"]},
                {"id": "top", "op": "top_k", "input": "u", "k": 2,
                 "partition_by": ["part"],
                 "order_by": [
                     {"expr": {"kind": "col", "name": "score"}, "dir": "desc"},
                     {"expr": {"kind": "col", "name": "tie"}, "dir": "asc",
                      "decisive": True},
                     {"expr": {"kind": "col", "name": "branch"}, "dir": "asc",
                      "decisive": True}]},
            ],
            "sinks": ["top"],
        }
        pkg = self.build_pkg(flow)
        # deleting lk row 1 releases rec1 through BOTH scans/joins at once
        corr = [{"op": "delete", "row_id": 1}]
        res = self.run_preview(pkg, "top", "lk_src", corr)
        self.assertEqual(sorted(res["affected_joins"]), ["anti1", "anti2"])
        self.assertEqual(preview_diff_shape(res), {
            ("M",): {
                "added": [
                    ({"part": "M", "score": 50, "tie": 1, "branch": "b1"}, 1),
                    ({"part": "M", "score": 50, "tie": 1, "branch": "b2"}, 1),
                ],
                "removed": [
                    ({"part": "M", "score": 30, "tie": 3, "branch": "b1"}, 1),
                    ({"part": "M", "score": 30, "tie": 3, "branch": "b2"}, 1),
                ],
            },
        })
        self.assert_matches_oracle(flow, "lk_src", "top", corr, res)

    def test_large_partition_exercises_in_list_chunking(self):
        records = [
            {"row_id": 1, "values": {"rec": 1, "part": "BIG", "lk": 10,
                                     "score": 100000, "tie": 9999}},
        ] + [
            {"row_id": 100 + i, "values": {"rec": 100 + i, "part": "BIG",
                                           "lk": 95000 + i, "score": i, "tie": i}}
            for i in range(649)
        ]
        flow = _anti_topk_flow(records, [{"row_id": 1, "values": {"k": 10, "flag": 1}}])
        next(n for n in flow["nodes"] if n["id"] == "top")["k"] = 3
        pkg = self.build_pkg(flow)
        corr = [{"op": "delete", "row_id": 1}]
        res = self.run_preview(pkg, "top", "lk_src", corr)
        self.assertEqual(preview_diff_shape(res), {
            ("BIG",): {
                "added": [({"part": "BIG", "rec": 1, "score": 100000, "tie": 9999}, 1)],
                "removed": [({"part": "BIG", "rec": 100 + 646, "score": 646, "tie": 646}, 1)],
            },
        })
        self.assert_matches_oracle(flow, "lk_src", "top", corr, res)


# --------------------------------------------------------------------------- #
# CLI end-to-end
# --------------------------------------------------------------------------- #
class TestCliPreview(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.pkg = os.path.join(self.tmp, "pkg")

    def _run(self, *args):
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(ROOT, "vendor", "duckdb_pkg")
        return subprocess.run(
            [sys.executable, "-I", AUDITOR, *args],
            capture_output=True, text=True, env=env, cwd=ROOT,
        )

    def _build(self):
        p = self._run("run", "--flow",
                      os.path.join(FIX, "preview_demo.json"), "--package", self.pkg)
        self.assertEqual(p.returncode, 0, p.stderr)

    def _write_corr(self, doc, name="corr.json"):
        path = os.path.join(self.tmp, name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(doc, f)
        return path

    def test_cli_preview_lifecycle(self):
        self._build()
        corr = self._write_corr({
            "source": "src_lookup",
            "corrections": [
                {"op": "replace", "row_id": 1,
                 "values": {"k": 200, "flag": 1, "note": "moved"}},
                {"op": "delete", "row_id": 3},
            ],
        })
        before = pkg_fingerprint(self.pkg)
        p = self._run("preview", "--package", self.pkg, "--sink", "top1",
                      "--corrections", corr)
        self.assertEqual(p.returncode, 0, p.stderr)
        out = json.loads(p.stdout)
        self.assertEqual(out["status"], "ok")
        self.assertEqual([pt["partition"] for pt in out["partitions"]],
                         [["A"], ["B"]])
        # deterministic: a second identical run prints the identical document
        p2 = self._run("preview", "--package", self.pkg, "--sink", "top1",
                       "--corrections", corr)
        self.assertEqual(p2.stdout, p.stdout)
        # package untouched by both previews
        self.assertEqual(before, pkg_fingerprint(self.pkg))

        # a copied package previews identically
        dest = os.path.join(self.tmp, "copy")
        p = self._run("copy-package", "--package", self.pkg, "--dest", dest)
        self.assertEqual(p.returncode, 0, p.stderr)
        p3 = self._run("preview", "--package", dest, "--sink", "top1",
                       "--corrections", corr)
        self.assertEqual(json.loads(p3.stdout)["partitions"],
                         json.loads(p2.stdout)["partitions"])

    def test_cli_rejections_and_unsupported(self):
        self._build()
        # non-existent row_id -> rejected, exit 2, nothing on stdout
        bad = self._write_corr({"source": "src_lookup",
                                "corrections": [{"op": "delete", "row_id": 777}]})
        p = self._run("preview", "--package", self.pkg, "--sink", "top1",
                      "--corrections", bad)
        self.assertEqual(p.returncode, 2)
        self.assertEqual(p.stdout, "")
        self.assertEqual(json.loads(p.stderr)["status"], "rejected")

        # sink not ending with top_k -> unsupported, exit 2
        ok = self._write_corr({"corrections": []}, "ok.json")
        p = self._run("preview", "--package", self.pkg, "--sink", "grp_other",
                      "--source", "src_lookup", "--corrections", ok)
        self.assertEqual(p.returncode, 2)
        err = json.loads(p.stderr)
        self.assertEqual(err["status"], "unsupported")
        self.assertIn("top_k", err["error"])

        # --source disagrees with the corrections file
        p = self._run("preview", "--package", self.pkg, "--sink", "top1",
                      "--source", "src_other", "--corrections", bad)
        self.assertEqual(p.returncode, 2)
        self.assertEqual(json.loads(p.stderr)["status"], "rejected")

    def test_cli_bare_array_corrections(self):
        self._build()
        corr = self._write_corr([{"op": "delete", "row_id": 3}])
        p = self._run("preview", "--package", self.pkg, "--sink", "top1",
                      "--source", "src_lookup", "--corrections", corr)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(json.loads(p.stdout)["partitions"], [])
        # a bare array without --source is a usage error, not a crash
        p = self._run("preview", "--package", self.pkg, "--sink", "top1",
                      "--corrections", corr)
        self.assertEqual(p.returncode, 2)
        self.assertEqual(json.loads(p.stderr)["status"], "rejected")

    def test_cli_old_package_explains_instead_of_rerunning(self):
        self._build()
        os.remove(os.path.join(self.pkg, "preview.json"))
        ok = self._write_corr({"corrections": []})
        p = self._run("preview", "--package", self.pkg, "--sink", "top1",
                      "--source", "src_lookup", "--corrections", ok)
        self.assertEqual(p.returncode, 2)
        err = json.loads(p.stderr)
        self.assertEqual(err["status"], "unsupported")
        self.assertIn("run", err["error"])  # tells the user to rebuild


if __name__ == "__main__":
    unittest.main(verbosity=2)
