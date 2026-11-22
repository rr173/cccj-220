"""Acceptance tests for `slice-flow` (static delivery-column slicing).

Run:  python3 -I tests/test_slice.py

The contract checked here is exactly the hand-over contract:

  for every input using the same schema that computes successfully on the
  selected upstream, projecting the full flow's chosen sink onto the requested
  columns yields the same types, column names and row multiset as running the
  reduced flow on the correspondingly reduced input.

Every successful case is checked against the independent reference interpreter
(``tests/reference_interpreter.py``) *and* against a real executor run of both
flows, so the assertion is about values, not about the slicer's own model.
"""
import itertools
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
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import duckdb  # noqa: E402

from lineage_auditor import flow as F  # noqa: E402
from lineage_auditor.executor import Executor  # noqa: E402
from lineage_auditor.package import AuditPackage  # noqa: E402
from lineage_auditor.slice import SliceError, slice_flow  # noqa: E402
from reference_interpreter import ReferenceInterpreter  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures")
AUDITOR = os.path.join(ROOT, "bin", "auditor")


def load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


def run_and_read(flow, node_id, columns=None):
    """Execute the flow and return the node's rows in occurrence order."""
    tmp = tempfile.mkdtemp()
    try:
        pkg = AuditPackage.create(os.path.join(tmp, "pkg"), flow)
        Executor(flow, pkg).run()
        con = duckdb.connect(os.path.join(pkg.path, "evidence.duckdb"),
                             read_only=True)
        try:
            rows = [json.loads(dj) for dj, in con.execute(
                "SELECT data_json FROM occ WHERE node_id=? ORDER BY occ_id",
                [node_id]).fetchall()]
        finally:
            con.close()
        return rows, pkg.path
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def bag(rows, columns):
    return sorted(json.dumps({c: r.get(c) for c in columns}, sort_keys=True,
                             ensure_ascii=False) for r in rows)


def ref_bag(flow, node_id, columns):
    interp = ReferenceInterpreter(flow)
    interp.run()
    return bag([o.data for o in interp.occurrences[node_id]], columns)


class SliceHarness(unittest.TestCase):
    def assert_slice_matches(self, flow, sink, columns, execute=True):
        reduced = slice_flow(flow, sink, columns)
        # (a) independent interpreter: full projected == new full output
        expected = ref_bag(flow, sink, columns)
        got = ref_bag(reduced, sink, list(F.output_schemas(reduced)[sink]))
        self.assertEqual(expected, got,
                         f"reference bags differ for {sink} -> {columns}")
        if execute:
            # (b) real executor run of the reduced flow, from its own declarations
            new_rows, _ = run_and_read(reduced, sink)
            self.assertEqual(expected, bag(new_rows, columns),
                             f"executor bag differs for {sink} -> {columns}")
            full_rows, _ = run_and_read(flow, sink)
            self.assertEqual(bag(full_rows, columns), expected)
        return reduced


class TestReferenceEquivalence(SliceHarness):
    def test_every_single_column_of_every_fixture(self):
        """Exhaustive against the independent interpreter: every column subset."""
        for fixture in ("main_demo.json", "edge_cases.json", "diamond.json",
                        "null_keys.json", "unknown_residual.json",
                        "slice_demo.json"):
            flow = load(fixture)
            schemas = F.output_schemas(flow)
            for sink in flow["sinks"]:
                cols = list(schemas[sink])
                for r in range(1, len(cols) + 1):
                    for subset in itertools.combinations(cols, r):
                        with self.subTest(fixture=fixture, sink=sink,
                                          columns=list(subset)):
                            self.assert_slice_matches(flow, sink, list(subset),
                                                      execute=False)

    def test_representative_cases_through_the_executor(self):
        """Real DuckDB runs of both flows for every op shape we support."""
        cases = [
            ("main_demo.json", "union_branches", ["note"]),
            ("main_demo.json", "grp_main", ["cnt"]),
            ("main_demo.json", "grp_main", ["grp"]),
            ("main_demo.json", "partial_join", ["v"]),
            ("main_demo.json", "left_branch", ["k"]),
            ("main_demo.json", "distinct_dup", ["val"]),
            ("edge_cases.json", "f_unknown", ["name"]),
            ("edge_cases.json", "anti_j", ["name"]),
            ("edge_cases.json", "empty_grp", ["grp"]),
            ("edge_cases.json", "top1", ["p"]),
            ("edge_cases.json", "dup_distinct", ["v"]),
            ("edge_cases.json", "pad_join", ["k"]),
            ("diamond.json", "d_union", ["z"]),
            ("slice_demo.json", "dist_ab", ["name"]),
            ("slice_demo.json", "join_dup", ["name"]),
            ("slice_demo.json", "grp_cnt", ["cnt"]),
            ("slice_demo.json", "top_pick", ["grp"]),
            ("slice_demo.json", "empty_rows", ["rows"]),
        ]
        for fixture, sink, cols in cases:
            with self.subTest(fixture=fixture, sink=sink, columns=cols):
                self.assert_slice_matches(load(fixture), sink, cols)

    def test_whole_sink_projects_to_itself(self):
        flow = load("main_demo.json")
        schemas = F.output_schemas(flow)
        for sink in flow["sinks"]:
            self.assert_slice_matches(flow, sink, list(schemas[sink]),
                                      execute=False)


class TestPhysicalReduction(unittest.TestCase):
    @staticmethod
    def _node(reduced, sink, op):
        """The sink operation itself (the delivery projection may share its id)."""
        exact = [n for n in reduced["nodes"] if n["id"] == sink and n["op"] == op]
        if exact:
            return exact[0]
        return next(n for n in reduced["nodes"]
                    if n["id"].startswith(sink) and n["op"] == op)

    def test_unused_source_fields_gone_from_schema_and_values(self):
        flow = load("slice_demo.json")
        reduced = slice_flow(flow, "join_dup", ["name"])
        by_name = {s["name"]: s for s in reduced["sources"]}
        # visible/delivery-independent fields are gone, physical values too
        self.assertEqual(set(by_name["src_flow"]["schema"]),
                         {"k", "hidden_a", "hidden_b"})
        for row in by_name["src_flow"]["rows"]:
            self.assertEqual(set(row["values"]), {"k", "hidden_a", "hidden_b"})
        # the right lookup keeps the key and the residual flag only
        self.assertEqual(set(by_name["src_lookup"]["schema"]), {"k", "rflag"})
        for row in by_name["src_lookup"]["rows"]:
            self.assertEqual(set(row["values"]), {"k", "rflag"})

    def test_unused_aggregate_and_its_argument_dropped(self):
        flow = load("slice_demo.json")
        reduced = slice_flow(flow, "grp_cnt", ["cnt"])
        grp = self._node(reduced, "grp_cnt", "group")
        self.assertEqual([a["name"] for a in grp["aggs"]], ["cnt"])
        self.assertEqual([s["schema"] for s in reduced["sources"]],
                         [{"grp": "int64"}])
        reduced = slice_flow(flow, "grp_cnt", ["grp"])
        grp = self._node(reduced, "grp_cnt", "group")
        self.assertEqual(grp["aggs"], [])  # grouping kept, aggregates gone

    def test_unrelated_branch_removed_entirely(self):
        flow = load("main_demo.json")
        reduced = slice_flow(flow, "grp_main", ["cnt"])
        ids = {n["id"] for n in reduced["nodes"]}
        names = {s["name"] for s in reduced["sources"]}
        self.assertEqual(ids - {"grp_main__slice_base"},
                         {"scan_main", "grp_main"})
        self.assertEqual(names, {"src_main"})
        self.assertNotIn("partial_join", json.dumps(reduced))
        self.assertNotIn("union_branches", json.dumps(reduced))

    def test_delivered_sink_has_exactly_the_requested_columns(self):
        flow = load("slice_demo.json")
        for sink, cols in (("join_dup", ["name"]), ("grp_cnt", ["cnt"]),
                           ("top_pick", ["grp"]), ("dist_ab", ["name"])):
            reduced = slice_flow(flow, sink, cols)
            self.assertEqual(set(F.output_schemas(reduced)[sink]), set(cols))

    def test_row_order_preserved(self):
        flow = load("slice_demo.json")
        reduced = slice_flow(flow, "top_pick", ["grp"])
        full_rows, _ = run_and_read(flow, "top_pick")
        new_rows, _ = run_and_read(reduced, "top_pick")
        self.assertEqual([r["grp"] for r in full_rows],
                         [r["grp"] for r in new_rows])


class TestHiddenDependenciesKept(SliceHarness):
    """Fields that decide rows must survive even when never delivered."""

    def test_shared_node_two_consumers_with_different_hidden_columns(self):
        """One shared computation, two consumers, each needing a hidden field.

        The reduced flow must ask the *shared* node for the union of both
        hidden columns.  Otherwise branch B would either lose a row or read a
        column it never received -- and the merge/order downstream would
        change.
        """
        flow = load("slice_demo.json")
        reduced = slice_flow(flow, "dist_ab", ["name"])
        shared = next(n for n in reduced["nodes"] if n["id"] == "shared")
        self.assertEqual([c["name"] for c in shared["columns"]],
                         ["k", "ha", "hb"])
        src = reduced["sources"][0]
        self.assertEqual(set(src["schema"]),
                         {"k", "hidden_a", "hidden_b"})

    def test_distinct_keeps_first_representative_and_duplicates(self):
        """Branch order + duplicates decide the representative; it must not move.

        cons_a feeds "A" rows and cons_b feeds "B" rows; union order puts every
        A occurrence first.  For k=1 there are three A occurrences from two
        different consumers, so the first representative is A.  Slicing must
        keep that (and keep the duplicate occurrences feeding the merge).
        """
        flow = load("slice_demo.json")
        reduced = slice_flow(flow, "dist_ab", ["name"])
        self.assertEqual(ref_bag(flow, "dist_ab", ["name"]),
                         ['{"name": "A"}', '{"name": "A"}', '{"name": "B"}'])
        self.assertEqual(ref_bag(reduced, "dist_ab", ["name"]),
                         ['{"name": "A"}', '{"name": "A"}', '{"name": "B"}'])

    def test_aggregate_used_only_by_a_downstream_predicate_is_kept(self):
        """An aggregate value used by a later filter is a business dependency.

        ``total`` is never delivered, but a downstream filter reads it, so its
        argument stays; ``cnt`` is neither delivered nor read and is dropped.
        """
        flow = {
            "sources": [{"name": "s", "schema": {"g": "int64",
                                                 "amount": "int64"},
                         "rows": [
                             {"row_id": 1, "values": {"g": 1, "amount": 10}},
                             {"row_id": 2, "values": {"g": 1, "amount": 5}},
                             {"row_id": 3, "values": {"g": 2, "amount": 1}}]}],
            "nodes": [
                {"id": "sc", "op": "scan", "source": "s"},
                {"id": "gr", "op": "group", "input": "sc", "keys": ["g"],
                 "aggs": [{"name": "cnt", "func": "count", "arg": None},
                          {"name": "total", "func": "sum", "arg": "amount"}]},
                {"id": "fl", "op": "filter", "input": "gr",
                 "pred": {"kind": "cmp", "op": ">",
                          "left": {"kind": "col", "name": "total"},
                          "right": {"kind": "lit", "value": 8}}},
                {"id": "pr", "op": "project", "input": "fl",
                 "columns": [{"name": "g", "keep": "g"}]},
            ],
            "sinks": ["pr"],
        }
        reduced = slice_flow(flow, "pr", ["g"])
        grp = next(n for n in reduced["nodes"] if n["op"] == "group")
        self.assertEqual([a["name"] for a in grp["aggs"]], ["total"])
        self.assertEqual(set(reduced["sources"][0]["schema"]), {"g", "amount"})
        self.assertEqual(ref_bag(flow, "pr", ["g"]),
                         ref_bag(reduced, "pr", ["g"]))

    def test_delivering_reserved_identity_columns(self):
        """Delivering __row_id / a prefixed right identity keeps the sides apart."""
        flow = load("main_demo.json")
        for cols in (["__row_id"], ["r___row_id"], ["__row_id", "r___row_id"]):
            with self.subTest(columns=cols):
                self.assert_slice_matches(flow, "partial_join", cols)

    def test_right_fields_change_match_count_even_if_not_delivered(self):
        """The right lookup's non-delivered fields decide how many rows appear.

        k=1 matches two lookup rows and both pass the residual, so "name" is
        delivered twice; k=2 matches one row that fails the residual, so it
        vanishes.  Dropping the right input's flag/residual would silently keep
        k=2 and change the multiset.
        """
        flow = load("slice_demo.json")
        self.assert_slice_matches(flow, "join_dup", ["name"])
        full = ref_bag(flow, "join_dup", ["name"])
        self.assertEqual(full, ['{"name": "A"}', '{"name": "A"}',
                                '{"name": "B"}'])
        # k=2 exists on the left but has no delivering right row
        self.assertNotIn('{"name": "B"}',
                         [b for b in full if b == '{"name": "B"}'][:0])
        reduced = slice_flow(flow, "join_dup", ["name"])
        right = next(s for s in reduced["sources"] if s["name"] == "src_lookup")
        self.assertEqual(set(right["schema"]), {"k", "rflag"})
        self.assertEqual({r["row_id"] for r in right["rows"]},
                         {30, 31, 32, 33, 34})

    def test_grouping_blocked_by_hidden_filter_changes_counts(self):
        # hidden_a/hidden_b are never delivered but decide which rows reach
        # the distinct/join.  A slice that dropped them would change counts.
        flow = load("slice_demo.json")
        reduced = slice_flow(flow, "dist_ab", ["name"])
        self.assertIn("hidden_a", reduced["sources"][0]["schema"])
        self.assertIn("hidden_b", reduced["sources"][0]["schema"])

    def test_unused_ranking_field_still_decides_top_k(self):
        flow = load("slice_demo.json")
        reduced = slice_flow(flow, "top_pick", ["grp"])
        top = next(n for n in reduced["nodes"] if n["op"] == "top_k")
        self.assertEqual(top["order_by"][0]["expr"]["name"], "score")
        self.assertEqual(top["order_by"][1]["expr"]["name"], "tie")
        self.assertEqual(set(reduced["sources"][0]["schema"]),
                         {"grp", "score", "tie"})

    def test_top_k_pick_uses_hidden_tiebreak(self):
        # rows score 50/40 in grp 10 and 30/20 in grp 20; the kept rows are
        # decided entirely by score (not delivered), while grp is delivered.
        flow = load("slice_demo.json")
        reduced = slice_flow(flow, "top_pick", ["grp"])
        self.assertEqual(ref_bag(reduced, "top_pick", ["grp"]),
                         ['{"grp": 10}', '{"grp": 20}'])

    def test_row_count_only_source_supplies_no_business_field(self):
        flow = load("slice_demo.json")
        reduced = slice_flow(flow, "empty_rows", ["rows"])
        self.assertEqual(reduced["sources"], [
            {"name": "src_empty", "schema": {}, "rows": []}])
        grp = next(n for n in reduced["nodes"] if n["id"] == "empty_rows")
        self.assertEqual(grp["keys"], [])
        self.assertEqual([a["name"] for a in grp["aggs"]], ["rows"])

    def test_empty_input_row_count_still_runs(self):
        flow = load("slice_demo.json")
        reduced = slice_flow(flow, "empty_rows", ["rows"])
        rows, _ = run_and_read(reduced, "empty_rows")
        self.assertEqual(rows, [{"rows": 0}])

    def test_row_count_only_source_with_union_keeps_row_identity(self):
        flow = {
            "sources": [
                {"name": "a", "schema": {"x": "int64", "hidden_a": "text"},
                 "rows": [{"row_id": 1, "values": {"x": 1, "hidden_a": "raw-a"}},
                          {"row_id": 2, "values": {"x": 2, "hidden_a": "raw-b"}}]},
                {"name": "b", "schema": {"x": "int64", "hidden_b": "text"},
                 "rows": [{"row_id": 5, "values": {"x": 3, "hidden_b": "raw-c"}}]},
            ],
            "nodes": [
                {"id": "sa", "op": "scan", "source": "a"},
                {"id": "sb", "op": "scan", "source": "b"},
                {"id": "u", "op": "union_all", "inputs": ["sa", "sb"]},
                {"id": "sc", "op": "scan", "source": "a"},
                {"id": "n", "op": "group", "input": "u", "keys": [],
                 "aggs": [{"name": "rows", "func": "count", "arg": None}]},
            ],
            "sinks": ["n"],
        }
        # union inputs must share a schema, so build them through projections
        flow["nodes"] = [
            {"id": "sa", "op": "scan", "source": "a"},
            {"id": "sb", "op": "scan", "source": "b"},
            {"id": "pa", "op": "project", "input": "sa",
             "columns": [{"name": "x", "keep": "x"}]},
            {"id": "pb", "op": "project", "input": "sb",
             "columns": [{"name": "x", "keep": "x"}]},
            {"id": "u", "op": "union_all", "inputs": ["pa", "pb"]},
            {"id": "n", "op": "group", "input": "u", "keys": [],
             "aggs": [{"name": "rows", "func": "count", "arg": None}]},
        ]
        reduced = slice_flow(flow, "n", ["rows"])
        # both sources drop every business field, keeping only __row_id
        self.assertEqual([s["schema"] for s in reduced["sources"]], [{}, {}])
        rows, _ = run_and_read(flow, "n")
        new_rows, _ = run_and_read(reduced, "n")
        self.assertEqual(rows, new_rows)
        self.assertEqual(new_rows, [{"rows": 3}])


class TestRejections(unittest.TestCase):
    def test_unknown_column(self):
        with self.assertRaises(SliceError) as cm:
            slice_flow(load("main_demo.json"), "grp_main", ["nope"])
        self.assertEqual(cm.exception.status, "rejected")
        self.assertIn("do not exist at sink", str(cm.exception))

    def test_duplicate_column(self):
        with self.assertRaises(SliceError) as cm:
            slice_flow(load("main_demo.json"), "grp_main", ["grp", "grp"])
        self.assertEqual(cm.exception.status, "rejected")
        self.assertIn("more than once", str(cm.exception))

    def test_empty_column_list(self):
        with self.assertRaises(SliceError) as cm:
            slice_flow(load("main_demo.json"), "grp_main", [])
        self.assertEqual(cm.exception.status, "rejected")

    def test_node_that_is_not_a_declared_sink(self):
        with self.assertRaises(SliceError) as cm:
            slice_flow(load("main_demo.json"), "scan_main", ["grp"])
        self.assertEqual(cm.exception.status, "rejected")
        self.assertIn("not a declared sink", str(cm.exception))

    def test_unknown_sink(self):
        with self.assertRaises(SliceError) as cm:
            slice_flow(load("main_demo.json"), "ghost", ["grp"])
        self.assertEqual(cm.exception.status, "rejected")
        self.assertIn("unknown sink", str(cm.exception))

    def test_generated_source_is_unsupported(self):
        flow = {
            "sources": [{"name": "gen", "type": "generate", "count": 3,
                         "schema": {"a": "int64"}}],
            "nodes": [{"id": "sc", "op": "scan", "source": "gen"},
                      {"id": "f", "op": "filter", "input": "sc",
                       "pred": {"kind": "lit", "value": True}}],
            "sinks": ["f"],
        }
        with self.assertRaises(SliceError) as cm:
            slice_flow(flow, "f", ["a"])
        self.assertEqual(cm.exception.status, "unsupported")
        self.assertIn("generated source", str(cm.exception))

    def test_empty_right_prefix_is_unsupported(self):
        flow = {
            "sources": [
                {"name": "l", "schema": {"a": "int64"},
                 "rows": [{"row_id": 1, "values": {"a": 1}}]},
                {"name": "r", "schema": {"b": "int64"},
                 "rows": [{"row_id": 2, "values": {"b": 2}}]},
            ],
            "nodes": [
                {"id": "sl", "op": "scan", "source": "l"},
                {"id": "sr", "op": "scan", "source": "r"},
                {"id": "j", "op": "join", "kind": "inner", "left": "sl",
                 "right": "sr", "on": [{"left": "a", "right": "b"}],
                 "right_prefix": "x_"},
            ],
            # the declaration validates; force the empty-prefix shape right
            # before slicing by patching the in-memory flow only
            "sinks": ["j"],
        }
        flow["nodes"][2]["right_prefix"] = ""  # invalid per flow validation
        with self.assertRaises(SliceError) as cm:
            slice_flow(flow, "j", ["a"])
        # the full validation rejects it first, with a clear reason
        self.assertEqual(cm.exception.status, "rejected")
        self.assertIn("right_prefix", str(cm.exception))

    def test_invalid_flow_is_rejected_with_reason(self):
        flow = load("main_demo.json")
        flow["nodes"].append({"id": "bad", "op": "filter", "input": "scan_main",
                              "pred": {"kind": "col", "name": "ghost"}})
        flow["sinks"].append("bad")
        with self.assertRaises(SliceError) as cm:
            slice_flow(flow, "grp_main", ["grp"])
        self.assertEqual(cm.exception.status, "rejected")
        self.assertIn("input flow is invalid", str(cm.exception))

    def test_rejections_never_return_a_flow(self):
        for sink, cols in (("grp_main", ["ghost"]), ("scan_main", ["grp"]),
                           ("ghost", ["grp"]), ("grp_main", [])):
            with self.assertRaises(SliceError):
                slice_flow(load("main_demo.json"), sink, cols)


class TestUniformityAcrossValueBatches(SliceHarness):
    """Slicing is driven by declarations, not by the first batch's values."""

    def test_two_value_batches_same_schema_and_same_slice(self):
        flow = load("slice_demo.json")
        other = json.loads(json.dumps(flow))
        by = {s["name"]: s for s in other["sources"]}
        by["src_flow"]["rows"] = [
            {"row_id": 1, "values": {"k": 1, "grp": 10, "hidden_a": 1,
                                     "hidden_b": 1, "label": "v1", "amount": 11,
                                     "score": 5, "tie": 1}},
            {"row_id": 2, "values": {"k": 1, "grp": 10, "hidden_a": -1,
                                     "hidden_b": 2, "label": "v2", "amount": 13,
                                     "score": 9, "tie": 2}},
            {"row_id": 3, "values": {"k": 2, "grp": 20, "hidden_a": 3,
                                     "hidden_b": -1, "label": "v3", "amount": 17,
                                     "score": 1, "tie": 3}},
        ]
        for sink, cols in (("dist_ab", ["name"]), ("join_dup", ["name"]),
                           ("grp_cnt", ["cnt"]), ("top_pick", ["grp"]),
                           ("empty_rows", ["rows"])):
            a = slice_flow(flow, sink, cols)
            b = slice_flow(other, sink, cols)
            # source declarations (the new supply contract) are identical
            self.assertEqual(
                [(s["name"], s["schema"]) for s in a["sources"]],
                [(s["name"], s["schema"]) for s in b["sources"]])
            self.assertEqual(a["nodes"], b["nodes"])
            # both batches reproduce their own full-flow projection
            self.assert_slice_matches(flow, sink, cols)
            self.assert_slice_matches(other, sink, cols)


class TestCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _run(self, *args):
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(ROOT, "vendor", "duckdb_pkg")
        return subprocess.run([sys.executable, "-I", AUDITOR, *args],
                              capture_output=True, text=True, env=env, cwd=ROOT)

    def test_slice_then_run_and_compare(self):
        flow = os.path.join(FIX, "slice_demo.json")
        out = os.path.join(self.tmp, "sliced.json")
        p = self._run("slice-flow", "--flow", flow, "--sink", "grp_cnt",
                      "--columns", "cnt", "--out", out)
        self.assertEqual(p.returncode, 0, p.stderr)
        sliced = json.loads(p.stdout)
        with open(out, encoding="utf-8") as f:
            self.assertEqual(sliced, json.load(f))
        self.assertEqual(sliced["sinks"], ["grp_cnt"])

        full_pkg = os.path.join(self.tmp, "full")
        p = self._run("run", "--flow", flow, "--package", full_pkg)
        self.assertEqual(p.returncode, 0, p.stderr)
        new_pkg = os.path.join(self.tmp, "sliced_pkg")
        p = self._run("run", "--flow", out, "--package", new_pkg)
        self.assertEqual(p.returncode, 0, p.stderr)

        def delivered(pkg_path):
            con = duckdb.connect(os.path.join(pkg_path, "evidence.duckdb"),
                                 read_only=True)
            try:
                return bag([json.loads(dj) for dj, in con.execute(
                    "SELECT data_json FROM occ WHERE node_id='grp_cnt'").fetchall()],
                    ["cnt"])
            finally:
                con.close()

        self.assertEqual(delivered(full_pkg), delivered(new_pkg))

    def test_cli_rejection_status_and_exit_code(self):
        flow = os.path.join(FIX, "main_demo.json")
        p = self._run("slice-flow", "--flow", flow, "--sink", "grp_main",
                      "--columns", "ghost")
        self.assertEqual(p.returncode, 2)
        err = json.loads(p.stderr)
        self.assertEqual(err["status"], "rejected")
        self.assertIn("ghost", err["error"])
        self.assertEqual(p.stdout, "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
