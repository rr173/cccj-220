"""Acceptance tests: production evidence vs. the independent interpreter.

Run:  python3 -I tests/test_acceptance.py

The reference interpreter (tests/reference_interpreter.py) never imports the
production package or evidence builder; these tests therefore validate the
DuckDB-backed executor against an independent occurrence-level semantics.
"""
import json
import os
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
from lineage_auditor.audit import AuditReader  # noqa: E402
from reference_interpreter import ReferenceInterpreter  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures")


def load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


class FlowHarness:
    def __init__(self, flow):
        self.flow = flow
        self.tmp = tempfile.mkdtemp()
        self.pkg_path = os.path.join(self.tmp, "pkg")
        self.pkg = AuditPackage.create(self.pkg_path, flow)
        Executor(flow, self.pkg).run()
        self.con = duckdb.connect(os.path.join(self.pkg_path, "evidence.duckdb"),
                                  read_only=True)
        self.ref = ReferenceInterpreter(flow)
        self.ref.run()

    def close(self):
        self.con.close()


def business_bag(rows):
    """Order-insensitive multiset of business rows."""
    return sorted(json.dumps(r, sort_keys=True) for r in rows)


class TestReferenceEquivalence(unittest.TestCase):
    def _assert_flow_matches_reference(self, fixture):
        flow = load(fixture)
        h = FlowHarness(flow)
        try:
            ref_bags = h.ref.business_bags()
            for nid, ref_rows in ref_bags.items():
                prod = h.con.execute(
                    "SELECT data_json FROM occ WHERE node_id=? ORDER BY occ_id", [nid]
                ).fetchall()
                prod_rows = [json.loads(dj) for dj, in prod]
                self.assertEqual(
                    business_bag(prod_rows), business_bag(ref_rows),
                    f"business bag mismatch at {nid} in {fixture}",
                )
            # occurrence ids must line up 1:1
            for nid, ref_occs in h.ref.occurrences.items():
                prod_n = h.con.execute(
                    "SELECT count(*) FROM occ WHERE node_id=?", [nid]
                ).fetchone()[0]
                self.assertEqual(prod_n, len(ref_occs),
                                 f"occurrence count mismatch at {nid}")
            # audit edges
            prod_edges = sorted(
                h.con.execute(
                    "SELECT from_node, from_occ, to_node, to_occ, role FROM edges"
                ).fetchall()
            )
            self.assertEqual(prod_edges, h.ref.expected_edges(),
                             f"edge mismatch in {fixture}")
            # rejects (reason codes, not full detail)
            prod_rej = sorted(
                h.con.execute(
                    "SELECT node_id, input_node, input_occ, reason_code FROM rejects"
                ).fetchall()
            )
            self.assertEqual(prod_rej, h.ref.expected_rejects(),
                             f"reject mismatch in {fixture}")
            # participation
            prod_part = sorted(
                h.con.execute(
                    "SELECT node_id, group_occ, member_node, member_occ, "
                    "agg_name, in_count, in_sum FROM participation"
                ).fetchall()
            )
            self.assertEqual(prod_part, h.ref.expected_participation(),
                             f"participation mismatch in {fixture}")
        finally:
            h.close()

    def test_main_demo(self):
        self._assert_flow_matches_reference("main_demo.json")

    def test_edge_cases(self):
        self._assert_flow_matches_reference("edge_cases.json")

    def test_null_keys(self):
        self._assert_flow_matches_reference("null_keys.json")

    def test_unknown_residual(self):
        self._assert_flow_matches_reference("unknown_residual.json")

    def test_diamond_dag(self):
        self._assert_flow_matches_reference("diamond.json")


class TestSpecificSemantics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.h = FlowHarness(load("edge_cases.json"))

    @classmethod
    def tearDownClass(cls):
        cls.h.close()

    def test_filter_unknown_distinguished(self):
        # row 1 amount NULL -> unknown; row 2 amount 5 -> true; rows 3,4 -> true
        reasons = {
            r[0]: r[1] for r in self.h.con.execute(
                "SELECT input_occ, reason_code FROM rejects WHERE node_id='f_unknown'"
            ).fetchall()
        }
        self.assertEqual(reasons, {0: "filter_unknown"})
        kept = [json.loads(dj)["__row_id"] for dj, in self.h.con.execute(
            "SELECT data_json FROM occ WHERE node_id='f_unknown' ORDER BY occ_id"
        ).fetchall()]
        self.assertEqual(kept, [2, 3, 4])

    def test_three_valued_logic_unit(self):
        from lineage_auditor import expr as E
        row = {"x": None}
        self.assertIsNone(E.eval_predicate(
            E.cmp("=", E.col("x"), E.lit(None)), row))
        self.assertIsNone(E.eval_predicate(
            E.cmp("=", E.col("x"), E.lit(1)), row))
        self.assertTrue(E.eval_predicate(E.is_null(E.col("x")), row))
        self.assertIsNone(E.eval_predicate(
            E.and_(E.cmp(">", E.col("x"), E.lit(1)),
                   E.cmp("<", E.col("x"), E.lit(9))), row))
        self.assertTrue(E.eval_predicate(
            E.or_(E.cmp(">", E.col("x"), E.lit(1)),
                  E.is_null(E.col("x"))), row))
        self.assertIsNone(E.eval_predicate(
            E.not_(E.cmp(">", E.col("x"), E.lit(1))), row))

    def test_anti_witness(self):
        rej = self.h.con.execute(
            "SELECT input_occ, reason_code, detail_json FROM rejects "
            "WHERE node_id='anti_j'"
        ).fetchall()
        # only grp=2 (row 4, occ 3) has a real match -> blocked with a witness
        self.assertEqual(len(rej), 1)
        self.assertEqual(rej[0][1], "anti_join_matched_witness")
        detail = json.loads(rej[0][2])
        self.assertEqual(detail["witness_right_occ"], 0)
        # anti output keeps the other three left rows
        kept = [json.loads(dj)["__row_id"] for dj, in self.h.con.execute(
            "SELECT data_json FROM occ WHERE node_id='anti_j'"
        ).fetchall()]
        self.assertEqual(sorted(kept), [1, 2, 3])

    def test_empty_aggregate(self):
        rows = self.h.con.execute(
            "SELECT data_json FROM occ WHERE node_id='empty_grp'"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0][0]),
                         {"grp": None, "cnt": 0, "amt": None})

    def test_union_duplicates_kept_then_distinct_merged(self):
        union_n = self.h.con.execute(
            "SELECT count(*) FROM occ WHERE node_id='dup_union'"
        ).fetchone()[0]
        self.assertEqual(union_n, 4)  # two branches x two rows, v=9 repeated
        distinct_n = self.h.con.execute(
            "SELECT count(*) FROM occ WHERE node_id='dup_distinct'"
        ).fetchone()[0]
        self.assertEqual(distinct_n, 1)
        merge_edges = self.h.con.execute(
            "SELECT count(*) FROM edges WHERE to_node='dup_distinct'"
        ).fetchone()[0]
        self.assertEqual(merge_edges, 4)  # all four occurrences still contribute

    def test_top_k_decisive_tiebreak_and_cutoff(self):
        # partition p=1: score 5 tie between rows 401(tie1)/402(tie2); tie asc -> 401
        kept = [json.loads(dj)["__row_id"] for dj, in self.h.con.execute(
            "SELECT data_json FROM occ WHERE node_id='top1' ORDER BY occ_id"
        ).fetchall()]
        self.assertEqual(kept, [401, 404])
        rej = self.h.con.execute(
            "SELECT input_occ, detail_json FROM rejects WHERE node_id='top1' "
            "ORDER BY input_occ"
        ).fetchall()
        self.assertEqual(len(rej), 2)
        detail0 = json.loads(rej[0][1])
        self.assertEqual(detail0["k"], 1)
        # full ordering key is present in the reject detail
        self.assertEqual([t["expr_sql"] for t in detail0["full_ordering"]][:2],
                         ['"score"', '"tie"'])
        self.assertIsNotNone(detail0["cutoff_last_kept"])

    def test_padded_value_looks_like_source_but_is_distinct(self):
        # left row carries its own r_k column that is genuinely NULL; the left
        # join pads an unrelated right side with NULLs too. Padding flag must
        # distinguish "no match" from the source's own NULL value.
        rows = self.h.con.execute(
            "SELECT data_json, padded FROM occ WHERE node_id='pad_join'"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        data, padded = json.loads(rows[0][0]), rows[0][1]
        self.assertTrue(padded)
        self.assertIsNone(data["own_null"])       # source's own value, genuinely null
        self.assertIsNone(data["r___row_id"])     # join-padded null
        self.assertIsNone(data["r_k"])            # join-padded null (no match)
        # evidence says the padding came from the join, not the data
        roles = [r[0] for r in self.h.con.execute(
            "SELECT role FROM edges WHERE to_node='pad_join'"
        ).fetchall()]
        self.assertEqual(roles, ["left_padded"])


class TestUnknownResidualAndAnti(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.h = FlowHarness(load("unknown_residual.json"))

    @classmethod
    def tearDownClass(cls):
        cls.h.close()

    def test_inner_reasons_split_by_candidate_and_unknown(self):
        rej = {r[0]: (r[1], json.loads(r[2])) for r in self.h.con.execute(
            "SELECT input_occ, reason_code, detail_json FROM rejects "
            "WHERE node_id='u_inner_resid' ORDER BY input_occ"
        ).fetchall()}
        # row 1: NULL join key -> no equal-key candidate at all
        self.assertEqual(rej[0][0], "inner_join_no_equal_key_candidate")
        # rows 2,3 have the candidate; row 2 residual NULL<100 -> unknown, fails
        self.assertEqual(rej[1][0], "inner_join_residual_failed")
        self.assertEqual(
            rej[1][1]["candidate_residual_truth"][0]["truth"], "unknown")
        # row 3: 5<100 true -> passes, no reject
        self.assertNotIn(2, rej)
        kept = [json.loads(dj)["__row_id"] for dj, in self.h.con.execute(
            "SELECT data_json FROM occ WHERE node_id='u_inner_resid'"
        ).fetchall()]
        self.assertEqual(kept, [3])

    def test_anti_null_key_survives_and_real_match_blocked(self):
        # row 1 NULL key finds no candidate -> survives anti (no witness possible)
        # rows 2,3 share key 1 which matches the right side -> blocked with witness
        rej = self.h.con.execute(
            "SELECT input_occ, reason_code FROM rejects WHERE node_id='u_anti' "
            "ORDER BY input_occ"
        ).fetchall()
        self.assertEqual([r[0] for r in rej], [1, 2])
        self.assertTrue(all(r[1] == "anti_join_matched_witness" for r in rej))
        kept = sorted(json.loads(dj)["__row_id"] for dj, in self.h.con.execute(
            "SELECT data_json FROM occ WHERE node_id='u_anti'"
        ).fetchall())
        self.assertEqual(kept, [1])


class TestPaddedPropagation(unittest.TestCase):
    def test_padded_flag_survives_downstream_filter(self):
        flow = {
            "sources": [
                {"name": "l", "schema": {"k": "int64", "ok": "bool"},
                 "rows": [{"row_id": 1, "values": {"k": 1, "ok": True}}]},
                {"name": "r", "schema": {"k": "int64"},
                 "rows": [{"row_id": 2, "values": {"k": 2}}]},
            ],
            "nodes": [
                {"id": "sl", "op": "scan", "source": "l"},
                {"id": "sr", "op": "scan", "source": "r"},
                {"id": "lj", "op": "join", "kind": "left", "left": "sl",
                 "right": "sr", "on": [{"left": "k", "right": "k"}],
                 "right_prefix": "r_"},
                {"id": "ff", "op": "filter", "input": "lj",
                 "pred": {"kind": "col", "name": "ok"}},
            ],
            "sinks": ["ff"],
        }
        h = FlowHarness(flow)
        try:
            padded = h.con.execute(
                "SELECT padded FROM occ WHERE node_id='ff'").fetchone()[0]
            self.assertTrue(padded)
            role = h.con.execute(
                "SELECT role FROM edges WHERE to_node='lj'").fetchone()[0]
            self.assertEqual(role, "left_padded")
        finally:
            h.close()


class TestDiamondDag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.h = FlowHarness(load("diamond.json"))

    @classmethod
    def tearDownClass(cls):
        cls.h.close()

    def test_occurrence_counting_dedups_edge_paths(self):
        with AuditReader(self.h.pkg) as rd:
            # forward: one source row fans into two join branches that meet at
            # the union sink; the source occurrence must be counted once
            t = rd.trace_source("d_src", 1, ["d_union"], page_size=10, page=0)
            self.assertEqual(t["surviving_outputs"]["total"], 2)
            # reverse: each sink occurrence's lineage is an occurrence set,
            # not a multiset of edge paths
            up0 = rd.trace_output("d_union", 0)
            up1 = rd.trace_output("d_union", 1)
        ids0 = {(x["node_id"], x["occ_id"]) for x in up0["lineage_page"]}
        # the shared scan occurrence d_scan#0 appears in both, never twice in one
        self.assertIn(("d_scan", 0), ids0)
        self.assertEqual(len(ids0), len(up0["lineage_page"]))
        # totals stay stable under pagination
        with AuditReader(self.h.pkg) as rd:
            full = rd.trace_output("d_union", 0, page_size=100, page=0)
        self.assertEqual(full["total_upstream_occurrences"],
                         len({(x["node_id"], x["occ_id"])
                              for x in full["lineage_page"]}))
        self.assertEqual(up1["total_upstream_occurrences"],
                         full["total_upstream_occurrences"])


class TestNullKeySemantics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.h = FlowHarness(load("null_keys.json"))

    @classmethod
    def tearDownClass(cls):
        cls.h.close()

    def test_null_never_equals_null_in_join(self):
        # only the fully non-null matching pair (1,y) survives inner
        inner = [json.loads(dj)["__row_id"] for dj, in self.h.con.execute(
            "SELECT data_json FROM occ WHERE node_id='nl_inner'"
        ).fetchall()]
        self.assertEqual(inner, [3])
        # left join keeps all three left rows; two are null-padded
        left_n = self.h.con.execute(
            "SELECT count(*) FROM occ WHERE node_id='nl_left'"
        ).fetchone()[0]
        padded_n = self.h.con.execute(
            "SELECT count(*) FROM occ WHERE node_id='nl_left' AND padded"
        ).fetchone()[0]
        self.assertEqual((left_n, padded_n), (3, 2))
        # anti join: the two null-key rows find no equal-key candidate -> survive
        anti = sorted(json.loads(dj)["__row_id"] for dj, in self.h.con.execute(
            "SELECT data_json FROM occ WHERE node_id='nl_anti'"
        ).fetchall())
        self.assertEqual(anti, [1, 2])
        rej = self.h.con.execute(
            "SELECT count(*) FROM rejects WHERE node_id='nl_anti'"
        ).fetchone()[0]
        self.assertEqual(rej, 1)


class TestGroupParticipation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.h = FlowHarness(load("main_demo.json"))

    @classmethod
    def tearDownClass(cls):
        cls.h.close()

    def test_sum_zero_membership(self):
        with AuditReader(self.h.pkg) as rd:
            p = rd.aggregate_participation("grp_main", 0)
        cnt = p["aggregates"]["cnt"]["members"]
        amt = p["aggregates"]["amt_sum"]["members"]
        self.assertEqual(len(cnt), 3)
        self.assertTrue(all(m["in_count"] for m in cnt))
        in_sum = [(m["member_occ"], m["in_sum"], m["sum_contribution"]) for m in amt]
        self.assertEqual(in_sum, [(0, True, 2), (1, True, -2), (2, False, None)])

    def test_trace_distinguishes_contribution_from_loss(self):
        with AuditReader(self.h.pkg) as rd:
            t = rd.trace_source("src_main", 3, ["grp_main"])
        self.assertEqual(t["verdict"], "contributing")
        self.assertEqual(t["surviving_outputs"]["rows"][0]["row"]["amt_sum"], 0)
        # row c's null excludes it from sum but not from the group
        merges = [m for m in t["merge_destinations"] if m["node_id"] == "grp_main"]
        self.assertEqual(merges[0]["total_contributing_input_occurrences"], 3)


class TestBranchFate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.h = FlowHarness(load("main_demo.json"))

    @classmethod
    def tearDownClass(cls):
        cls.h.close()

    def test_one_source_row_blocked_and_another_kept(self):
        with AuditReader(self.h.pkg) as rd:
            # row 1 (a): blocked at f_gt2 on the union path, contributes at grp
            ta = rd.trace_source("src_main", 1,
                                 ["grp_main", "union_branches", "partial_join"])
            self.assertEqual(ta["verdict"],
                             "partially_contributing_some_branches_blocked")
            blockers = {(b["blocked_at_node"], b["reason_code"])
                        for b in ta["first_blockers"]}
            self.assertIn(("f_gt2", "filter_false"), blockers)
            fate = {b["sink"]: b for b in ta["branch_fate"]}
            self.assertEqual(fate["grp_main"]["status"], "contributes")
            self.assertEqual(fate["union_branches"]["status"],
                             "blocked_before_sink")
            self.assertEqual(
                fate["union_branches"]["first_blocker_reasons_on_its_paths"],
                ["filter_false"])
            self.assertEqual(
                fate["grp_main"]["first_blocker_reasons_on_its_paths"], [])

            # left-join row reaches the union sink via padding, no blocker reported
            tl = rd.trace_source("src_lj", 40, ["union_branches"])
            self.assertEqual(tl["verdict"], "contributing")
            self.assertEqual(tl["first_blockers"], [])
            self.assertTrue(tl["surviving_outputs"]["rows"][0]["left_join_padded"])

    def test_partial_match_is_not_whole_row_loss(self):
        with AuditReader(self.h.pkg) as rd:
            t = rd.trace_source("src_partial", 20, ["partial_join"])
        self.assertEqual(t["verdict"], "contributing")
        self.assertEqual(len(t["surviving_outputs"]["rows"]), 1)
        # the failed candidate is documented nowhere as a reject of the left row;
        # the join node has zero rejects because one candidate passed
        rej = self.h.con.execute(
            "SELECT count(*) FROM rejects WHERE node_id='partial_join'"
        ).fetchone()[0]
        self.assertEqual(rej, 0)


class TestPaginationAndReverseTrace(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.h = FlowHarness(load("main_demo.json"))

    @classmethod
    def tearDownClass(cls):
        cls.h.close()

    def test_output_pagination(self):
        with AuditReader(self.h.pkg) as rd:
            p0 = rd.trace_output("partial_join", 0, page_size=1, page=0)
            p1 = rd.trace_output("partial_join", 0, page_size=1, page=1)
            p2 = rd.trace_output("partial_join", 0, page_size=1, page=2)
            p3 = rd.trace_output("partial_join", 0, page_size=1, page=3)
        # total includes the sink occurrence itself + its two parent scans
        self.assertEqual(p0["total_upstream_occurrences"], 3)
        self.assertEqual(len(p0["lineage_page"]), 1)
        self.assertEqual(len(p1["lineage_page"]), 1)
        self.assertEqual(len(p2["lineage_page"]), 1)
        self.assertEqual(len(p3["lineage_page"]), 0)
        self.assertNotEqual(p0["lineage_page"][0]["node_id"],
                            p1["lineage_page"][0]["node_id"])

    def test_source_pagination(self):
        with AuditReader(self.h.pkg) as rd:
            t = rd.trace_source("src_lj", 40,
                                ["left_branch", "union_branches"],
                                page_size=1, page=0)
            t2 = rd.trace_source("src_lj", 40,
                                 ["left_branch", "union_branches"],
                                 page_size=1, page=1)
        self.assertEqual(t["surviving_outputs"]["total"], 2)
        self.assertEqual(len(t["surviving_outputs"]["rows"]), 1)
        self.assertEqual(len(t2["surviving_outputs"]["rows"]), 1)
        self.assertNotEqual(t["surviving_outputs"]["rows"][0]["sink"],
                            t2["surviving_outputs"]["rows"][0]["sink"])


class TestPreExecutionRejection(unittest.TestCase):
    def _base_flow(self):
        return {
            "sources": [{"name": "s", "schema": {"a": "int64"},
                         "rows": [{"row_id": 1, "values": {"a": 1}}]}],
            "nodes": [{"id": "sc", "op": "scan", "source": "s"}],
            "sinks": ["sc"],
        }

    def test_dangling_column(self):
        flow = self._base_flow()
        flow["nodes"].append({"id": "f", "op": "filter", "input": "sc",
                              "pred": {"kind": "col", "name": "ghost"}})
        flow["sinks"] = ["f"]
        with self.assertRaises(F.FlowValidationError):
            F.validate(flow)

    def test_cycle(self):
        flow = self._base_flow()
        flow["nodes"] = [
            {"id": "n1", "op": "filter", "input": "n2",
             "pred": {"kind": "is_not_null", "arg": {"kind": "col", "name": "a"}}},
            {"id": "n2", "op": "filter", "input": "n1",
             "pred": {"kind": "is_not_null", "arg": {"kind": "col", "name": "a"}}},
        ]
        flow["sinks"] = ["n1"]
        with self.assertRaises(F.FlowValidationError) as cm:
            F.validate(flow)
        self.assertIn("cycle", str(cm.exception))

    def test_top_k_without_decisive_field(self):
        flow = self._base_flow()
        flow["nodes"].append({"id": "tk", "op": "top_k", "input": "sc", "k": 1,
                              "order_by": [{"expr": {"kind": "col", "name": "a"},
                                            "dir": "asc"}]})
        flow["sinks"] = ["tk"]
        with self.assertRaises(F.FlowValidationError) as cm:
            F.validate(flow)
        self.assertIn("decisive", str(cm.exception))

    def test_top_k_runtime_decisive_collision_rejected(self):
        # same primary key AND same decisive value on two rows: validation passes
        # (a decisive field is declared) but execution must refuse, not pick one
        flow = {
            "sources": [{"name": "s", "schema": {"score": "int64", "tie": "int64"},
                         "rows": [
                             {"row_id": 1, "values": {"score": 5, "tie": 9}},
                             {"row_id": 2, "values": {"score": 5, "tie": 9}},
                         ]}],
            "nodes": [
                {"id": "sc", "op": "scan", "source": "s"},
                {"id": "tk", "op": "top_k", "input": "sc", "k": 1, "order_by": [
                    {"expr": {"kind": "col", "name": "score"}, "dir": "desc",
                     "decisive": False},
                    {"expr": {"kind": "col", "name": "tie"}, "dir": "asc",
                     "decisive": True},
                ]},
            ],
            "sinks": ["tk"],
        }
        F.validate(flow)  # declaration itself is legal
        tmp = tempfile.mkdtemp()
        pkg = AuditPackage.create(os.path.join(tmp, "p"), flow)
        from lineage_auditor.executor import ExecutionError
        with self.assertRaises(ExecutionError):
            Executor(flow, pkg).run()
        # failed package must not serve traces
        with self.assertRaises(Exception):
            AuditReader(pkg)

    def test_schema_ambiguity_in_project(self):
        flow = self._base_flow()
        flow["nodes"].append({"id": "p", "op": "project", "input": "sc", "columns": [
            {"name": "x", "keep": "a"},
            {"name": "x", "expr": {"kind": "lit", "value": 1}},
        ]})
        flow["sinks"] = ["p"]
        with self.assertRaises(F.FlowValidationError):
            F.validate(flow)

    def test_union_schema_mismatch(self):
        flow = self._base_flow()
        flow["sources"].append({"name": "s2", "schema": {"b": "text"},
                                "rows": [{"row_id": 1, "values": {"b": "z"}}]})
        flow["nodes"] = [
            {"id": "sc", "op": "scan", "source": "s"},
            {"id": "sc2", "op": "scan", "source": "s2"},
            {"id": "u", "op": "union_all", "inputs": ["sc", "sc2"]},
        ]
        flow["sinks"] = ["u"]
        with self.assertRaises(F.FlowValidationError):
            F.validate(flow)

    def test_join_ambiguous_output_column(self):
        flow = self._base_flow()
        flow["sources"].append({"name": "s2", "schema": {"a": "int64"},
                                "rows": [{"row_id": 1, "values": {"a": 1}}]})
        flow["nodes"] = [
            {"id": "sc", "op": "scan", "source": "s"},
            {"id": "sc2", "op": "scan", "source": "s2"},
            {"id": "j", "op": "join", "kind": "inner", "left": "sc",
             "right": "sc2", "on": [{"left": "a", "right": "a"}],
             "right_prefix": ""},
        ]
        flow["sinks"] = ["j"]
        with self.assertRaises(F.FlowValidationError):
            F.validate(flow)


class TestPackageLifecycle(unittest.TestCase):
    def test_failed_package_refuses_traces(self):
        flow = load("main_demo.json")
        tmp = tempfile.mkdtemp()
        pkg = AuditPackage.create(os.path.join(tmp, "bad"), flow)
        pkg.mark_failed(RuntimeError("boom"))
        with self.assertRaises(Exception) as cm:
            AuditReader(pkg)
        self.assertIn("FAILED", str(cm.exception))

    def test_cancelled_package_refuses_traces(self):
        flow = load("main_demo.json")
        tmp = tempfile.mkdtemp()
        pkg = AuditPackage.create(os.path.join(tmp, "cancelled"), flow)
        pkg.mark_cancelled()
        with self.assertRaises(Exception) as cm:
            AuditReader(pkg)
        self.assertIn("CANCELLED", str(cm.exception))

    def test_stale_running_package_refused_and_recoverable(self):
        flow = load("main_demo.json")
        tmp = tempfile.mkdtemp()
        path = os.path.join(tmp, "stale")
        pkg = AuditPackage.create(path, flow)  # left in "running", like a hard kill
        with self.assertRaises(Exception) as cm:
            AuditReader(pkg)
        self.assertIn("RUNNING", str(cm.exception))
        # explicit recovery closes it as failed, still refusing traces
        pkg.mark_failed(RuntimeError("recovered"))
        with self.assertRaises(Exception) as cm:
            AuditReader(pkg)
        self.assertIn("FAILED", str(cm.exception))

    def test_copy_is_independent(self):
        h = FlowHarness(load("main_demo.json"))
        try:
            dest = os.path.join(h.tmp, "copy")
            copied = h.pkg.copy_to(dest)
            with AuditReader(AuditPackage(copied)) as rd:
                # asking only for grp_main: the f_gt2 blocker on another branch
                # is out of scope, so this trace is cleanly "contributing"
                t = rd.trace_source("src_main", 1, ["grp_main"])
            self.assertEqual(t["surviving_outputs"]["total"], 1)
            self.assertEqual(t["verdict"], "contributing")
            self.assertEqual(t["first_blockers"], [])
            # the unrelated blocker is still reported, explicitly out of scope
            self.assertEqual(
                t["blockers_on_other_paths_not_requested"][0]["blocked_at_node"],
                "f_gt2")
            # asking for every sink restores the partial verdict in the copy
            with AuditReader(AuditPackage(copied)) as rd:
                tall = rd.trace_source("src_main", 1)
            self.assertEqual(tall["verdict"],
                             "partially_contributing_some_branches_blocked")
            with AuditReader(AuditPackage(copied)) as rd:
                nb = rd.node_balance()
            self.assertTrue(any(n["node_id"] == "grp_main" for n in nb))
        finally:
            h.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
