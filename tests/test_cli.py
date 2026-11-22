"""End-to-end CLI smoke test (subprocess, isolated -I environment)."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUDITOR = os.path.join(ROOT, "bin", "auditor")
FIX = os.path.join(ROOT, "tests", "fixtures", "main_demo.json")


class TestCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.pkg = os.path.join(self.tmp, "pkg")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _run(self, *args):
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(ROOT, "vendor", "duckdb_pkg")
        p = subprocess.run(
            [sys.executable, "-I", AUDITOR, *args],
            capture_output=True, text=True, env=env, cwd=ROOT,
        )
        return p

    def test_full_cli_lifecycle(self):
        p = self._run("run", "--flow", FIX, "--package", self.pkg)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(json.loads(p.stdout)["status"], "completed")

        p = self._run("trace-source", "--package", self.pkg,
                      "--source", "src_dup", "--row-id", "11",
                      "--sinks", "distinct_dup")
        self.assertEqual(p.returncode, 0, p.stderr)
        t = json.loads(p.stdout)
        self.assertEqual(t["verdict"], "contributing")
        self.assertEqual(t["merge_destinations"][0]
                         ["total_contributing_input_occurrences"], 2)

        p = self._run("node-balance", "--package", self.pkg)
        self.assertEqual(p.returncode, 0, p.stderr)
        nb = {n["node_id"]: n for n in json.loads(p.stdout)["node_balance"]}
        self.assertEqual(nb["grp_main"]["merge_occurrences"], 3)
        self.assertEqual(nb["f_gt2"]["rejected_occurrences"], 3)

        dest = os.path.join(self.tmp, "copy")
        p = self._run("copy-package", "--package", self.pkg, "--dest", dest)
        self.assertEqual(p.returncode, 0, p.stderr)
        p = self._run("trace-output", "--package", dest,
                      "--sink", "grp_main", "--occ-id", "0")
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(json.loads(p.stdout)["total_upstream_occurrences"], 4)

    def test_validation_failure_exit_code(self):
        bad = os.path.join(self.tmp, "bad.json")
        with open(bad, "w") as f:
            json.dump({"sources": [], "nodes": [], "sinks": []}, f)
        p = self._run("run", "--flow", bad, "--package",
                      os.path.join(self.tmp, "x"))
        self.assertEqual(p.returncode, 2)
        self.assertIn("rejected_before_execution", p.stderr)
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "x")))


if __name__ == "__main__":
    unittest.main(verbosity=2)
