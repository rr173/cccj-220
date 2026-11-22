"""On-disk audit package lifecycle.

A package directory is a self-contained, copyable unit::

    <pkg>/
      manifest.json     status + flow fingerprint + engine versions
      flow.json         exact declarative flow the package was built from
      evidence.duckdb   occurrences, edges, rejects, participation (all evidence)
                        plus the narrow preview index tables
      preview.json      row-less flow structure for the preview command

A package that is cancelled or fails is explicitly marked non-completed and every
trace command refuses it: a half-built package can never be served as a normal trace.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from typing import Any, Dict

try:  # duckdb lives in vendor/ in the reference environment
    import duckdb
except ImportError:  # pragma: no cover - fallback for normal installs
    duckdb = None

STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"
FINAL_STATUSES = (STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED)

EVIDENCE_DDL = """
CREATE TABLE occ (
    node_id  VARCHAR NOT NULL,
    occ_id   BIGINT  NOT NULL,
    data_json VARCHAR NOT NULL,
    padded   BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE edges (
    from_node VARCHAR NOT NULL,
    from_occ  BIGINT  NOT NULL,
    to_node   VARCHAR NOT NULL,
    to_occ    BIGINT  NOT NULL,
    role      VARCHAR NOT NULL
);
CREATE TABLE rejects (
    reject_id BIGINT NOT NULL,
    node_id   VARCHAR NOT NULL,
    input_node VARCHAR NOT NULL,
    input_occ  BIGINT NOT NULL,
    reason_code VARCHAR NOT NULL,
    detail_json VARCHAR NOT NULL
);
CREATE TABLE participation (
    node_id    VARCHAR NOT NULL,
    group_occ  BIGINT NOT NULL,
    member_node VARCHAR NOT NULL,
    member_occ  BIGINT NOT NULL,
    agg_name    VARCHAR NOT NULL,
    in_count    BOOLEAN NOT NULL,
    in_sum      BOOLEAN NOT NULL,
    contribution BIGINT
);
CREATE TABLE node_stats (
    node_id VARCHAR PRIMARY KEY,
    op VARCHAR NOT NULL,
    input_occ BIGINT NOT NULL,
    output_occ BIGINT NOT NULL,
    rejected BIGINT NOT NULL,
    expanded BIGINT NOT NULL,
    merged BIGINT NOT NULL
);
"""

# Built once after the bulk load completes: maintaining indexes per insert is
# far more expensive than building them in one pass over the finished tables.
EVIDENCE_INDEXES = """
CREATE INDEX idx_edges_from ON edges(from_node, from_occ);
CREATE INDEX idx_edges_to   ON edges(to_node, to_occ);
CREATE INDEX idx_occ_node   ON occ(node_id);
CREATE INDEX idx_rej_in     ON rejects(input_node, input_occ);
CREATE INDEX idx_part_mem   ON participation(member_node, member_occ);
"""


class PackageError(RuntimeError):
    pass


def flow_fingerprint(flow: Dict[str, Any]) -> str:
    blob = json.dumps(flow, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]


class AuditPackage:
    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        self.manifest_path = os.path.join(self.path, "manifest.json")
        self.flow_path = os.path.join(self.path, "flow.json")
        self.db_path = os.path.join(self.path, "evidence.duckdb")
        self.preview_path = os.path.join(self.path, "preview.json")

    # ------------------------------------------------------------------ #
    @classmethod
    def create(cls, path: str, flow: Dict[str, Any]) -> "AuditPackage":
        pkg = cls(path)
        if os.path.exists(pkg.path):
            if os.path.isdir(pkg.path) and not os.listdir(pkg.path):
                pass
            else:
                raise PackageError(f"package path {pkg.path} exists and is not empty")
        os.makedirs(pkg.path, exist_ok=True)
        with open(pkg.flow_path, "w", encoding="utf-8") as f:
            json.dump(flow, f, ensure_ascii=False, indent=2)
        manifest = {
            "status": STATUS_RUNNING,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "flow_fingerprint": flow_fingerprint(flow),
            "engine": f"duckdb {duckdb.__version__}" if duckdb else "duckdb?",
            "error": None,
        }
        pkg._write_manifest(manifest)
        if duckdb is None:
            raise PackageError("duckdb is not available")
        con = duckdb.connect(pkg.db_path)
        con.execute(EVIDENCE_DDL)
        con.close()
        return pkg

    def connect(self, read_only: bool = False):
        if duckdb is None:
            raise PackageError("duckdb is not available")
        return duckdb.connect(self.db_path, read_only=read_only)

    # ------------------------------------------------------------------ #
    def manifest(self) -> Dict[str, Any]:
        with open(self.manifest_path, encoding="utf-8") as f:
            return json.load(f)

    def _write_manifest(self, manifest: Dict[str, Any]) -> None:
        tmp = self.manifest_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.manifest_path)

    def load_flow(self) -> Dict[str, Any]:
        with open(self.flow_path, encoding="utf-8") as f:
            return json.load(f)

    def mark_completed(self, stats: Dict[str, Any]) -> None:
        m = self.manifest()
        m["status"] = STATUS_COMPLETED
        m["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        m["stats"] = stats
        self._write_manifest(m)

    def mark_failed(self, err: BaseException) -> None:
        self._mark_bad(STATUS_FAILED, err)

    def mark_cancelled(self) -> None:
        self._mark_bad(STATUS_CANCELLED, RuntimeError("execution cancelled before completion"))

    def _mark_bad(self, status: str, err: BaseException) -> None:
        if not os.path.exists(self.manifest_path):
            return
        m = self.manifest()
        if m["status"] in FINAL_STATUSES:
            return
        m["status"] = status
        m["error"] = f"{type(err).__name__}: {err}"
        self._write_manifest(m)

    def require_completed(self) -> Dict[str, Any]:
        if not os.path.exists(self.manifest_path):
            raise PackageError(f"{self.path} is not an audit package (no manifest)")
        m = self.manifest()
        if m["status"] != STATUS_COMPLETED:
            raise PackageError(
                f"package {self.path} is {m['status'].upper()}, not completed; "
                f"trace queries are refused. error={m.get('error')}"
            )
        return m

    def copy_to(self, dest: str) -> str:
        """Produce an independent, self-contained copy that can be queried elsewhere."""
        dest = os.path.abspath(dest)
        if os.path.exists(dest):
            raise PackageError(f"destination {dest} already exists")
        shutil.copytree(self.path, dest)
        return dest
