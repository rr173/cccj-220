"""Command line interface for the offline lineage auditor.

Commands:
  run           execute a declarative flow, write a self-contained audit package
  trace-source  forward fate explanation for one source row_id
  trace-output  reverse lineage for one sink occurrence
  node-balance  real occurrence accounting for every node
  participation count/sum membership evidence for one aggregate occurrence
  copy-package  produce an independent portable copy of a completed package
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List

from . import flow as F
from .audit import AuditReader
from .executor import Executor
from .package import AuditPackage, STATUS_CANCELLED, PackageError


def _load_flow(path: str) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _print(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def cmd_run(args: argparse.Namespace) -> int:
    flow = _load_flow(args.flow)
    try:
        F.validate(flow)
    except F.FlowValidationError as exc:
        print(json.dumps({"status": "rejected_before_execution", "error": str(exc)},
                         ensure_ascii=False, indent=2), file=sys.stderr)
        return 2
    pkg = AuditPackage.create(args.package, flow)

    def _term(signum, frame):  # SIGTERM must also leave an explicit unfinished mark
        pkg.mark_cancelled()
        print(json.dumps({"status": STATUS_CANCELLED, "package": pkg.path,
                          "signal": signum}, ensure_ascii=False, indent=2),
              file=sys.stderr)
        raise SystemExit(130)

    import signal
    signal.signal(signal.SIGTERM, _term)
    try:
        stats = Executor(flow, pkg).run()
    except KeyboardInterrupt:
        pkg.mark_cancelled()
        print(json.dumps({"status": STATUS_CANCELLED,
                          "package": pkg.path,
                          "note": "package marked unfinished; no trace will be served"},
                         ensure_ascii=False, indent=2), file=sys.stderr)
        return 130
    except BaseException as exc:  # mark failed; executor already recorded the cause
        print(json.dumps({"status": "failed", "package": pkg.path,
                          "error": f"{type(exc).__name__}: {exc}"},
                         ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    out = {
        "status": "completed",
        "package": pkg.path,
        "sinks": flow["sinks"],
        "node_count": len(flow["nodes"]),
        "node_stats": stats,
    }
    _print(out)
    return 0


def _open_reader(package_path: str) -> AuditReader:
    return AuditReader(AuditPackage(package_path))


def cmd_trace_source(args: argparse.Namespace) -> int:
    with _open_reader(args.package) as rd:
        sinks = args.sinks.split(",") if args.sinks else None
        _print(rd.trace_source(args.source, args.row_id, sinks,
                               page_size=args.page_size, page=args.page))
    return 0


def cmd_trace_output(args: argparse.Namespace) -> int:
    with _open_reader(args.package) as rd:
        _print(rd.trace_output(args.sink, args.occ_id,
                               page_size=args.page_size, page=args.page))
    return 0


def cmd_node_balance(args: argparse.Namespace) -> int:
    with _open_reader(args.package) as rd:
        _print({"node_balance": rd.node_balance()})
    return 0


def cmd_participation(args: argparse.Namespace) -> int:
    with _open_reader(args.package) as rd:
        _print(rd.aggregate_participation(args.node, args.occ_id))
    return 0


def cmd_copy_package(args: argparse.Namespace) -> int:
    src = AuditPackage(args.package)
    src.require_completed()
    dest = src.copy_to(args.dest)
    _print({"status": "copied", "source": src.path, "destination": dest,
            "note": "independent package; query it without the original directory"})
    return 0


def cmd_recover(args: argparse.Namespace) -> int:
    """Explicitly close a package stuck in 'running' after a hard kill (SIGKILL)."""
    pkg = AuditPackage(args.package)
    manifest = pkg.manifest()
    if manifest["status"] != "running":
        _print({"package": pkg.path, "status": manifest["status"],
                "note": "recover only applies to packages still marked running"})
        return 0
    pkg.mark_failed(RuntimeError(
        f"recovered: process disappeared while status was running; "
        f"marked unfinished at {time.strftime('%Y-%m-%dT%H:%M:%S')}"))
    _print({"package": pkg.path, "status": "failed",
            "note": "package is now explicitly unfinished; traces will be refused"})
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    flow = _load_flow(args.flow)
    try:
        F.validate(flow)
    except F.FlowValidationError as exc:
        _print({"valid": False, "error": str(exc)})
        return 2
    _print({"valid": True, "nodes": [n["id"] for n in flow["nodes"]],
            "sinks": flow["sinks"]})
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="lineage-auditor",
                                description="offline lineage auditor")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="execute a flow and build an audit package")
    r.add_argument("--flow", required=True)
    r.add_argument("--package", required=True)
    r.set_defaults(fn=cmd_run)

    t = sub.add_parser("trace-source", help="forward trace for a source row_id")
    t.add_argument("--package", required=True)
    t.add_argument("--source", required=True)
    t.add_argument("--row-id", required=True, type=int)
    t.add_argument("--sinks", help="comma-separated sink node ids (default: all)")
    t.add_argument("--page-size", type=int, default=50)
    t.add_argument("--page", type=int, default=0)
    t.set_defaults(fn=cmd_trace_source)

    o = sub.add_parser("trace-output", help="reverse trace for an output occurrence")
    o.add_argument("--package", required=True)
    o.add_argument("--sink", required=True)
    o.add_argument("--occ-id", required=True, type=int)
    o.add_argument("--page-size", type=int, default=50)
    o.add_argument("--page", type=int, default=0)
    o.set_defaults(fn=cmd_trace_output)

    b = sub.add_parser("node-balance", help="per-node occurrence accounting")
    b.add_argument("--package", required=True)
    b.set_defaults(fn=cmd_node_balance)

    pa = sub.add_parser("participation", help="aggregate membership evidence")
    pa.add_argument("--package", required=True)
    pa.add_argument("--node", required=True)
    pa.add_argument("--occ-id", required=True, type=int)
    pa.set_defaults(fn=cmd_participation)

    cp = sub.add_parser("copy-package", help="copy a completed package")
    cp.add_argument("--package", required=True)
    cp.add_argument("--dest", required=True)
    cp.set_defaults(fn=cmd_copy_package)

    v = sub.add_parser("validate", help="validate a flow without executing")
    v.add_argument("--flow", required=True)
    v.set_defaults(fn=cmd_validate)

    rc = sub.add_parser("recover", help="mark a hard-killed 'running' package failed")
    rc.add_argument("--package", required=True)
    rc.set_defaults(fn=cmd_recover)
    return p


def main(argv: List[str] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.fn(args)
    except PackageError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 3
    except FileNotFoundError as exc:
        print(json.dumps({"error": f"file not found: {exc}"}, ensure_ascii=False),
              file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
