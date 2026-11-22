"""Offline lineage auditor: declarative DAG flow execution with full per-row fate auditing.

Modules:
  expr            - declarative expressions with SQL-style three-valued logic
  flow            - flow declaration, schema resolution and pre-execution validation
  executor        - DuckDB-backed execution that materializes occurrence/edge/reject evidence
  audit           - trace-source / trace-output / node-balance queries over an audit package
  preview         - read-only rehearsal of lookup corrections against a top_k sink list
  preview_support - run-time preparation of the narrow index tables preview reads
  slice           - static delivery-column slicing (reduced flow = supply contract)
  package         - on-disk audit package lifecycle (build, open, cancel/failure marking)
  archive         - content-addressed history library: archive/restore evidence versions
  cli             - command line interface
"""

__version__ = "1.0.0"
