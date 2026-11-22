"""Offline lineage auditor: declarative DAG flow execution with full per-row fate auditing.

Modules:
  expr     - declarative expressions with SQL-style three-valued logic
  flow     - flow declaration, schema resolution and pre-execution validation
  executor - DuckDB-backed execution that materializes occurrence/edge/reject evidence
  audit    - trace-source / trace-output / node-balance queries over an audit package
  package  - on-disk audit package lifecycle (build, open, cancel/failure marking)
  cli      - command line interface
"""

__version__ = "1.0.0"
