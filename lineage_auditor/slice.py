"""Static delivery-column slicing: turn one sink of a flow into a smaller flow.

``slice_flow`` answers a concrete hand-over question: downstream will consume
only some of the columns a declared sink already produces, and the new supply
protocol will only hand over the source fields the selected upstream really
needs.  The reduced source schema *is* the new supply contract, so this module
rewrites the flow itself -- source schemas, inline row values, scans, projection
expressions and aggregate items are physically reduced -- and returns a flow
JSON that can be handed straight to ``run``.  No side report or manifest is
produced.

What decides a column is the declaration, never this batch of values:

* the requested delivery columns of the selected sink are the only business
  columns that must reach the output;
* every column a node *reads to decide rows* is kept even when not delivered:
  filter predicates, both sides' equi-keys and residual conditions, group keys,
  distinct keys, partition and ranking expressions;
* an aggregate argument is a business dependency only when the aggregate value
  has a downstream use (delivery, or a later predicate/key/ranking/aggregate).
  A row-count-only use keeps the *grouping* semantics (keys and the empty-input
  baseline) but not the aggregate arguments.

Correctness facts used by the rewrite:

* right-side fields that never appear in the delivery still change how many
  rows a join produces (duplicate keys, residual conditions).  The whole right
  *input* is kept; only unused right *columns* are dropped.
* ``distinct`` picks its first representative by occurrence order; branch order
  is never changed and the columns it must carry are its merge keys plus the
  delivered ones, so the surviving representative value cannot change.
* ``union_all`` keeps every branch in order, even a branch that carries no
  delivered column (it still contributes occurrences and duplicates).
* a source with only a row-count requirement keeps ``__row_id`` and row order
  and may shrink to an empty business schema.
* every shared intermediate node is rewritten exactly once (a single identity),
  so two consumers of a diamond still meet the same occurrences in the same
  order.

Unsupported inputs are refused with a reason and no flow is produced.  Nothing
here executes data, so "supported" means "the rewrite is well defined for this
declaration", not "the values happen to work out".
"""
from __future__ import annotations

import copy
from typing import Any, Dict, Iterable, List, Set

from . import expr as E
from . import flow as F

IDENTITY = F.ROW_ID_COL


class SliceError(ValueError):
    """A slice request that cannot be honoured; carries the reason."""

    def __init__(self, status: str, message: str):
        super().__init__(message)
        self.status = status  # "rejected" (bad request) or "unsupported" (path)
        self.message = message


# --------------------------------------------------------------------------- #
# public entry point
# --------------------------------------------------------------------------- #
def slice_flow(
    flow: Dict[str, Any], sink_id: str, columns: Iterable[str]
) -> Dict[str, Any]:
    """Return a reduced, directly runnable flow delivering ``columns`` at ``sink_id``.

    Raises SliceError (status ``rejected`` for a bad request, ``unsupported``
    for a declaration this static slicer cannot honour).
    """
    try:
        F.validate(flow)
    except F.FlowValidationError as exc:
        raise SliceError("rejected", f"input flow is invalid: {exc}") from exc

    nodes = F.node_map(flow)
    if sink_id not in nodes:
        raise SliceError(
            "rejected", f"unknown sink {sink_id!r}: no node with that id")
    if sink_id not in flow["sinks"]:
        raise SliceError(
            "rejected",
            f"node {sink_id!r} is not a declared sink of this flow "
            f"(declared sinks: {flow['sinks']})")
    if nodes[sink_id]["op"] == "scan":
        raise SliceError(
            "rejected",
            f"sink {sink_id!r} is a scan; its row values are the source rows and "
            f"cannot be projected like a delivery table")

    sink_schema = F.output_schemas(flow)[sink_id]
    requested = list(columns)
    if not requested:
        raise SliceError("rejected", "no delivery columns were requested")
    seen: Set[str] = set()
    unknown: List[Any] = []
    dup: List[str] = []
    for c in requested:
        if not isinstance(c, str) or c not in sink_schema:
            unknown.append(c)
        elif c in seen:
            dup.append(c)
        else:
            seen.add(c)
    if unknown:
        raise SliceError(
            "rejected",
            f"column(s) {unknown} do not exist at sink {sink_id!r}; its columns "
            f"are {list(sink_schema)}")
    if dup:
        raise SliceError(
            "rejected", f"column(s) {dup} were requested more than once")

    result = _Slicer(flow, sink_id, requested).slice()
    try:
        F.validate(result)
    except F.FlowValidationError as exc:  # a slicer bug must never ship
        raise SliceError(
            "unsupported",
            f"the reduced flow would be invalid ({exc}); refusing to emit it",
        ) from exc
    return result


# --------------------------------------------------------------------------- #
# graph helpers (local, so this module does not depend on private helpers)
# --------------------------------------------------------------------------- #
def _parents(flow: Dict[str, Any]) -> Dict[str, List[str]]:
    return F._parents(flow["nodes"])


def _ancestors_incl(parents: Dict[str, List[str]], nid: str) -> Set[str]:
    out: Set[str] = set()
    stack = [nid]
    while stack:
        cur = stack.pop()
        if cur in out:
            continue
        out.add(cur)
        stack.extend(parents[cur])
    return out


class _Slicer:
    def __init__(self, flow: Dict[str, Any], sink_id: str, requested: List[str]):
        self.flow = flow
        self.sink_id = sink_id
        self.requested = requested
        self.nodes = F.node_map(flow)
        self.source_map = F.source_map(flow)
        self.parents = _parents(flow)
        self.region = _ancestors_incl(self.parents, sink_id)
        self.order = [n for n in F.topological_order(flow["nodes"])
                      if n in self.region]
        # minimum output columns each node must produce
        self.kept: Dict[str, Set[str]] = {}
        # columns demanded of each node's output by its consumers
        self.demand: Dict[str, Set[str]] = {}
        # rewritten node declarations (before union normalisation)
        self.rewritten: Dict[str, Dict[str, Any]] = {}
        # projections that became no-ops and are removed (consumer -> input)
        self.dropped: Dict[str, str] = {}
        self.reduced_sources: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------ #
    def slice(self) -> Dict[str, Any]:
        self._check_supported()
        self._propagate_demand()
        self._rewrite_sources()
        for nid in self.order:
            self._rewrite_node(nid)
        new_flow = self._assemble()
        self._normalise_unions(new_flow)
        F.validate(new_flow)  # internal sanity; the public wrapper re-checks
        sink_schema = F.output_schemas(new_flow)[self.sink_id]
        if set(sink_schema) != set(self.requested):
            # The selected sink operation still has to read hidden columns to
            # decide rows (predicate, keys, ranking, ...), so its output can be
            # a superset.  The delivery table itself is exactly the requested
            # columns: a final 1:1 projection is the sink, so no helper column
            # leaves the reduced flow.  The original sink id stays the sink; its
            # operation is renamed to a clearly derived base node, which keeps
            # the delivery table addressable as the declared sink.
            self._append_delivery_projection(new_flow)
        return new_flow

    def _append_delivery_projection(self, new_flow: Dict[str, Any]) -> None:
        sink = self.sink_id
        base_id = self._fresh_id(new_flow, f"{sink}__slice_base")
        by_id = {n["id"]: n for n in new_flow["nodes"]}
        by_id[sink]["id"] = base_id
        # consumers of the old sink (none: it is a sink, but keep the rewrite
        # total for robustness) are not present in the selected sub-graph
        new_flow["nodes"].append({
            "id": sink,
            "op": "project",
            "input": base_id,
            "columns": [{"name": c, "keep": c} for c in self.requested],
        })
        new_flow["sinks"] = [sink]

    # ------------------------------------------------------------------ #
    # supported input shapes
    # ------------------------------------------------------------------ #
    def _check_supported(self) -> None:
        for nid in self.order:
            node = self.nodes[nid]
            if node["op"] == "scan":
                src = self.source_map[node["source"]]
                if src.get("type", "inline") != "inline":
                    raise SliceError(
                        "unsupported",
                        f"scan {nid!r} reads generated source "
                        f"{node['source']!r}; static slicing only supplies "
                        f"inline sources")
                continue
            if node["op"] not in ("filter", "project", "union_all", "join",
                                  "group", "distinct", "top_k"):
                raise SliceError(
                    "unsupported",
                    f"node {nid!r} has op {node['op']!r}, which static slicing "
                    f"does not support")
            if node["op"] == "join":
                prefix = node.get("right_prefix", "r_")
                if prefix == "":
                    raise SliceError(
                        "unsupported",
                        f"join {nid!r} uses an empty right_prefix; left and right "
                        f"columns are then indistinguishable, so a column cannot "
                        f"be assigned to its side without guessing")

    # ------------------------------------------------------------------ #
    # demand propagation
    # ------------------------------------------------------------------ #
    def _propagate_demand(self) -> None:
        for nid in reversed(self.order):
            node = self.nodes[nid]
            if node["op"] == "scan":
                self.kept[nid] = set(self.demand.get(nid, set()))
                continue
            if nid == self.sink_id:
                kept = set(self.requested)
            else:
                kept = set(self.demand.get(nid, set()))
            # A union has to carry at least one column to stay a valid relation
            # when only its row count matters.  Prefer the reserved identity
            # column (row order, no business field).  Only if a branch cannot
            # produce it (e.g. a group collapses rows) fall back to a column
            # that every branch really produces; never invent a placeholder.
            if node["op"] == "union_all" and not kept:
                kept = self._row_carrier_for_union(node)
            self.kept[nid] = kept
            inputs = self._input_needs(node, kept)
            for parent, cols in inputs.items():
                if cols:
                    self.demand.setdefault(parent, set()).update(cols)

    def _group_input_need(self, node: Dict[str, Any], kept_out: Set[str]) -> Set[str]:
        """Group input columns: keys always; aggregate args only when used.

        ``count(*)`` needs no argument.  ``count(x)`` and ``sum(x)`` need their
        argument only when the aggregate value itself has a downstream purpose;
        an unused aggregate is dropped entirely from the declaration without
        changing the grouping (row counts, empty-input baseline).
        """
        need = set(node.get("keys", []))
        used = self._agg_names_used(node, kept_out)
        for a in node["aggs"]:
            if a["name"] in used and a.get("arg") is not None:
                need.add(a["arg"])
        return need

    def _agg_names_used(self, node: Dict[str, Any], kept_out: Set[str]) -> Set[str]:
        return {a["name"] for a in node.get("aggs", []) if a["name"] in kept_out}

    def _row_carrier_for_union(self, node: Dict[str, Any]) -> Set[str]:
        """Pick a column so a row-count-only union stays a valid relation.

        Prefer ``__row_id``: it keeps row order and supplies no business field,
        and scan/filter/project/join/union branches all propagate it.  If a
        branch collapses the identity (a group), fall back to a grouping key of
        that group -- the branch must keep it for its own row decisions anyway,
        so this adds no unused business dependency.  Otherwise refuse: never
        invent a placeholder and never force an otherwise unused business field
        downstream.
        """
        inputs = node["inputs"]
        if all(self._produces(i, IDENTITY) for i in inputs):
            return {IDENTITY}
        # columns every branch can still emit after the rewrite
        original = F.output_schemas(self.flow)
        candidates: Set[str] = set(original[inputs[0]])
        for i in inputs[1:]:
            candidates &= set(original[i])
        candidates.discard(IDENTITY)
        for col in sorted(candidates):
            if all(self._produces(b, col) for b in inputs):
                # this column is now demanded from every branch below (see
                # _input_needs for union_all), so it survives the rewrite
                return {col}
        raise SliceError(
            "unsupported",
            f"union {node['id']!r} only has to supply row counts, but its "
            f"branches cannot carry the reserved identity column and share no "
            f"emittable column; static slicing will not invent a placeholder "
            f"column or add an unused business field")

    def _produces(self, nid: str, column: str) -> bool:
        """Can ``nid`` emit ``column`` after the rewrite?"""
        node = self.nodes[nid]
        op = node["op"]
        if column == IDENTITY:
            if op == "scan":
                return True
            if op == "group":
                return IDENTITY in node.get("keys", [])
            if op in ("filter", "distinct", "top_k"):
                return self._produces(node["input"], column)
            if op == "project":
                # 1:1 pass-through can always carry the input's identity
                return self._produces(node["input"], column)
            if op == "union_all":
                return all(self._produces(i, column) for i in node["inputs"])
            if op == "join":
                return self._produces(node["left"], column)
            return False
        if op == "project":
            if any(c["name"] == column for c in node["columns"]):
                return True
            return self._produces(node["input"], column)
        if op == "group":
            return column in node.get("keys", []) or any(
                a["name"] == column for a in node["aggs"])
        if op == "union_all":
            return all(self._produces(i, column) for i in node["inputs"])
        if op == "join":
            if column in F.output_schemas(self.flow)[node["left"]]:
                return self._produces(node["left"], column)
            return column in F.output_schemas(self.flow)[node["id"]]
        return column in F.output_schemas(self.flow)[nid]

    def _input_needs(
        self, node: Dict[str, Any], kept_out: Set[str]
    ) -> Dict[str, Set[str]]:
        op = node["op"]
        if op == "filter":
            # filtering decides rows; pass-through demands its output columns too
            return {node["input"]: set(E.column_refs(node["pred"])) | kept_out}
        if op == "project":
            return {node["input"]: self._project_input_need(node, kept_out)}
        if op == "union_all":
            return {i: set(kept_out) for i in node["inputs"]}
        if op == "join":
            return {
                node["left"]: self._join_left_need(node, kept_out),
                node["right"]: self._join_right_need(node, kept_out),
            }
        if op == "group":
            # keys always; aggregate arguments only when the aggregate itself
            # has a downstream purpose
            return {node["input"]: self._group_input_need(node, kept_out)}
        if op == "distinct":
            # merge order decides the first representative; its delivered/key
            # values must be carried, so demand those and the merge keys
            return {node["input"]: set(kept_out) | set(node.get("keys", []))}
        if op == "top_k":
            need = set(kept_out)
            need.update(node.get("partition_by", []))
            for term in node.get("order_by", []):
                need.update(E.column_refs(term["expr"]))
            return {node["input"]: need}
        raise SliceError("unsupported", f"node {node['id']!r} op {op!r} unsupported")

    def _project_input_need(self, node: Dict[str, Any], kept_out: Set[str]) -> Set[str]:
        definitions = {c["name"]: c for c in node["columns"]}
        in_schema = F.output_schemas(self.flow)[node["input"]]
        need: Set[str] = set()
        for name in kept_out:
            c = definitions.get(name)
            if c is not None:
                if "keep" in c:
                    need.add(c["keep"])
                else:
                    need.update(E.column_refs(c["expr"]))
            elif name == IDENTITY and self._produces(node["input"], IDENTITY):
                # 1:1 pass-through can carry the reserved identity even when the
                # projection's declared columns do not mention it
                need.add(IDENTITY)
            elif name in in_schema:
                need.add(name)
            else:
                # A column this project does not define is still faithfully
                # passed through from its input.  It only survives if the input
                # can really produce it; otherwise the request is unsupported.
                check = F.output_schemas(self.flow)[node["input"]]
                if name in check:
                    need.add(name)
                else:
                    raise SliceError(
                        "unsupported",
                        f"project {node['id']!r} cannot supply required column "
                        f"{name!r} from its input")
        return need

    def _join_left_need(self, node: Dict[str, Any], kept_out: Set[str]) -> Set[str]:
        right_schema = self._right_schema(node)
        prefix = node.get("right_prefix", "r_")
        need: Set[str] = set()
        for pair in node["on"]:
            need.add(pair["left"])
        if node.get("residual") is not None:
            for ref in E.column_refs(node["residual"]):
                if prefix and ref.startswith(prefix) and ref[len(prefix):] in right_schema:
                    continue  # a right-side residual reference
                need.add(ref)
        # every delivered column that is not a right-side output comes from the
        # left input (including the reserved identity when it is delivered)
        right_out = self._right_columns_in_output(node, kept_out)
        need.update(set(kept_out) - right_out)
        return need

    def _join_right_need(self, node: Dict[str, Any], kept_out: Set[str]) -> Set[str]:
        right_schema = self._right_schema(node)
        prefix = node.get("right_prefix", "r_")
        need: Set[str] = set(pair["right"] for pair in node["on"])
        if node.get("residual") is not None:
            for ref in E.column_refs(node["residual"]):
                if prefix and ref.startswith(prefix):
                    rest = ref[len(prefix):]
                    if rest in right_schema:
                        need.add(rest)
                    elif rest == IDENTITY:
                        need.add(IDENTITY)
                elif prefix == "" and ref in right_schema:
                    need.add(ref)
        for out_name in self._right_columns_in_output(node, kept_out):
            rest = out_name[len(prefix):]
            need.add(rest)  # includes the right's reserved identity passthrough
        return need

    def _right_schema(self, node: Dict[str, Any]) -> Dict[str, str]:
        """Business schema of the right input only (identity is a left concern)."""
        return self._business_schema(node["right"])

    def _business_schema(self, nid: str) -> Dict[str, str]:
        """Schema a node can address without relying on the reserved identity."""
        node = self.nodes[nid]
        op = node["op"]
        if op == "scan":
            return dict(self.source_map[node["source"]]["schema"])
        if op in ("filter", "distinct", "top_k"):
            return self._business_schema(node["input"])
        if op == "project":
            in_schema = F.output_schemas(self.flow)[node["input"]]
            out: Dict[str, str] = {}
            for c in node["columns"]:
                if "keep" in c:
                    if c["keep"] == IDENTITY:
                        continue
                    out[c["name"]] = in_schema[c["keep"]]
                else:
                    out[c["name"]] = E.infer_type(c["expr"], in_schema)
            return out
        if op == "union_all":
            return self._business_schema(node["inputs"][0])
        if op == "join":
            out = self._business_schema(node["left"])
            prefix = node.get("right_prefix", "r_")
            if node["kind"] != "anti":
                out.update({prefix + c: t
                            for c, t in self._business_schema(node["right"]).items()})
            return out
        if op == "group":
            in_schema = F.output_schemas(self.flow)[node["input"]]
            out = {k: in_schema[k] for k in node.get("keys", []) if k != IDENTITY}
            for a in node["aggs"]:
                out[a["name"]] = "int64"
            return out
        raise SliceError("unsupported", f"node {nid!r} op {op!r} unsupported")

    def _right_columns_in_output(self, node: Dict[str, Any], kept_out: Set[str]) -> Set[str]:
        right_schema = self._right_schema(node)
        prefix = node.get("right_prefix", "r_")
        if not prefix:
            return set()
        out = {c for c in kept_out
               if c.startswith(prefix) and c[len(prefix):] in right_schema}
        # a residual/predicate or a delivery may address the right side's
        # reserved identity under the prefix as well
        if prefix + IDENTITY in kept_out:
            out.add(prefix + IDENTITY)
        return out

    # ------------------------------------------------------------------ #
    # rewrites
    # ------------------------------------------------------------------ #
    def _rewrite_sources(self) -> None:
        by_source: Dict[str, Set[str]] = {}
        for nid in self.order:
            node = self.nodes[nid]
            if node["op"] == "scan":
                business = self.kept.get(nid, set()) - {IDENTITY}
                by_source.setdefault(node["source"], set()).update(business)
        for name, cols in by_source.items():
            src = self.source_map[name]
            schema = {c: t for c, t in src["schema"].items() if c in cols}
            new = copy.deepcopy(src)
            new["schema"] = schema
            if src.get("type", "inline") != "inline":  # pragma: no cover
                raise SliceError("unsupported", "generated source encountered")
            new["rows"] = [
                {"row_id": r["row_id"],
                 "values": {c: r["values"][c] for c in schema}}
                for r in src.get("rows", [])
            ]
            self.reduced_sources[name] = new

    def _rewrite_node(self, nid: str) -> None:
        node = self.nodes[nid]
        op = node["op"]
        if op == "scan":
            self.rewritten[nid] = {"id": nid, "op": "scan", "source": node["source"]}
            return
        if op == "filter":
            self.rewritten[nid] = {
                "id": nid, "op": "filter", "input": node["input"],
                "pred": copy.deepcopy(node["pred"]),
            }
            return
        if op == "project":
            kept = self.kept[nid]
            if not kept:
                # nothing is required of this 1:1 node: remove it entirely
                self.dropped[nid] = node["input"]
                return
            in_schema = F.output_schemas(self.flow)[node["input"]]
            definitions = {c["name"]: c for c in node["columns"]}
            cols: List[Dict[str, Any]] = []
            for c in node["columns"]:
                if c["name"] not in kept:
                    continue
                if "keep" in c:
                    cols.append({"name": c["name"], "keep": c["keep"]})
                else:
                    cols.append({"name": c["name"],
                                 "expr": copy.deepcopy(c["expr"])})
            present = {c["name"] for c in cols}
            for name in sorted(kept - present):
                if name not in in_schema:
                    raise SliceError(
                        "unsupported",
                        f"project {nid!r} cannot supply required column {name!r}")
                cols.append({"name": name, "keep": name})
            self.rewritten[nid] = {
                "id": nid, "op": "project", "input": node["input"], "columns": cols}
            return
        if op == "union_all":
            self.rewritten[nid] = {
                "id": nid, "op": "union_all", "inputs": list(node["inputs"])}
            return
        if op == "join":
            self.rewritten[nid] = copy.deepcopy(node)
            return
        if op == "group":
            used = self._agg_names_used(node, self.kept[nid])
            aggs = [copy.deepcopy(a) for a in node["aggs"] if a["name"] in used]
            # An empty `aggs` list is a valid *internal* reducer: the executor
            # groups on the keys alone and still emits the empty-input baseline
            # row (count 0 / sum NULL semantics for zero rows).  It is never the
            # delivered sink -- the delivery projection at the end drops it.
            self.rewritten[nid] = {
                "id": nid, "op": "group", "input": node["input"],
                "keys": list(node.get("keys", [])), "aggs": aggs,
            }
            return
        if op in ("distinct", "top_k"):
            self.rewritten[nid] = copy.deepcopy(node)
            return
        raise SliceError("unsupported", f"node {nid!r} op {op!r} unsupported")

    # ------------------------------------------------------------------ #
    # assembly
    # ------------------------------------------------------------------ #
    def _resolve_dropped(self, ref: str) -> str:
        seen: Set[str] = set()
        while ref in self.dropped:
            if ref in seen:  # pragma: no cover - cycle impossible in a DAG
                raise SliceError("unsupported", f"drop cycle at {ref!r}")
            seen.add(ref)
            ref = self.dropped[ref]
        return ref

    def _assemble(self) -> Dict[str, Any]:
        sources = [self.reduced_sources[s["name"]] for s in self.flow["sources"]
                   if s["name"] in self.reduced_sources]
        nodes: List[Dict[str, Any]] = []
        for nid in self.order:
            if nid in self.dropped:
                continue
            node = copy.deepcopy(self.rewritten[nid])
            op = node["op"]
            if op == "union_all":
                node["inputs"] = [self._resolve_dropped(i) for i in node["inputs"]]
            elif op == "join":
                node["left"] = self._resolve_dropped(node["left"])
                node["right"] = self._resolve_dropped(node["right"])
            elif op != "scan":
                node["input"] = self._resolve_dropped(node["input"])
            nodes.append(node)
        return {"sources": sources, "nodes": nodes, "sinks": [self.sink_id]}

    # --------------------------------------------------------------------------- #
    # union input normalisation
    # --------------------------------------------------------------------------- #
    def _normalise_unions(self, new_flow: Dict[str, Any]) -> None:
        """Make every union's inputs carry exactly the columns it must emit.

        Union inputs have to share one schema.  The demand sweep keeps a branch
        from dropping anything the union needs, but a branch can legitimately
        carry *more* (a predicate column, a join key).  We do not widen the other
        branches (that would keep fields nothing uses); instead the over-wide
        branch gets an explicit projection to the union schema.
        """
        by_id = {n["id"]: n for n in new_flow["nodes"]}
        topo = F.topological_order(new_flow["nodes"])
        schemas: Dict[str, Dict[str, str]] = {}
        for nid in topo:
            node = by_id[nid]
            if node["op"] == "union_all":
                target_cols = set(self.kept[nid])
                # The union's output schema is the (first input's) schema
                # restricted to the kept columns, with their declared types.
                first = schemas[node["inputs"][0]]
                target = {c: first[c] for c in first if c in target_cols}
                for i, ref in enumerate(node["inputs"]):
                    child = schemas[ref]
                    missing = target_cols - set(child)
                    if missing:
                        raise SliceError(
                            "unsupported",
                            f"union {nid!r} input {ref!r} cannot supply "
                            f"required column(s) {sorted(missing)}")
                    if child == target:
                        continue
                    wrap_id = self._fresh_id(new_flow, f"{nid}__slice_in_{i}")
                    wrap = {
                        "id": wrap_id, "op": "project", "input": ref,
                        "columns": [{"name": c, "keep": c} for c in target],
                    }
                    new_flow["nodes"].insert(
                        new_flow["nodes"].index(node), wrap)
                    by_id[wrap_id] = wrap
                    schemas[wrap_id] = dict(target)
                    node["inputs"][i] = wrap_id
                schemas[nid] = dict(target)
                continue
            schemas[nid] = self._natural_schema(node, schemas, by_id)

    @staticmethod
    def _fresh_id(flow: Dict[str, Any], base: str) -> str:
        used = {n["id"] for n in flow["nodes"]}
        candidate, n = base, 1
        while candidate in used:
            n += 1
            candidate = f"{base}_{n}"
        return candidate

    def _natural_schema(
        self, node: Dict[str, Any], schemas: Dict[str, Dict[str, str]],
        by_id: Dict[str, Dict[str, Any]],
    ) -> Dict[str, str]:
        op = node["op"]
        if op == "scan":
            src = next(s for s in self._reduced_source_list()
                       if s["name"] == node["source"])
            return {IDENTITY: "int64", **dict(src["schema"])}
        if op == "filter":
            return dict(schemas[node["input"]])
        if op == "project":
            in_schema = schemas[node["input"]]
            out: Dict[str, str] = {}
            for c in node["columns"]:
                if "keep" in c:
                    out[c["name"]] = in_schema[c["keep"]]
                else:
                    out[c["name"]] = E.infer_type(c["expr"], in_schema)
            return out
        if op == "union_all":
            return dict(schemas[node["inputs"][0]])
        if op == "join":
            left = schemas[node["left"]]
            right = schemas[node["right"]]
            if node["kind"] == "anti":
                return dict(left)
            prefix = node.get("right_prefix", "r_")
            out = dict(left)
            out.update({prefix + c: t for c, t in right.items()})
            return out
        if op == "group":
            in_schema = schemas[node["input"]]
            out = {k: in_schema[k] for k in node.get("keys", [])}
            for a in node["aggs"]:
                out[a["name"]] = "int64"
            return out
        if op in ("distinct", "top_k"):
            return dict(schemas[node["input"]])
        raise SliceError("unsupported", f"node {node['id']!r} op {op!r} unsupported")

    def _reduced_source_list(self) -> List[Dict[str, Any]]:
        return list(self.reduced_sources.values())
