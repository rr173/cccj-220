"""Declarative expressions with SQL three-valued logic (TRUE / FALSE / UNKNOWN).

Expression JSON forms:
  {"kind": "col", "name": "amount"}
  {"kind": "lit", "value": 2 | "x" | true | null}
  {"kind": "cmp", "op": "="|"!="|"<"|"<="|">"|">=", "left": <expr>, "right": <expr>}
  {"kind": "and", "args": [<expr>, ...]}
  {"kind": "or",  "args": [<expr>, ...]}
  {"kind": "not", "arg": <expr>}
  {"kind": "is_null",     "arg": <expr>}
  {"kind": "is_not_null", "arg": <expr>}

Boolean expressions evaluate to True / False / None, where None means UNKNOWN.
NULL = NULL is UNKNOWN (never TRUE), matching the required join semantics.
The same declarative expression is rendered to DuckDB SQL and evaluated by the
independent in-memory test interpreter, so both paths share one semantics.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

VALUE_TYPES = ("int64", "text", "bool", "null")
CMP_OPS = ("=", "!=", "<", "<=", ">", ">=")


class ExpressionError(ValueError):
    """Raised for malformed/ill-typed expressions.

    It is intentionally a ValueError sibling of flow.FlowValidationError so
    pre-execution rejection can be caught uniformly via the shared base below;
    flow.py wraps it in FlowValidationError when validating a full flow.
    """


# --------------------------------------------------------------------------- #
# construction helpers
# --------------------------------------------------------------------------- #
def col(name: str) -> Dict[str, Any]:
    return {"kind": "col", "name": name}


def lit(value: Any) -> Dict[str, Any]:
    if value is not None and not isinstance(value, (int, str, bool)):
        raise ExpressionError(f"literal must be int64/text/bool/null, got {type(value)}")
    return {"kind": "lit", "value": value}


def cmp(op: str, left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    if op not in CMP_OPS:
        raise ExpressionError(f"unknown comparison operator {op!r}")
    return {"kind": "cmp", "op": op, "left": left, "right": right}


def and_(*args: Dict[str, Any]) -> Dict[str, Any]:
    return {"kind": "and", "args": list(args)}


def or_(*args: Dict[str, Any]) -> Dict[str, Any]:
    return {"kind": "or", "args": list(args)}


def not_(arg: Dict[str, Any]) -> Dict[str, Any]:
    return {"kind": "not", "arg": arg}


def is_null(arg: Dict[str, Any]) -> Dict[str, Any]:
    return {"kind": "is_null", "arg": arg}


def is_not_null(arg: Dict[str, Any]) -> Dict[str, Any]:
    return {"kind": "is_not_null", "arg": arg}


def column_refs(expr: Optional[Dict[str, Any]]) -> List[str]:
    """Collect every column reference inside an expression (in encounter order)."""
    if expr is None:
        return []
    kind = expr.get("kind")
    if kind == "col":
        return [expr["name"]]
    if kind == "lit":
        return []
    if kind == "cmp":
        return column_refs(expr["left"]) + column_refs(expr["right"])
    if kind in ("and", "or"):
        out: List[str] = []
        for a in expr["args"]:
            out.extend(column_refs(a))
        return out
    if kind == "not":
        return column_refs(expr["arg"])
    if kind in ("is_null", "is_not_null"):
        return column_refs(expr["arg"])
    raise ExpressionError(f"unknown expression kind {kind!r}")


# --------------------------------------------------------------------------- #
# static type inference
# --------------------------------------------------------------------------- #
def infer_type(expr: Dict[str, Any], schema: Dict[str, str]) -> str:
    """Infer the value type of an expression given a column-name -> type schema."""
    kind = expr["kind"]
    if kind == "col":
        name = expr["name"]
        if name not in schema:
            raise ExpressionError(f"dangling column reference {name!r}")
        return schema[name]
    if kind == "lit":
        v = expr["value"]
        if v is None:
            return "null"
        if isinstance(v, bool):
            return "bool"
        if isinstance(v, int):
            return "int64"
        return "text"
    if kind == "cmp":
        lt = infer_type(expr["left"], schema)
        rt = infer_type(expr["right"], schema)
        _check_comparable(lt, rt)
        return "bool"
    if kind in ("and", "or"):
        for a in expr["args"]:
            t = infer_type(a, schema)
            if t not in ("bool", "null"):
                raise ExpressionError(f"{kind} requires boolean operands, got {t}")
        return "bool"
    if kind == "not":
        t = infer_type(expr["arg"], schema)
        if t not in ("bool", "null"):
            raise ExpressionError(f"NOT requires boolean operand, got {t}")
        return "bool"
    if kind in ("is_null", "is_not_null"):
        infer_type(expr["arg"], schema)
        return "bool"
    raise ExpressionError(f"unknown expression kind {kind!r}")


def _check_comparable(lt: str, rt: str) -> None:
    # the untyped NULL literal is comparable with everything; otherwise types must match
    pair = {lt, rt}
    if "null" in pair:
        return
    if lt != rt:
        raise ExpressionError(f"incompatible comparison between {lt} and {rt}")
    if lt not in ("int64", "text", "bool"):
        raise ExpressionError(f"cannot compare values of type {lt}")


# --------------------------------------------------------------------------- #
# Python three-valued-logic evaluator (shared semantics reference)
# --------------------------------------------------------------------------- #
def _truth(v: Any) -> Optional[bool]:
    """Coerce a value to SQL truth: TRUE/FALSE/UNKNOWN(None)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    raise ExpressionError(f"value {v!r} is not boolean")


def eval_expr(expr: Dict[str, Any], row: Dict[str, Any]) -> Any:
    """Evaluate an expression against a row dict (missing key / None == NULL)."""
    kind = expr["kind"]
    if kind == "col":
        return row.get(expr["name"])
    if kind == "lit":
        return expr["value"]
    if kind == "cmp":
        lv = eval_expr(expr["left"], row)
        rv = eval_expr(expr["right"], row)
        if lv is None or rv is None:
            return None  # UNKNOWN, including NULL = NULL
        op = expr["op"]
        if op == "=":
            return lv == rv
        if op == "!=":
            return lv != rv
        if op == "<":
            return lv < rv
        if op == "<=":
            return lv <= rv
        if op == ">":
            return lv > rv
        if op == ">=":
            return lv >= rv
    if kind == "and":
        result = True
        for a in expr["args"]:
            t = _truth(eval_expr(a, row))
            if t is False:
                return False
            if t is None:
                result = None
        return result
    if kind == "or":
        result = False
        for a in expr["args"]:
            t = _truth(eval_expr(a, row))
            if t is True:
                return True
            if t is None:
                result = None
        return result
    if kind == "not":
        t = _truth(eval_expr(expr["arg"], row))
        return None if t is None else (not t)
    if kind == "is_null":
        return eval_expr(expr["arg"], row) is None
    if kind == "is_not_null":
        return eval_expr(expr["arg"], row) is not None
    raise ExpressionError(f"unknown expression kind {kind!r}")


def eval_predicate(expr: Dict[str, Any], row: Dict[str, Any]) -> Optional[bool]:
    """Evaluate a boolean predicate; returns True / False / None(UNKNOWN)."""
    return _truth(eval_expr(expr, row))


# --------------------------------------------------------------------------- #
# DuckDB SQL rendering
# --------------------------------------------------------------------------- #
def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def to_sql(expr: Dict[str, Any], qualified: Optional[str] = None) -> str:
    """Render the expression as DuckDB SQL with identical 3VL semantics."""
    kind = expr["kind"]
    if kind == "col":
        name = '"' + expr["name"].replace('"', '""') + '"'
        return f"{qualified}.{name}" if qualified else name
    if kind == "lit":
        return _sql_literal(expr["value"])
    if kind == "cmp":
        return f"({to_sql(expr['left'], qualified)} {expr['op']} {to_sql(expr['right'], qualified)})"
    if kind == "and":
        if not expr["args"]:
            return "TRUE"
        return "(" + " AND ".join(to_sql(a, qualified) for a in expr["args"]) + ")"
    if kind == "or":
        if not expr["args"]:
            return "FALSE"
        return "(" + " OR ".join(to_sql(a, qualified) for a in expr["args"]) + ")"
    if kind == "not":
        return f"(NOT {to_sql(expr['arg'], qualified)})"
    if kind == "is_null":
        return f"({to_sql(expr['arg'], qualified)} IS NULL)"
    if kind == "is_not_null":
        return f"({to_sql(expr['arg'], qualified)} IS NOT NULL)"
    raise ExpressionError(f"unknown expression kind {kind!r}")
