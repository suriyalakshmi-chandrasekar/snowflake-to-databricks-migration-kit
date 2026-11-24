"""Translate Snowflake SQL to Databricks SQL, fix known semantic gaps, report the rest.

sqlglot does the syntax translation. On top of it this module:

* rewrites constructs that translate *syntactically* but change results
  (DATEDIFF boundaries, TRY_TO_NUMBER precision, SPLIT_PART index 0)
* flags constructs that need a human decision (FLATTEN, JSON paths, ...)

Every fix and finding points at an entry in ``register.yaml``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from importlib import resources

import sqlglot
import yaml
from sqlglot import exp


@dataclass(frozen=True)
class Finding:
    diff_id: str
    risk: str
    snippet: str
    fixed: bool


@dataclass
class Translation:
    source: str
    sql: str
    findings: list[Finding] = field(default_factory=list)

    @property
    def needs_review(self) -> list[Finding]:
        return [f for f in self.findings if not f.fixed]


@lru_cache
def register() -> dict[str, dict]:
    text = resources.files("sf2dbx").joinpath("register.yaml").read_text(encoding="utf-8")
    return {e["id"]: e for e in yaml.safe_load(text)}


def _finding(diff_id: str, node: exp.Expression, fixed: bool) -> Finding:
    return Finding(diff_id, register()[diff_id]["risk"], node.sql(dialect="snowflake")[:120], fixed)


# ---------------------------------------------------------------- fixes
def _fix_datediff(node: exp.DateDiff, findings: list[Finding]) -> exp.Expression:
    if not node.args.get("date_part_boundary"):
        return node
    unit = (node.args.get("unit").name if node.args.get("unit") else "DAY").upper()
    if unit.startswith("WEEK"):
        findings.append(_finding("DIFF-DATE-WEEK", node, fixed=False))
        return node
    findings.append(_finding("DIFF-DATEDIFF", node, fixed=True))
    start, end = node.expression, node.this

    def trunc(e: exp.Expression) -> exp.Expression:
        return exp.Anonymous(this="date_trunc", expressions=[exp.Literal.string(unit), e.copy()])

    return exp.Anonymous(this="DATEDIFF", expressions=[exp.var(unit), trunc(start), trunc(end)])


def _fix_to_number(node: exp.ToNumber, findings: list[Finding]) -> exp.Expression:
    if node.args.get("format"):
        return node                                  # format-mask conversions: leave to sqlglot
    precision = node.args.get("precision")
    scale = node.args.get("scale")
    p = precision.name if precision is not None else "38"
    s = scale.name if scale is not None else "0"
    findings.append(_finding("DIFF-TRY-TO-NUMBER", node, fixed=True))
    to = exp.DataType.build(f"DECIMAL({p}, {s})")
    cast = exp.TryCast if node.args.get("safe") else exp.Cast
    return cast(this=node.this.copy(), to=to)


def _fix_split_part(node: exp.SplitPart, findings: list[Finding]) -> exp.Expression:
    idx = node.args.get("part_index")
    if not node.args.get("part_index_zero_as_one") or idx is None:
        return node
    if isinstance(idx, exp.Literal):
        if idx.name != "0":
            return node
        new_idx: exp.Expression = exp.Literal.number(1)
    else:
        new_idx = exp.Case(ifs=[exp.If(this=exp.EQ(this=idx.copy(), expression=exp.Literal.number(0)),
                                       true=exp.Literal.number(1))], default=idx.copy())
    findings.append(_finding("DIFF-SPLIT-PART-ZERO", node, fixed=True))
    return exp.Anonymous(this="SPLIT_PART", expressions=[node.this.copy(), node.args["delimiter"].copy(), new_idx])


# ---------------------------------------------------------------- scan
REPORT_ONLY = [
    (exp.Lateral, "DIFF-FLATTEN", lambda n: "FLATTEN" in n.sql(dialect="snowflake").upper()),
    (exp.JSONExtract, "DIFF-JSON-PATH", None),
    (exp.Struct, "DIFF-OBJECT-CONSTRUCT", None),
    (exp.GroupConcat, "DIFF-ORDERED-AGG", None),
    (exp.ArrayAgg, "DIFF-ORDERED-AGG", None),
    (exp.Div, "DIFF-DIVISION", None),
    (exp.ConvertTimezone, "DIFF-TIMEZONE", None),
    (exp.CurrentTimestamp, "DIFF-TIMEZONE", None),
]


def translate(sql: str, pretty: bool = False) -> Translation:
    """Translate one or more Snowflake statements."""
    statements = []
    findings: list[Finding] = []
    for tree in sqlglot.parse(sql, read="snowflake"):
        if tree is None:
            continue
        for cls, diff_id, predicate in REPORT_ONLY:
            for node in tree.find_all(cls):
                if predicate is None or predicate(node):
                    findings.append(_finding(diff_id, node, fixed=False))
        if any(True for _ in tree.find_all(exp.Ordered)):
            findings.append(Finding("DIFF-NULL-ORDER", "high", "ORDER BY: explicit NULLS FIRST/LAST added", True))

        def fix(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.DateDiff):
                return _fix_datediff(node, findings)
            if isinstance(node, exp.ToNumber):
                return _fix_to_number(node, findings)
            if isinstance(node, exp.SplitPart):
                return _fix_split_part(node, findings)
            return node

        tree = tree.transform(fix)
        statements.append(tree.sql(dialect="databricks", pretty=pretty))
    sep = ";\n\n" if pretty else ";\n"
    return Translation(source=sql, sql=sep.join(statements), findings=_dedupe(findings))


def _dedupe(findings: list[Finding]) -> list[Finding]:
    seen, out = set(), []
    for f in findings:
        if (f.diff_id, f.snippet) not in seen:
            seen.add((f.diff_id, f.snippet))
            out.append(f)
    return out
