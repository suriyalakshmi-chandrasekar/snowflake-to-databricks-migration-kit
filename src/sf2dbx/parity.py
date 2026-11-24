"""Compare a Snowflake result with a Databricks result as multisets.

Why multisets: many real datasets contain intentional duplicate rows (event
logs, fan-out joins). A set comparison or a join on keys says "match" even
when one side has two copies of a row and the other has one. Counting every
distinct row on both sides catches that.

Values are normalised first, so cosmetic differences don't count as
mismatches: floats are rounded, timestamps are made naive UTC, decimals are
compared as numbers, and strings can optionally be trimmed.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


@dataclass
class ParityResult:
    left_rows: int
    right_rows: int
    only_left: Counter = field(default_factory=Counter)
    only_right: Counter = field(default_factory=Counter)

    @property
    def match(self) -> bool:
        return not self.only_left and not self.only_right

    def summary(self, sample: int = 5) -> str:
        if self.match:
            return f"MATCH: {self.left_rows} rows on both sides (duplicates included)"
        lines = [f"MISMATCH: left {self.left_rows} rows, right {self.right_rows} rows",
                 f"  rows only/extra on left : {sum(self.only_left.values())}",
                 f"  rows only/extra on right: {sum(self.only_right.values())}"]
        for row, n in list(self.only_left.items())[:sample]:
            lines.append(f"  left  x{n}: {row}")
        for row, n in list(self.only_right.items())[:sample]:
            lines.append(f"  right x{n}: {row}")
        return "\n".join(lines)


def normalize(value: Any, float_digits: int = 6, trim: bool = False) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (float, Decimal)):
        return round(float(value), float_digits) + 0.0          # +0.0 turns -0.0 into 0.0
    if isinstance(value, int):
        return float(value)                                    # 3 (int) == 3.0 (double) == 3.000 (decimal)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat(timespec="microseconds")
    if isinstance(value, str) and trim:
        return value.strip()
    return value


def compare_rows(left: Iterable[Sequence[Any]], right: Iterable[Sequence[Any]],
                 float_digits: int = 6, trim: bool = False) -> ParityResult:
    """Multiset comparison of two row collections (column order must match)."""
    def norm(rows):
        return Counter(tuple(normalize(v, float_digits, trim) for v in r) for r in rows)

    lc, rc = norm(left), norm(right)
    return ParityResult(sum(lc.values()), sum(rc.values()), lc - rc, rc - lc)


def compare_dataframes(left, right, float_digits: int = 6, sample: int = 20) -> ParityResult:
    """Spark version for big tables: counts every distinct row on both sides in one pass."""
    from pyspark.sql import functions as F

    def prep(df):
        cols = []
        for name, dtype in df.dtypes:
            c = F.col(f"`{name}`")
            if dtype.startswith(("double", "float", "decimal", "int", "bigint", "smallint", "tinyint")):
                c = F.round(c.cast("double"), float_digits)
            elif dtype.startswith("timestamp"):
                c = F.date_format(c, "yyyy-MM-dd HH:mm:ss.SSSSSS")
            cols.append(c.cast("string").alias(name))
        return df.select(*cols).groupBy(*df.columns).count()

    if left.columns != right.columns:
        right = right.select(*left.columns)
    lg, rg = prep(left).withColumnRenamed("count", "_l"), prep(right).withColumnRenamed("count", "_r")
    joined = lg.join(rg, [lg[c].eqNullSafe(rg[c]) for c in left.columns], "full_outer").select(
        *[F.coalesce(lg[c], rg[c]).alias(c) for c in left.columns],
        F.coalesce("_l", F.lit(0)).alias("_l"), F.coalesce("_r", F.lit(0)).alias("_r"))
    diff = joined.where("_l <> _r").cache()
    totals = joined.agg(F.sum("_l"), F.sum("_r")).first()
    result = ParityResult(int(totals[0] or 0), int(totals[1] or 0))
    for row in diff.limit(sample).collect():
        values = tuple(row[c] for c in left.columns)
        if row._l > row._r:
            result.only_left[values] = row._l - row._r
        else:
            result.only_right[values] = row._r - row._l
    diff.unpersist()
    return result
