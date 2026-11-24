"""Atomic publish: build in staging, check, then swap into the target in one commit.

A common Snowflake pattern is "DELETE the snapshot, then INSERT it again".
Between the two statements, readers see missing or duplicated rows, and if
the job runs twice at once, the data is double-counted. On Delta this becomes:

1. write the new data to a staging table
2. run gates: not empty, exactly one snapshot, optional custom checks
3. INSERT OVERWRITE the target from staging: a single atomic Delta commit

Readers see the old version until the commit, then the new one. Never half.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


class PublishGateError(RuntimeError):
    pass


@dataclass
class PublishResult:
    target: str
    rows: int
    snapshot: str | None


def atomic_publish(spark, df, target: str, staging: str | None = None, snapshot_col: str | None = None,
                   min_rows: int = 1, checks: list[Callable] | None = None) -> PublishResult:
    staging = staging or f"{target}__staging"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(staging)
    staged = spark.table(staging)

    rows = staged.count()
    if rows < min_rows:
        raise PublishGateError(f"{staging}: {rows} rows, expected at least {min_rows}")

    snapshot = None
    if snapshot_col:
        snapshots = [r[0] for r in staged.select(snapshot_col).distinct().limit(2).collect()]
        if len(snapshots) != 1:
            raise PublishGateError(f"{staging}: expected exactly one {snapshot_col}, found {snapshots}")
        snapshot = str(snapshots[0])

    for check in checks or []:
        problem = check(staged)
        if problem:
            raise PublishGateError(f"{staging}: {problem}")

    if spark.catalog.tableExists(target):
        spark.sql(f"INSERT OVERWRITE TABLE {target} SELECT * FROM {staging}")
    else:
        staged.write.format("delta").saveAsTable(target)
    return PublishResult(target, rows, snapshot)
