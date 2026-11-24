"""Run translated SQL on a real Spark engine and check it returns what Snowflake returns.

Expected values are documented Snowflake behaviour (boundary-counting DATEDIFF,
SPLIT_PART index 0, TRY_TO_NUMBER default scale 0). Runs in CI.
"""

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from sf2dbx.parity import compare_dataframes  # noqa: E402
from sf2dbx.publish import PublishGateError, atomic_publish  # noqa: E402
from sf2dbx.translate import translate  # noqa: E402


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
        b = (SparkSession.builder.master("local[2]").appName("sf2dbx")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("wh")))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false"))
        return configure_spark_with_delta_pip(b).getOrCreate()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Spark not available: {exc}")


def run(spark, snowflake_sql):
    return spark.sql(translate(snowflake_sql).sql).first()[0]


@pytest.mark.parametrize("unit, start, end, snowflake_result", [
    ("year", "2025-12-31", "2026-01-01", 1),
    ("month", "2026-01-31", "2026-02-01", 1),
    ("quarter", "2026-03-31", "2026-04-01", 1),
    ("day", "2026-01-01 23:59:00", "2026-01-02 00:01:00", 1),
    ("hour", "2026-01-01 10:59:00", "2026-01-01 11:01:00", 1),
    ("month", "2026-01-01", "2026-01-31", 0),
    ("year", "2026-06-01", "2025-01-01", -1),
])
def test_datediff_matches_snowflake(spark, unit, start, end, snowflake_result):
    sql = f"select datediff({unit}, to_timestamp('{start}'), to_timestamp('{end}'))"
    assert run(spark, sql) == snowflake_result


def test_naive_translation_would_have_been_wrong(spark):
    # the plain translation counts whole units: 0 instead of Snowflake's 1
    assert spark.sql("select datediff(YEAR, timestamp'2025-12-31', timestamp'2026-01-01')").first()[0] == 0


def test_split_part_zero_and_to_number(spark):
    assert run(spark, "select split_part('a/b/c', '/', 0)") == "a"
    assert int(run(spark, "select try_to_number('12.7')")) == 13
    assert run(spark, "select try_to_number('not a number')") is None
    assert str(run(spark, "select try_to_number('12.345', 10, 2)")) == "12.35"


def test_example_query_runs_on_spark(spark):
    spark.createDataFrame(
        [("2026-01-05", "EU", "WEB", 10.0, "2026-01-05 10:00:00", "2026-01-07 09:00:00", "15"),
         ("2026-01-05", "EU", "STORE", None, "2026-01-05 11:00:00", "2026-01-06 12:00:00", "x")],
        "order_date string, region string, channel string, amount double, created_at string, "
        "shipped_at string, discount_code_pct string",
    ).selectExpr("cast(order_date as date) order_date", "region", "channel", "amount",
                 "cast(created_at as timestamp) created_at", "cast(shipped_at as timestamp) shipped_at",
                 "discount_code_pct").createOrReplaceTempView("orders")
    sql = open("examples/snowflake/daily_revenue.sql", encoding="utf-8").read().replace("sales.orders", "orders")
    sql = sql.replace("DATEADD(month, -3, CURRENT_DATE())", "DATE '2025-12-01'")
    # QUALIFY runs on Databricks SQL but not on open-source Spark 3.5, which CI uses
    sql = "\n".join(line for line in sql.splitlines() if not line.startswith("QUALIFY"))
    rows = spark.sql(translate(sql).sql.rstrip(";")).collect()
    assert len(rows) == 2 and {r.channel_group for r in rows} == {"online", "store"}


def test_parity_on_dataframes_counts_duplicates(spark):
    a = spark.createDataFrame([(1, "x"), (1, "x"), (2, "y")], "id int, v string")
    b = spark.createDataFrame([(1, "x"), (2, "y")], "id long, v string")
    r = compare_dataframes(a, b)
    assert not r.match and r.left_rows == 3 and r.right_rows == 2
    assert compare_dataframes(a, a).match


def test_atomic_publish(spark):
    good = spark.createDataFrame([("2026-01-01", 1), ("2026-01-01", 2)], "snapshot string, v int")
    atomic_publish(spark, good, "default.scores", snapshot_col="snapshot")
    assert spark.table("default.scores").count() == 2

    mixed = spark.createDataFrame([("2026-01-02", 1), ("2026-01-03", 2)], "snapshot string, v int")
    with pytest.raises(PublishGateError, match="exactly one"):
        atomic_publish(spark, mixed, "default.scores", snapshot_col="snapshot")
    with pytest.raises(PublishGateError, match="at least"):
        atomic_publish(spark, good.limit(0), "default.scores")
    assert spark.table("default.scores").count() == 2             # failed publishes leave the target alone

    newer = spark.createDataFrame([("2026-01-02", i) for i in range(5)], "snapshot string, v int")
    result = atomic_publish(spark, newer, "default.scores", snapshot_col="snapshot",
                            checks=[lambda df: None if df.where("v < 0").isEmpty() else "negative values"])
    assert result.snapshot == "2026-01-02" and spark.table("default.scores").count() == 5
