# Snowflake to Databricks migration kit

[![ci](https://github.com/suriyalakshmi-chandrasekar/snowflake-to-databricks-migration-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/suriyalakshmi-chandrasekar/snowflake-to-databricks-migration-kit/actions/workflows/ci.yml)
![python](https://img.shields.io/badge/python-3.10%2B-blue) ![sqlglot](https://img.shields.io/badge/sqlglot-30-purple) ![spark](https://img.shields.io/badge/Spark-3.5-orange) ![license](https://img.shields.io/badge/license-MIT-green)

Tools for moving SQL workloads from Snowflake to Databricks **without silently changing the numbers**.

Syntax translation is the easy part: tools like sqlglot already do it well. The risk is SQL that **translates cleanly and runs, but returns different results**. This kit adds the missing pieces:

| Piece | What it does |
|---|---|
| **Translator** (`sf2dbx translate`) | sqlglot translation **plus semantic fixes** for constructs that change results, and a report of everything that needs a human decision |
| **Difference register** (`register.yaml`) | Each known Snowflake vs Databricks behaviour difference: an example, risk level, whether it's auto-fixed, and how to fix it |
| **Parity checker** | Compares Snowflake and Databricks results **as multisets**, so duplicate rows count, with normalisation of floats, decimals and timestamps. Pure Python for small results, Spark for big tables. |
| **Atomic publish** | Staging → gates (not empty, exactly one snapshot, custom checks) → one atomic `INSERT OVERWRITE`. Replaces delete-then-insert patterns that let readers see half-loaded or double-counted data. |
| **Cost baseline** (`sql/snowflake_cost_baseline.sql`) | ACCOUNT_USAGE queries: credits per warehouse, warehouses that never suspend, query families by parameterized hash, polling suspects. Use them to plan migration waves by cost. |

## Translations that run but give different answers

Every row in this table is checked by a test that runs the translated SQL on a real Spark engine:

| Snowflake | Plain translation | This kit | Why |
|---|---|---|---|
| `DATEDIFF(year, '2025-12-31', '2026-01-01')` → **1** | `DATEDIFF(YEAR, …)` → **0** | `DATEDIFF(YEAR, date_trunc('YEAR', a), date_trunc('YEAR', b))` → **1** | Snowflake counts boundaries crossed; Databricks counts whole units |
| `TRY_TO_NUMBER('12.7')` → **13** | `TRY_CAST(… AS DOUBLE)` → **12.7** | `TRY_CAST(… AS DECIMAL(38, 0))` → **13** | Snowflake defaults to NUMBER(38,0) |
| `SPLIT_PART('a/b', '/', 0)` → **'a'** | `SPLIT_PART(…, 0)` → **error** | `SPLIT_PART(…, 1)` (a column index gets a `CASE`) | Snowflake treats 0 as 1 |
| `ORDER BY x` (NULLs last) | — | `ORDER BY x NULLS LAST` | Default NULL ordering is opposite |

**Reported for review, not rewritten:** `LATERAL FLATTEN`, JSON path access (`v:a.b`), `OBJECT_CONSTRUCT`, ordered aggregates (`LISTAGG` / `ARRAY_AGG WITHIN GROUP`), time zones, week maths, division types. See `register.yaml` for details and fixes.

## Usage

```bash
pip install -e ".[dev]"
sf2dbx translate examples/snowflake --out translated/ --report report.md
```

The report has one row per file, plus the register entries it references:

```
| File              | Status | Auto-fixed                                   | Needs review                         |
|-------------------|--------|----------------------------------------------|--------------------------------------|
| daily_revenue.sql | AUTO   | DIFF-DATEDIFF, DIFF-NULL-ORDER, DIFF-TRY-... | -                                    |
| kitchen_sink.sql  | REVIEW | DIFF-DATEDIFF, DIFF-SPLIT-PART-ZERO, ...     | DIFF-FLATTEN, DIFF-JSON-PATH, ...    |
```

From Python:

```python
from sf2dbx.translate import translate
from sf2dbx.parity import compare_rows, compare_dataframes
from sf2dbx.publish import atomic_publish

result = translate(snowflake_sql)
result.sql            # Databricks SQL
result.needs_review   # findings that need a human

compare_rows(snowflake_rows, databricks_rows).summary()
# MISMATCH: left 3 rows, right 2 rows ... left x1: (1.0, 'a')

atomic_publish(spark, df, "gold.scores", snapshot_col="snapshot_date")
```

## A migration workflow that works

1. **Baseline cost** with `sql/snowflake_cost_baseline.sql`, and order waves by credits saved.
2. **Translate** each query and read the report. Fix every `REVIEW` item by hand.
3. **Run both sides** on the same snapshot and compare with the parity checker. Use multiset comparison, since many real datasets have intentional duplicates.
4. **Publish atomically** so consumers never see partial loads, then switch readers over.

## Tests

```bash
pytest -v
```

- **Unit** (pure Python): every fix, every reported construct, multi-statement files, the CLI, the parity normalisation, and register consistency.
- **Spark** (CI): the translated SQL runs on Spark and returns the **documented Snowflake results**. Also covers the example query end to end, DataFrame parity with duplicates, and atomic publish gates (a failed publish leaves the target untouched).

## Timeline

I built and used these tools in **Oct–Nov 2026** while leading a Snowflake to Databricks serving migration: a 424M-row dataset rebuilt with exact parity, about 80 dashboard queries translated, and a cost baseline used to plan the migration waves. This repository is a **generic open-source version (Nov 2026)**, written from scratch with example queries and no client code.

## Author

**Suriyalakshmi Chandrasekar**: Technical Lead, Data Engineering · Databricks Certified Data Engineer Professional · Databricks Data Warehouse Migration Project Expert (Snowflake)
[LinkedIn](https://www.linkedin.com/in/suriyalakshmi-chandrasekar) · [GitHub](https://github.com/suriyalakshmi-chandrasekar)

Built on [sqlglot](https://github.com/tobymao/sqlglot). Not affiliated with Snowflake or Databricks. MIT licensed.
