import pytest

from sf2dbx.translate import register, translate


def ids(result, fixed=None):
    return {f.diff_id for f in result.findings if fixed is None or f.fixed is fixed}


@pytest.mark.parametrize("unit", ["DAY", "MONTH", "QUARTER", "YEAR", "HOUR"])
def test_datediff_counts_boundaries_like_snowflake(unit):
    r = translate(f"select datediff({unit.lower()}, a, b) from t")
    assert f"DATEDIFF({unit}, DATE_TRUNC('{unit}', a), DATE_TRUNC('{unit}', b))" in r.sql
    assert "DIFF-DATEDIFF" in ids(r, fixed=True)


def test_datediff_week_is_reported_not_rewritten():
    r = translate("select datediff(week, a, b) from t")
    assert "DATE_TRUNC" not in r.sql and "DIFF-DATE-WEEK" in ids(r, fixed=False)


@pytest.mark.parametrize("sf, dbx", [
    ("try_to_number(s)", "TRY_CAST(s AS DECIMAL(38, 0))"),
    ("try_to_number(s, 12, 4)", "TRY_CAST(s AS DECIMAL(12, 4))"),
    ("to_number(s)", "CAST(s AS DECIMAL(38, 0))"),
])
def test_to_number_keeps_snowflake_precision(sf, dbx):
    assert dbx in translate(f"select {sf} from t").sql


def test_split_part_zero():
    assert "SPLIT_PART(s, '-', 1)" in translate("select split_part(s, '-', 0) from t").sql
    assert "CASE WHEN i = 0 THEN 1 ELSE i END" in translate("select split_part(s, '-', i) from t").sql
    r = translate("select split_part(s, '-', 2) from t")
    assert "SPLIT_PART(s, '-', 2)" in r.sql and "DIFF-SPLIT-PART-ZERO" not in ids(r)


def test_null_ordering_is_made_explicit():
    r = translate("select a from t order by a, b desc")
    assert "ORDER BY a NULLS LAST, b DESC NULLS FIRST" in r.sql


def test_standard_functions_translate_cleanly():
    r = translate("select iff(x > 0, 'p', 'n'), nvl(a, 0), div0(a, b) from t")
    assert "IF(x > 0, 'p', 'n')" in r.sql and "COALESCE(a, 0)" in r.sql
    assert ids(r) == {"DIFF-DIVISION"}


@pytest.mark.parametrize("sql, diff_id", [
    ("select f.value from t, lateral flatten(input => t.items) f", "DIFF-FLATTEN"),
    ("select v:a.b from t", "DIFF-JSON-PATH"),
    ("select object_construct('a', 1) from t", "DIFF-OBJECT-CONSTRUCT"),
    ("select listagg(n, ',') within group (order by n) from t", "DIFF-ORDERED-AGG"),
    ("select convert_timezone('UTC', 'Asia/Kolkata', ts) from t", "DIFF-TIMEZONE"),
])
def test_risky_constructs_are_reported(sql, diff_id):
    r = translate(sql)
    assert diff_id in ids(r, fixed=False) and r.needs_review


def test_clean_query_needs_no_review():
    r = translate("select id, sum(amount) total from sales where region = 'EU' group by id")
    assert r.needs_review == [] and "SUM(amount) AS total" in r.sql


def test_multiple_statements():
    r = translate("select 1; select datediff(day, a, b) from t;")
    assert r.sql.count("SELECT") == 2


def test_every_finding_points_at_the_register():
    reg = register()
    r = translate(open("examples/snowflake/kitchen_sink.sql", encoding="utf-8").read())
    assert r.findings and all(f.diff_id in reg for f in r.findings)
    assert all({"title", "risk", "auto_fix", "fix"} <= set(e) for e in reg.values())
