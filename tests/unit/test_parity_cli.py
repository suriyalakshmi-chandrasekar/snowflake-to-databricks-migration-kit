from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sf2dbx.cli import main
from sf2dbx.parity import compare_rows


def test_duplicates_are_counted():
    left = [(1, "a"), (1, "a"), (2, "b")]
    right = [(1, "a"), (2, "b")]                       # same set, different multiset
    r = compare_rows(left, right)
    assert not r.match and r.only_left == {(1.0, "a"): 1}


def test_order_does_not_matter():
    assert compare_rows([(1,), (2,), (2,)], [(2,), (1,), (2,)]).match


def test_cosmetic_differences_are_normalised():
    ts_utc = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    ts_ist = ts_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    left = [(Decimal("3.000"), 0.1 + 0.2, ts_utc, -0.0)]
    right = [(3, 0.3, ts_ist, 0.0)]
    assert compare_rows(left, right).match


def test_trim_option():
    assert not compare_rows([("a ",)], [("a",)]).match
    assert compare_rows([("a ",)], [("a",)], trim=True).match


def test_summary():
    assert compare_rows([(1,)], [(1,)]).summary().startswith("MATCH")
    assert "MISMATCH" in compare_rows([(1,)], [(2,)]).summary()


def test_cli_writes_files_and_report(tmp_path):
    out, report = tmp_path / "out", tmp_path / "report.md"
    code = main(["translate", "examples/snowflake", "--out", str(out), "--report", str(report)])
    assert code == 0
    assert (out / "kitchen_sink.sql").exists() and (out / "daily_revenue.sql").exists()
    text = report.read_text()
    assert "| daily_revenue.sql | AUTO |" in text and "| kitchen_sink.sql | REVIEW |" in text
    assert "DIFF-FLATTEN" in text
