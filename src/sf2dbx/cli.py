"""Command line: translate .sql files and write a review report.

    sf2dbx translate queries/ --out translated/ --report report.md
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .translate import register, translate


def _files(path: Path) -> list[Path]:
    return sorted(path.rglob("*.sql")) if path.is_dir() else [path]


def cmd_translate(args) -> int:
    src = Path(args.path)
    out = Path(args.out) if args.out else None
    rows, failures = [], 0
    for f in _files(src):
        try:
            result = translate(f.read_text(encoding="utf-8"), pretty=True)
        except Exception as exc:  # report and continue with the other files
            failures += 1
            rows.append((f.name, "PARSE ERROR", str(exc).splitlines()[0], ""))
            continue
        if out:
            target = out / (f.relative_to(src) if src.is_dir() else f.name)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(result.sql + "\n", encoding="utf-8")
        fixed = sorted({x.diff_id for x in result.findings if x.fixed})
        review = sorted({x.diff_id for x in result.needs_review})
        status = "REVIEW" if review else "AUTO"
        rows.append((f.name, status, ", ".join(fixed) or "-", ", ".join(review) or "-"))

    lines = ["# Translation report", "",
             f"{len(rows)} file(s): {sum(r[1] == 'AUTO' for r in rows)} fully automatic, "
             f"{sum(r[1] == 'REVIEW' for r in rows)} need review, {failures} failed to parse.", "",
             "| File | Status | Auto-fixed | Needs review |", "|---|---|---|---|"]
    lines += [f"| {a} | {b} | {c} | {d} |" for a, b, c, d in rows]
    used = sorted({d.strip() for r in rows for d in (r[2] + "," + r[3]).split(",") if d.strip() not in ("", "-")})
    if used:
        lines += ["", "## Differences referenced", ""]
        for d in used:
            e = register().get(d)
            if e:
                lines.append(f"- **{d}** ({e['risk']}): {e['title']}. Fix: {e['fix']}")
    report = "\n".join(lines) + "\n"
    if args.report:
        Path(args.report).write_text(report, encoding="utf-8")
    else:
        sys.stdout.write(report)
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="sf2dbx")
    sub = p.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("translate", help="translate a .sql file or a folder of them")
    t.add_argument("path")
    t.add_argument("--out", help="folder for translated files")
    t.add_argument("--report", help="write the markdown report here (default: stdout)")
    t.set_defaults(func=cmd_translate)
    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
