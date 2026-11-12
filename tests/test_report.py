import json
from pathlib import Path

from lakewatch.executor import apply_plan
from lakewatch.ledger.db import Ledger
from lakewatch.planner.plan import build_plan
from lakewatch.reports.render import report_to_html, report_to_markdown
from lakewatch.scanners import scan_lake


def test_report_compares_before_and_after(tmp_path, lake, policy):
    before = scan_lake(lake, policy)
    plan = build_plan(before, policy)
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    result = apply_plan(
        lake, plan, ledger, before=before, policy=policy,
        reports_dir=tmp_path / "reports"
    )
    after = scan_lake(lake, policy)
    report = result["report"]
    assert report is not None
    assert report["before"] == before.summary()
    assert report["after"] == after.summary()
    assert set(report) == {
        "format", "schema_version", "report_id", "plan_hash", "as_of",
        "before", "after", "delta", "outcomes", "actions",
    }
    assert report["format"] == "lakewatch.report/1"
    for field, value in report["before"].items():
        if isinstance(value, int) and not isinstance(value, bool):
            assert report["delta"][field + "_change"] == (
                report["after"][field] - value
            )
    assert report["outcomes"]["bytes_released"] > 0
    assert report["outcomes"]["compactions"] == 7
    assert report["outcomes"]["missing_resolved"] == 1
    assert len(report["actions"]) == len(plan["actions"])

    files = result["report_files"]
    json_path = Path(files["json_path"])
    markdown_path = Path(files["markdown_path"])
    assert json_path.is_file() and markdown_path.is_file()
    loaded = json.loads(json_path.read_text(encoding="utf-8"))
    assert loaded["report_id"] == report["report_id"]
    assert "Before / after" in report_to_markdown(report)
    assert "<table>" in report_to_html(report)
    stored = ledger.get_report(report["report_id"])
    assert stored is not None
    assert stored["payload"]["plan_hash"] == plan["plan_hash"]
