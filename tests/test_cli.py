import json

from lakewatch.cli import main

from conftest import write_cli_config


def _json_output(capsys):
    return json.loads(capsys.readouterr().out)


def test_cli_scan_plan_apply_report_end_to_end(tmp_path, lake, policy, capsys):
    config = write_cli_config(
        tmp_path / "configs" / "default.yaml", lake, policy,
        tmp_path / "ledger.sqlite3", tmp_path / "reports",
    )
    assert main(["scan", "--config", str(config), "--json"]) == 0
    scan = _json_output(capsys)
    assert scan["summary"]["file_count"] == 47

    plan_path = tmp_path / "plan.json"
    assert main(["plan", "--config", str(config), "--out", str(plan_path)]) == 0
    plan = _json_output(capsys)
    assert plan["summary"]["action_count"] == 10
    assert json.loads(plan_path.read_text(encoding="utf-8"))["plan_hash"] == plan["plan_hash"]

    assert main(["apply", "--config", str(config), "--plan", str(plan_path)]) == 0
    first = _json_output(capsys)
    assert first["status"] == "completed"
    assert first["report"]["format"] == "lakewatch.report/1"
    assert first["report"]["plan_hash"] == plan["plan_hash"]

    assert main(["apply", "--config", str(config), "--plan", str(plan_path)]) == 0
    second = _json_output(capsys)
    assert second["status"] == "already_completed"
    assert second["skipped_actions"] == 10

    assert main(["report", "--config", str(config), "--json"]) == 0
    report = _json_output(capsys)
    assert report["plan_hash"] == plan["plan_hash"]


def test_cli_exit_code_semantics(tmp_path, lake, policy, capsys):
    config = write_cli_config(
        tmp_path / "configs" / "default.yaml", lake, policy,
        tmp_path / "ledger.sqlite3", tmp_path / "reports",
    )
    assert main(["scan", "--config", str(config), "--strict"]) == 1
    capsys.readouterr()

    assert main(["scan", "--config", str(tmp_path / "missing.yaml")]) == 2
    capsys.readouterr()

    empty_config = write_cli_config(
        tmp_path / "empty" / "config.yaml", lake, policy,
        tmp_path / "empty" / "ledger.sqlite3", tmp_path / "empty" / "reports",
    )
    assert main(["report", "--config", str(empty_config)]) == 2
    capsys.readouterr()
