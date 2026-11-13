"""Command-line entry points for scan, plan, apply, report, and serve."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .config import AppConfig, ConfigError, default_config_path, load_config
from .executor import ApplyError, apply_plan
from .ledger.db import Ledger, LedgerBusyError
from .planner.plan import PlanError, build_plan, load_plan, save_plan
from .planner.policy import PolicyError
from .scanners import scan_lake
from .utils import LakewatchError, json_text

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
# EX_TEMPFAIL: a transient condition (SQLite lock/timeout) the caller may retry.
EXIT_BUSY = 75


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config", default=str(default_config_path()), help="application YAML config"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lakewatch", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    scan = sub.add_parser("scan", help="scan the lake and show diagnostics")
    _common(scan)
    scan.add_argument("--json", action="store_true", help="emit JSON")
    scan.add_argument("--out", help="write scan JSON to this file")
    scan.add_argument("--strict", action="store_true", help="fail on error diagnostics")
    plan = sub.add_parser("plan", help="generate a dry-run maintenance plan")
    _common(plan)
    plan.add_argument("--out", help="write plan JSON to this file")
    plan.add_argument("--max-actions", type=int, help="temporarily cap actions")
    apply = sub.add_parser("apply", help="execute a plan and write a report")
    _common(apply)
    apply.add_argument("--plan", help="apply a previously saved plan JSON")
    apply.add_argument("--no-report", action="store_true", help="skip report generation")
    apply.add_argument("--dry-run", action="store_true", help="show the plan only")
    report = sub.add_parser("report", help="show and re-render the latest report")
    _common(report)
    report.add_argument("--json", action="store_true", help="emit full report JSON")
    serve = sub.add_parser("serve", help="run the local read-only web UI")
    _common(serve)
    serve.add_argument("--host", help="override listen host")
    serve.add_argument("--port", type=int, help="override listen port")
    return parser


def _print_json(value: Any) -> None:
    sys.stdout.write(json_text(value))
    sys.stdout.flush()


def _scan(cfg: AppConfig, args: argparse.Namespace) -> int:
    result = scan_lake(cfg.lake_root, cfg.policy)
    payload = result.public()
    if args.out:
        target = Path(args.out)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json_text(payload), encoding="utf-8", newline="\n")
    if args.json:
        _print_json(payload)
    else:
        summary = payload["summary"]
        print(
            f"tables={summary['table_count']} files={summary['file_count']} "
            f"bytes={summary['total_bytes']} small={summary['small_file_count']} "
            f"diagnostics={summary['diagnostic_count']}"
        )
        for item in payload["diagnostics"]:
            print(
                f"[{item['severity']}] {item['category']} "
                f"{item['table']}/{item['partition'] or '-'}: {item['reason']}"
            )
    if args.strict and any(
        item["severity"] == "error" for item in payload["diagnostics"]
    ):
        return EXIT_FAILED
    return EXIT_OK


def _plan(cfg: AppConfig, args: argparse.Namespace) -> int:
    if args.max_actions is not None and args.max_actions < 0:
        raise ValueError("--max-actions must be >= 0")
    scan = scan_lake(cfg.lake_root, cfg.policy)
    plan = build_plan(scan, cfg.policy, args.max_actions)
    if args.out:
        save_plan(plan, args.out)
    _print_json(plan)
    return EXIT_OK


def _apply(cfg: AppConfig, args: argparse.Namespace) -> int:
    ledger = Ledger(cfg.ledger_path)
    if args.plan:
        plan = load_plan(args.plan)
        before = scan_lake(cfg.lake_root, cfg.policy) if not args.no_report else None
    else:
        before = scan_lake(cfg.lake_root, cfg.policy)
        plan = build_plan(before, cfg.policy)
    plan_path = cfg.reports_dir / "plans" / f"plan-{plan['plan_hash'][:16]}.json"
    if not args.dry_run:
        save_plan(plan, plan_path)
    if args.dry_run:
        _print_json(plan)
        return EXIT_OK
    result = apply_plan(
        cfg.lake_root,
        plan,
        ledger,
        before=before,
        policy=cfg.policy if not args.no_report else None,
        reports_dir=None if args.no_report else cfg.reports_dir,
    )
    result["plan_file"] = str(plan_path)
    _print_json(result)
    return EXIT_OK


def _report(cfg: AppConfig, args: argparse.Namespace) -> int:
    ledger = Ledger(cfg.ledger_path)
    ledger.migrate()
    rows = ledger.list_reports(limit=1)
    if not rows:
        print("lakewatch: no reports found; run apply first", file=sys.stderr)
        return EXIT_USAGE
    row = ledger.get_report(rows[0]["report_id"])
    if row is None:
        print("lakewatch: report payload is missing", file=sys.stderr)
        return EXIT_USAGE
    if args.json:
        _print_json(row["payload"])
    else:
        report = row["payload"]
        print(
            f"report={row['report_id']} plan={report['plan_hash']} "
            f"files {report['before']['file_count']} -> "
            f"{report['after']['file_count']} "
            f"released={report['outcomes']['bytes_released']}"
        )
        print(f"json={row['json_path']}")
        print(f"markdown={row['markdown_path']}")
    return EXIT_OK


def _serve(cfg: AppConfig, args: argparse.Namespace) -> int:
    from .web.app import create_app

    host = args.host or cfg.host
    port = args.port if args.port is not None else cfg.port
    if not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("--port must be between 1 and 65535")
    app = create_app(cfg)
    app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        cfg = load_config(args.config)
        if args.command == "scan":
            return _scan(cfg, args)
        if args.command == "plan":
            return _plan(cfg, args)
        if args.command == "apply":
            return _apply(cfg, args)
        if args.command == "report":
            return _report(cfg, args)
        if args.command == "serve":
            return _serve(cfg, args)
        parser.error(f"unknown command: {args.command}")
    except ApplyError as exc:
        print(f"lakewatch: apply failed: {exc}", file=sys.stderr)
        return EXIT_FAILED
    except LedgerBusyError as exc:
        print(
            f"lakewatch: ledger busy, retry shortly: {exc}", file=sys.stderr
        )
        return EXIT_BUSY
    except (ConfigError, PolicyError, PlanError, LakewatchError) as exc:
        print(f"lakewatch: error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"lakewatch: error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except KeyboardInterrupt:
        return 130
    return EXIT_FAILED


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
