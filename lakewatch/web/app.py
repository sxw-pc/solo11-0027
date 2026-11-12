"""Read-only local Flask UI.  Every response is generated on this machine."""
from __future__ import annotations

import json
from pathlib import Path

from flask import Flask, abort, jsonify, render_template, send_file

from ..config import AppConfig
from ..ledger.db import Ledger
from ..planner.plan import build_plan
from ..planner.policy import Policy
from ..scanners import scan_lake
from ..utils import LakewatchError
from .. import __version__


def _decoded_rows(rows: list[dict]) -> list[dict]:
    result = []
    for row in rows:
        item = dict(row)
        for key in ("result_json", "before_json", "after_json", "report_json"):
            if key in item and item[key]:
                try:
                    item[key[:-5]] = json.loads(item[key])
                except ValueError:
                    item[key[:-5]] = None
                item.pop(key, None)
        result.append(item)
    return result


def create_app(config: AppConfig, policy: Policy | None = None) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["LAKEWATCH"] = config
    app.json.sort_keys = True
    ledger = Ledger(config.ledger_path)
    ledger.migrate()
    active_policy = policy or config.policy

    @app.get("/")
    def index():
        return render_template(
            "index.html",
            lake_root=str(config.lake_root),
            policy_hash=active_policy.policy_hash,
            version=__version__,
        )

    @app.get("/healthz")
    def healthz():
        return jsonify(
            {"status": "ok", "offline": True, "version": __version__,
             "policy_hash": active_policy.policy_hash}
        )

    @app.get("/api/config")
    def api_config():
        return jsonify(config.public())

    @app.get("/api/tables")
    def api_tables():
        scan = scan_lake(config.lake_root, active_policy)
        payload = scan.public()
        payload["root"] = str(config.lake_root)
        return jsonify(payload)

    @app.get("/api/plan")
    def api_plan():
        scan = scan_lake(config.lake_root, active_policy)
        return jsonify(build_plan(scan, active_policy))

    @app.get("/api/tasks")
    def api_tasks():
        return jsonify({"tasks": _decoded_rows(ledger.list_tasks(limit=200))})

    @app.get("/api/plans")
    def api_plans():
        return jsonify({"plans": _decoded_rows(ledger.list_plans(limit=50))})

    @app.get("/api/reports")
    def api_reports():
        rows = []
        for row in ledger.list_reports(limit=50):
            item = dict(row)
            item.pop("payload_json", None)
            rows.append(item)
        return jsonify({"reports": rows})

    @app.get("/api/reports/<report_id>")
    def api_report(report_id: str):
        row = ledger.get_report(report_id)
        if row is None:
            abort(404, description="report not found")
        return jsonify(row["payload"])

    @app.get("/api/reports/<report_id>/download")
    def download_report(report_id: str):
        row = ledger.get_report(report_id)
        if row is None:
            abort(404, description="report not found")
        path = Path(row["json_path"]).resolve()
        reports_dir = config.reports_dir.resolve()
        if reports_dir not in path.parents or not path.is_file():
            abort(404, description="report file not found")
        return send_file(
            path,
            mimetype="application/json",
            as_attachment=True,
            download_name=f"{report_id}.json",
        )

    @app.errorhandler(404)
    def not_found(error):
        return jsonify({"error": "not found", "detail": str(error.description)}), 404

    @app.errorhandler(LakewatchError)
    def lakewatch_error(error):
        return jsonify({"error": "lakewatch_error", "detail": str(error)}), 500

    return app
