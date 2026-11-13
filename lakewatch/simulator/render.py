"""Human-readable rendering of a simulation result.

The JSON document is the stable machine contract; this module renders the same
data as a concise review text for an operator deciding whether to apply.
"""
from __future__ import annotations

from ..utils import human_bytes
from .engine import COUNTER_FIELDS


def _line_for_counter(field: str, before: int, after: int) -> str:
    change = after - before
    sign = "+" if change > 0 else ""
    label = field.replace("_", " ")
    if field in ("data_bytes", "manifest_bytes", "snapshot_bytes", "total_bytes"):
        return (
            f"  {label:<22} {human_bytes(before):>10} -> {human_bytes(after):<10} "
            f"{sign}{change} bytes"
        )
    return f"  {label:<22} {before:>10} -> {after:<10} {sign}{change}"


def render_simulation_text(result: dict) -> str:
    summary = result["summary"]
    lines = [
        "Lakewatch plan simulation (read-only, nothing was written)",
        "",
        f"- Simulation: `{result['simulation_id']}`",
        f"- Plan hash:  `{result['plan_hash']}`",
        f"- Lake root:  {result['lake_root']}",
        f"- Reference:  {result['as_of']}",
        "",
        "## Action verdict",
        "",
        f"- Would apply: {summary['would_apply']}",
        f"- Would no-op: {summary['would_noop']}",
        f"- Would fail:  {summary['would_fail']}",
        f"- Risks:       {summary['risk_count']} "
        f"(error={summary['risks_by_severity'].get('error', 0)}, "
        f"warning={summary['risks_by_severity'].get('warning', 0)}, "
        f"info={summary['risks_by_severity'].get('info', 0)})",
        f"- State drift: {'yes - review before applying' if summary['state_drift'] else 'no'}",
        "",
        "## Projected lake totals",
        "",
    ]
    for field in COUNTER_FIELDS:
        lines.append(
            _line_for_counter(
                field, summary["before"][field], summary["after"][field]
            )
        )

    projected = summary["projected"]
    lines += [
        "",
        "## Projected outcomes",
        "",
        f"- Compactions:            {projected['compactions']}",
        f"- Input files removed:    {projected['compaction_input_files_removed']}",
        f"- Output files created:   {projected['compaction_output_files']}",
        f"- Manifest entries pruned: {projected['manifest_entries_dropped']}",
        f"- Snapshots removed:      {projected['snapshots_removed']}",
        f"- Data files vacuumed:    {projected['vacuumed_data_files']}",
        f"- New snapshots archived: {projected['snapshots_projected']}",
        f"- Released space:         {projected['released_human']} "
        f"({projected['released_bytes']} bytes)",
        "",
        "## Actions in execution order",
        "",
        "  # | kind    | table     | partition                     | status",
        "  -+-+---------+-----------+-------------------------------+-----------",
    ]
    for item in result["actions"]:
        lines.append(
            f"  {item['order_index']:>1} | {item['kind']:<7} | "
            f"{item['table']:<9} | {(item['partition'] or '-')[:29]:<29} | "
            f"{item['status']}"
        )

    if result["risks"]:
        lines += ["", "## Risks", ""]
        for item in result["risks"]:
            target = item["action_id"] or "<plan>"
            location = f" {item['path']}" if item["path"] else ""
            lines.append(
                f"- [{item['severity'].upper()}] {item['code']} {target}{location}: "
                f"{item['message']}"
            )
    else:
        lines += ["", "## Risks", "", "- none; every plan input matches the current state"]

    scan = result.get("scan_comparison") or {}
    if scan.get("before") is not None:
        before, after = scan["before"], scan["after"]
        lines += [
            "",
            "## Scan-summary comparison (scanner field names)",
            "",
            f"  {'field':<26}{'before':>10}{'after':>10}{'change':>10}",
        ]
        for field in before:
            change = after[field] - before[field]
            sign = "+" if change > 0 else ""
            lines.append(
                f"  {field:<26}{before[field]:>10}{after[field]:>10}{sign}{change:>9}"
            )
    lines.append("")
    return "\n".join(lines)


__all__ = ["render_simulation_text"]
