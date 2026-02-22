from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from pydantic import ValidationError

from dart_pipeline.contracts import Step6TrackCIntegrationResult
from dart_pipeline.pipeline_step6 import build_track_b_handoff_request
from dart_pipeline.routing import route_by_coverage
from dart_pipeline.track_c import (
    extract_segment_members,
    extract_sga_accounts,
    parse_xbrl_notes,
)
from dart_pipeline.timeseries import build_dual_views
from dart_pipeline.validation import run_tieout


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dart-pipeline-cli",
        description="Run deterministic demo commands for DART pipeline components.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    tieout = subparsers.add_parser("tieout", help="Run tie-out demo")
    tieout.add_argument(
        "--output",
        type=str,
        help="Optional JSON output path under ./out",
    )

    restatement = subparsers.add_parser("restatement", help="Run restatement demo")
    restatement.add_argument(
        "--output",
        type=str,
        help="Optional JSON output path under ./out",
    )

    coverage = subparsers.add_parser("coverage", help="Run coverage-routing demo")
    coverage.add_argument(
        "--output",
        type=str,
        help="Optional JSON output path under ./out",
    )

    handoff_request = subparsers.add_parser(
        "handoff-request",
        help="Build Track-B handoff request from Step6 integration JSON",
    )
    handoff_request.add_argument(
        "--integration-json",
        type=str,
        required=True,
        help="Path to a Step6TrackCIntegrationResult JSON file",
    )

    track_c_helpers = subparsers.add_parser(
        "track-c-helpers",
        help="Extract deterministic Track C helper outputs from an XBRL directory",
    )
    track_c_helpers.add_argument(
        "--xbrl-dir",
        type=str,
        required=True,
        help="Path to XBRL directory",
    )

    return parser


def _demo_tieout() -> dict[str, Any]:
    expected = [
        {
            "metric": "revenue",
            "period": "2024Q4",
            "unit": "KRW",
            "dimensions": {"segment": "A"},
            "value": 100.0,
        }
    ]
    observed = [
        {
            "metric": "revenue",
            "period": "2024Q4",
            "unit": "KRW",
            "dimensions": {"segment": "A"},
            "value": 100.2,
        }
    ]

    result = run_tieout(
        expected=expected,
        observed=observed,
        abs_tol=0.1,
        rel_tol=0.0,
        warn_multiplier=3.0,
    )
    return result.model_dump(mode="json")


def _demo_restatement() -> dict[str, Any]:
    reports = [
        {
            "metric": "revenue",
            "period": "2024Q4",
            "unit": "KRW",
            "dimensions": {},
            "value": 100.0,
            "filing_datetime_utc": "2024-03-01T00:00:00Z",
            "rcept_no": "20240301000001",
            "source_row_idx": 0,
        },
        {
            "metric": "revenue",
            "period": "2024Q4",
            "unit": "KRW",
            "dimensions": {},
            "value": 110.0,
            "filing_datetime_utc": "2024-04-01T00:00:00Z",
            "rcept_no": "20240401000001",
            "source_row_idx": 0,
        },
    ]

    as_reported, as_latest = build_dual_views(reports)
    return {
        "as_reported": as_reported.model_dump(mode="json"),
        "as_latest": as_latest.model_dump(mode="json"),
    }


def _demo_coverage() -> dict[str, Any]:
    decision, report = route_by_coverage(
        required_roles=["income_statement", "balance_sheet", "cash_flow"],
        found_roles=["income_statement", "balance_sheet"],
        critical_roles=["cash_flow"],
        threshold=0.8,
    )
    return {
        "decision": decision.model_dump(mode="json"),
        "report": report.model_dump(mode="json") if report is not None else None,
    }


def _validate_output_path(raw_output: str, cwd: Path) -> Path:
    output = Path(raw_output)
    if any(part == ".." for part in output.parts):
        raise ValueError("--output must not contain '..' path traversal segments")

    out_root = cwd / "out"
    if out_root.is_symlink():
        raise ValueError("./out must not be a symlink")

    target = output if output.is_absolute() else cwd / output
    resolved_target = target.resolve(strict=False)
    resolved_out_root = out_root.resolve(strict=False)

    try:
        resolved_target.relative_to(resolved_out_root)
    except ValueError as exc:
        raise ValueError("--output must resolve to a path under ./out") from exc

    if resolved_target == resolved_out_root:
        raise ValueError("--output must point to a file path under ./out")

    return resolved_target


def _run_command(command: str) -> dict[str, Any]:
    if command == "tieout":
        return _demo_tieout()
    if command == "restatement":
        return _demo_restatement()
    if command == "coverage":
        return _demo_coverage()
    raise ValueError(f"unsupported command: {command}")


def _build_handoff_request_payload(integration_json_path: str) -> dict[str, Any]:
    integration_path = Path(integration_json_path)
    try:
        payload = integration_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(
            f"unable to read --integration-json at {integration_path}: {exc}"
        ) from exc

    try:
        integration_result = Step6TrackCIntegrationResult.model_validate_json(payload)
    except ValidationError as exc:
        first_error = exc.errors()[0] if exc.errors() else {"loc": (), "msg": str(exc)}
        location = ".".join(str(part) for part in first_error.get("loc", ()))
        message = first_error.get("msg", "invalid payload")
        detail = (
            f"{location}: {message}"
            if location
            else message
        )
        raise ValueError(
            "--integration-json is not a valid Step6TrackCIntegrationResult "
            f"({detail})"
        ) from exc

    return build_track_b_handoff_request(
        integration_result=integration_result
    ).model_dump(mode="json")


def _build_track_c_helpers_payload(xbrl_dir: str) -> dict[str, Any]:
    notes = parse_xbrl_notes(xbrl_dir=xbrl_dir)
    return {
        "sga_accounts": extract_sga_accounts(notes),
        "segment_members": [
            member.model_dump(mode="json") for member in extract_segment_members(notes)
        ],
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "handoff-request":
        try:
            payload = _build_handoff_request_payload(args.integration_json)
        except ValueError as exc:
            parser.exit(status=2, message=f"error: {exc}\n")
    elif args.command == "track-c-helpers":
        try:
            payload = _build_track_c_helpers_payload(args.xbrl_dir)
        except ValueError as exc:
            parser.exit(status=2, message=f"error: {exc}\n")
    else:
        payload = _run_command(args.command)

    rendered = json.dumps(payload, sort_keys=True, indent=2) + "\n"

    output = getattr(args, "output", None)
    if output:
        try:
            output_path = _validate_output_path(output, cwd=Path.cwd().resolve())
        except ValueError as exc:
            parser.exit(status=2, message=f"error: {exc}\n")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")

    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
