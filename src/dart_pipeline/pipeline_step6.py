from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from dart_pipeline.contracts import (
    Route,
    Step6ExecutionResult,
    Step6TrackCIntegrationResult,
    TrackARow,
    TrackASnapshot,
    TrackBHandoffExecutionResult,
    TrackBHandoffExecutionStatus,
    TrackBHandoffExecutorOutcome,
    TrackBHandoffRequest,
)
from dart_pipeline.dart_api import DartApiClient, DartApiError, DartApiErrorCode
from dart_pipeline.routing import route_from_track_c_roles
from dart_pipeline.track_c import parse_xbrl_notes

ANNUAL_REPRT_CODE = "11011"


def _raise_invalid_schema(message: str) -> None:
    raise DartApiError(DartApiErrorCode.INVALID_RESPONSE_SCHEMA, message)


def _parse_rcept_dt(report: Mapping[str, Any]) -> str:
    raw_rcept_dt = report.get("rcept_dt")
    if not isinstance(raw_rcept_dt, str) or len(raw_rcept_dt) != 8 or not raw_rcept_dt.isdigit():
        _raise_invalid_schema("report.rcept_dt must be an 8-digit YYYYMMDD string")
    return raw_rcept_dt


def _parse_rcept_no(report: Mapping[str, Any]) -> str:
    raw_rcept_no = report.get("rcept_no")
    if not isinstance(raw_rcept_no, str) or not raw_rcept_no.strip():
        _raise_invalid_schema("report.rcept_no must be a non-empty string")
    return raw_rcept_no.strip()


def _infer_reprt_code_from_report_name(report_name: str) -> str | None:
    normalized = report_name.strip()
    if not normalized:
        return None

    if normalized.startswith("사업보고서"):
        return "11011"
    if normalized.startswith("반기보고서"):
        return "11012"
    if normalized.startswith("분기보고서"):
        # DART list.json often omits reprt_code; infer quarter by month token.
        month_match = re.search(r"\((\d{4})\.(\d{2})\)", normalized)
        if not month_match:
            return None
        month = month_match.group(2)
        if month == "03":
            return "11013"
        if month == "09":
            return "11014"
        return None

    return None


def _report_matches(
    report: Mapping[str, Any],
    *,
    reprt_code: str,
    bsns_year: str,
) -> bool:
    raw_reprt_code = report.get("reprt_code")
    report_name = report.get("report_nm")
    inferred_code = (
        _infer_reprt_code_from_report_name(report_name)
        if isinstance(report_name, str)
        else None
    )

    matched_code = None
    if isinstance(raw_reprt_code, str) and raw_reprt_code.strip():
        matched_code = raw_reprt_code.strip()
    elif inferred_code is not None:
        matched_code = inferred_code

    if matched_code != reprt_code:
        return False

    if reprt_code == ANNUAL_REPRT_CODE:
        if not isinstance(report_name, str):
            return False
        # Annual filing for bsns_year N typically appears as "사업보고서 (N.12)".
        if f"({bsns_year}.12)" not in report_name:
            return False

    return True


def _select_latest_report(
    reports: list[dict[str, Any]],
    *,
    reprt_code: str,
    bsns_year: str,
) -> dict[str, Any]:
    candidates: list[tuple[str, str, dict[str, Any]]] = []
    for index, report in enumerate(reports):
        if not isinstance(report, Mapping):
            _raise_invalid_schema(f"reports[{index}] must be an object")

        if not _report_matches(report, reprt_code=reprt_code, bsns_year=bsns_year):
            continue

        rcept_dt = _parse_rcept_dt(report)
        rcept_no = _parse_rcept_no(report)
        candidates.append((rcept_dt, rcept_no, dict(report)))

    if not candidates:
        raise DartApiError(
            DartApiErrorCode.NO_REPORT_FOUND,
            f"no report found for reprt_code={reprt_code} bsns_year={bsns_year}",
        )

    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[-1][2]


def _parse_ord(raw_ord: Any, row_index: int) -> int:
    if isinstance(raw_ord, bool):
        _raise_invalid_schema(f"rows[{row_index}].ord must be a non-negative integer")
    if isinstance(raw_ord, int):
        parsed = raw_ord
    elif isinstance(raw_ord, str) and raw_ord.strip().isdigit():
        parsed = int(raw_ord.strip())
    else:
        _raise_invalid_schema(f"rows[{row_index}].ord must be a non-negative integer")

    if parsed < 0:
        _raise_invalid_schema(f"rows[{row_index}].ord must be a non-negative integer")
    return parsed


def _parse_required_str_field(row: Mapping[str, Any], field_name: str, row_index: int) -> str:
    value = row.get(field_name)
    if not isinstance(value, str):
        _raise_invalid_schema(f"rows[{row_index}].{field_name} must be a string")
    return value


def _normalize_amount(raw_value: Any) -> tuple[str | None, int | Decimal | None]:
    if raw_value is None:
        return None, None
    if not isinstance(raw_value, str):
        return str(raw_value), None

    preserved = raw_value
    stripped = raw_value.strip()
    if not stripped:
        return preserved, None

    cleaned = stripped.replace(",", "")
    if re.fullmatch(r"[+-]?\d+", cleaned):
        return preserved, int(cleaned)

    try:
        return preserved, Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return preserved, None


def _build_track_a_rows(
    *,
    api_rows: list[dict[str, Any]],
    corp_code: str,
    rcept_no: str,
    rcept_dt: str,
    bsns_year: str,
    reprt_code: str,
    fs_div: str,
) -> list[TrackARow]:
    rows: list[TrackARow] = []
    for source_row_idx, raw_row in enumerate(api_rows):
        if not isinstance(raw_row, Mapping):
            _raise_invalid_schema(f"rows[{source_row_idx}] must be an object")

        sj_div = _parse_required_str_field(raw_row, "sj_div", source_row_idx)
        account_id = _parse_required_str_field(raw_row, "account_id", source_row_idx)
        account_nm = _parse_required_str_field(raw_row, "account_nm", source_row_idx)
        ord_value = _parse_ord(raw_row.get("ord"), source_row_idx)

        th_raw, th_value = _normalize_amount(raw_row.get("thstrm_amount"))
        fr_raw, fr_value = _normalize_amount(raw_row.get("frmtrm_amount"))
        bfr_raw, bfr_value = _normalize_amount(raw_row.get("bfefrmtrm_amount"))

        rows.append(
            TrackARow(
                corp_code=corp_code,
                rcept_no=rcept_no,
                rcept_dt=rcept_dt,
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                fs_div=fs_div,
                sj_div=sj_div,
                account_id=account_id,
                account_nm=account_nm,
                ord=ord_value,
                source_row_idx=source_row_idx,
                thstrm_amount_raw=th_raw,
                frmtrm_amount_raw=fr_raw,
                bfefrmtrm_amount_raw=bfr_raw,
                thstrm_amount=th_value,
                frmtrm_amount=fr_value,
                bfefrmtrm_amount=bfr_value,
            )
        )

    rows.sort(
        key=lambda row: (
            row.sj_div,
            row.ord,
            row.account_id,
            row.account_nm,
            row.source_row_idx,
        )
    )
    return rows


def build_track_a_snapshot(
    *,
    dart_api_client: DartApiClient,
    corp_code: str,
    bsns_year: str,
    reprt_code: str = ANNUAL_REPRT_CODE,
    allow_ofs_fallback: bool = False,
) -> TrackASnapshot:
    """Build Track-A annual snapshot from DART list + fnlttSinglAcntAll APIs."""
    if reprt_code != ANNUAL_REPRT_CODE:
        raise ValueError("Step6 currently supports annual reprt_code=11011 only")
    if not isinstance(corp_code, str) or not corp_code.strip():
        raise ValueError("corp_code must be a non-empty string")
    if not isinstance(bsns_year, str) or len(bsns_year) != 4 or not bsns_year.isdigit():
        raise ValueError("bsns_year must be a 4-digit string")

    try:
        next_year = str(int(bsns_year) + 1)
        reports = dart_api_client.list_reports(
            corp_code=corp_code.strip(),
            bgn_de=f"{bsns_year}0101",
            end_de=f"{next_year}1231",
            pblntf_ty="A",
        )
    except DartApiError as exc:
        if exc.code == DartApiErrorCode.NO_DATA:
            raise DartApiError(
                DartApiErrorCode.NO_REPORT_FOUND,
                f"no reports found for corp_code={corp_code} bsns_year={bsns_year}",
            ) from exc
        raise

    latest_report = _select_latest_report(
        reports,
        reprt_code=reprt_code,
        bsns_year=bsns_year,
    )
    latest_rcept_dt = _parse_rcept_dt(latest_report)
    latest_rcept_no = _parse_rcept_no(latest_report)

    fs_div = "CFS"
    try:
        account_rows = dart_api_client.fetch_fnltt_singl_acnt_all(
            corp_code=corp_code,
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            fs_div=fs_div,
        )
    except DartApiError as exc:
        if not allow_ofs_fallback or exc.code != DartApiErrorCode.NO_DATA:
            raise

        fs_div = "OFS"
        account_rows = dart_api_client.fetch_fnltt_singl_acnt_all(
            corp_code=corp_code,
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            fs_div=fs_div,
        )

    rows = _build_track_a_rows(
        api_rows=account_rows,
        corp_code=corp_code,
        rcept_no=latest_rcept_no,
        rcept_dt=latest_rcept_dt,
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        fs_div=fs_div,
    )

    return TrackASnapshot(
        corp_code=corp_code,
        rcept_no=latest_rcept_no,
        rcept_dt=latest_rcept_dt,
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        fs_div=fs_div,
        rows=rows,
    )


def build_step6_track_c_integration(
    *,
    dart_api_client: DartApiClient,
    corp_code: str,
    bsns_year: str,
    xbrl_dir: str | Path,
    required_roles: list[str],
    critical_roles: list[str],
    threshold: float,
    reprt_code: str = ANNUAL_REPRT_CODE,
    allow_ofs_fallback: bool = False,
    note_roles: Mapping[str, str] | None = None,
    role_aliases: Mapping[str, str] | None = None,
) -> Step6TrackCIntegrationResult:
    """Build Step6 Track-A snapshot and Track-C routing decision deterministically."""
    track_a_snapshot = build_track_a_snapshot(
        dart_api_client=dart_api_client,
        corp_code=corp_code,
        bsns_year=bsns_year,
        reprt_code=reprt_code,
        allow_ofs_fallback=allow_ofs_fallback,
    )
    track_c_notes = parse_xbrl_notes(xbrl_dir=xbrl_dir, note_roles=note_roles)
    routing_decision, coverage_report = route_from_track_c_roles(
        parsed_notes=track_c_notes,
        required_roles=required_roles,
        critical_roles=critical_roles,
        threshold=threshold,
        role_aliases=role_aliases,
    )

    return Step6TrackCIntegrationResult(
        track_a_snapshot=track_a_snapshot,
        track_c_notes=track_c_notes,
        routing_decision=routing_decision,
        coverage_report=coverage_report,
        fallback_required=routing_decision.route == Route.TRACK_B_FALLBACK,
    )


def build_track_b_handoff_request(
    *,
    integration_result: Step6TrackCIntegrationResult,
) -> TrackBHandoffRequest:
    if (
        integration_result.routing_decision.route != Route.TRACK_B_FALLBACK
        or not integration_result.fallback_required
    ):
        raise ValueError(
            "Track B handoff request requires TRACK_B_FALLBACK route "
            "and fallback_required=true"
        )

    snapshot = integration_result.track_a_snapshot
    coverage_report = integration_result.coverage_report
    reason_code = integration_result.routing_decision.reason_code
    missing_roles = (
        [] if coverage_report is None else sorted(set(coverage_report.missing_roles))
    )
    critical_missing_roles = (
        []
        if coverage_report is None
        else sorted(set(coverage_report.critical_missing_roles))
    )
    coverage_score = 0.0 if coverage_report is None else coverage_report.coverage_score
    idempotency_key = _build_track_b_handoff_idempotency_key(
        corp_code=snapshot.corp_code,
        bsns_year=snapshot.bsns_year,
        reprt_code=snapshot.reprt_code,
        rcept_no=snapshot.rcept_no,
        rcept_dt=snapshot.rcept_dt,
        fs_div=snapshot.fs_div,
        reason_code=reason_code,
        missing_roles=missing_roles,
        critical_missing_roles=critical_missing_roles,
        coverage_score=coverage_score,
    )

    return TrackBHandoffRequest(
        corp_code=snapshot.corp_code,
        bsns_year=snapshot.bsns_year,
        reprt_code=snapshot.reprt_code,
        rcept_no=snapshot.rcept_no,
        rcept_dt=snapshot.rcept_dt,
        fs_div=snapshot.fs_div,
        idempotency_key=idempotency_key,
        reason_code=reason_code,
        missing_roles=missing_roles,
        critical_missing_roles=critical_missing_roles,
        coverage_score=coverage_score,
    )


def _build_track_b_handoff_idempotency_key(
    *,
    corp_code: str,
    bsns_year: str,
    reprt_code: str,
    rcept_no: str,
    rcept_dt: str,
    fs_div: str,
    reason_code: str,
    missing_roles: list[str],
    critical_missing_roles: list[str],
    coverage_score: float,
) -> str:
    canonical_payload = {
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
        "rcept_no": rcept_no,
        "rcept_dt": rcept_dt,
        "fs_div": fs_div,
        "reason_code": reason_code,
        "missing_roles": missing_roles,
        "critical_missing_roles": critical_missing_roles,
        "coverage_score": coverage_score,
    }
    canonical_json = json.dumps(
        canonical_payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def execute_step6_with_track_b_handoff(
    *,
    dart_api_client: DartApiClient,
    corp_code: str,
    bsns_year: str,
    xbrl_dir: str | Path,
    required_roles: list[str],
    critical_roles: list[str],
    threshold: float,
    reprt_code: str = ANNUAL_REPRT_CODE,
    allow_ofs_fallback: bool = False,
    note_roles: Mapping[str, str] | None = None,
    role_aliases: Mapping[str, str] | None = None,
    track_b_handoff_executor: (
        Callable[[TrackBHandoffRequest], TrackBHandoffExecutorOutcome | None] | None
    ) = None,
    max_handoff_attempts: int = 1,
) -> Step6ExecutionResult:
    """Execute Step6 integration and explicitly prepare/trigger Track-B handoff."""
    integration_result = build_step6_track_c_integration(
        dart_api_client=dart_api_client,
        corp_code=corp_code,
        bsns_year=bsns_year,
        xbrl_dir=xbrl_dir,
        required_roles=required_roles,
        critical_roles=critical_roles,
        threshold=threshold,
        reprt_code=reprt_code,
        allow_ofs_fallback=allow_ofs_fallback,
        note_roles=note_roles,
        role_aliases=role_aliases,
    )

    if integration_result.routing_decision.route == Route.TRACK_C:
        return Step6ExecutionResult(
            integration_result=integration_result,
            track_b_handoff_request=None,
            track_b_handoff_triggered=False,
            track_b_handoff_execution_result=None,
        )

    handoff_request = build_track_b_handoff_request(
        integration_result=integration_result
    )
    if track_b_handoff_executor is None:
        return Step6ExecutionResult(
            integration_result=integration_result,
            track_b_handoff_request=handoff_request,
            track_b_handoff_triggered=False,
            track_b_handoff_execution_result=None,
        )

    if max_handoff_attempts < 1:
        raise ValueError("max_handoff_attempts must be >= 1")

    attempts = 0
    final_outcome: TrackBHandoffExecutorOutcome = TrackBHandoffExecutorOutcome(
        status=TrackBHandoffExecutionStatus.SUCCESS
    )
    while attempts < max_handoff_attempts:
        attempts += 1
        executor_outcome = track_b_handoff_executor(handoff_request)
        final_outcome = (
            TrackBHandoffExecutorOutcome(
                status=TrackBHandoffExecutionStatus.SUCCESS
            )
            if executor_outcome is None
            else executor_outcome
        )

        if final_outcome.status == TrackBHandoffExecutionStatus.SUCCESS:
            break
        if final_outcome.status == TrackBHandoffExecutionStatus.PERMANENT_ERROR:
            break
        if attempts >= max_handoff_attempts:
            break

    execution_result = TrackBHandoffExecutionResult(
        idempotency_key=handoff_request.idempotency_key,
        attempts=attempts,
        max_attempts=max_handoff_attempts,
        outcome=final_outcome,
    )

    return Step6ExecutionResult(
        integration_result=integration_result,
        track_b_handoff_request=handoff_request,
        track_b_handoff_triggered=True,
        track_b_handoff_execution_result=execution_result,
    )


__all__ = [
    "ANNUAL_REPRT_CODE",
    "build_track_a_snapshot",
    "build_step6_track_c_integration",
    "build_track_b_handoff_request",
    "execute_step6_with_track_b_handoff",
]
