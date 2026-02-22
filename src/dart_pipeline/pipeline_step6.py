from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from dart_pipeline.contracts import TrackARow, TrackASnapshot
from dart_pipeline.dart_api import DartApiClient, DartApiError, DartApiErrorCode

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


__all__ = [
    "ANNUAL_REPRT_CODE",
    "build_track_a_snapshot",
]
