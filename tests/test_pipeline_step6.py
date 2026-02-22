from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from dart_pipeline.dart_api import DartApiError, DartApiErrorCode
from dart_pipeline.pipeline_step6 import ANNUAL_REPRT_CODE, build_track_a_snapshot


def _report(
    *,
    reprt_code: str,
    rcept_dt: str,
    rcept_no: str,
    report_nm: str | None = None,
) -> dict[str, str]:
    if report_nm is None:
        year = rcept_dt[:4] if len(rcept_dt) >= 4 else "0000"
        if reprt_code == "11011":
            report_nm = f"사업보고서 ({year}.12)"
        elif reprt_code == "11012":
            report_nm = f"반기보고서 ({year}.06)"
        elif reprt_code == "11013":
            report_nm = f"분기보고서 ({year}.03)"
        elif reprt_code == "11014":
            report_nm = f"분기보고서 ({year}.09)"
        else:
            report_nm = "기타보고서"

    return {
        "reprt_code": reprt_code,
        "report_nm": report_nm,
        "rcept_dt": rcept_dt,
        "rcept_no": rcept_no,
    }


def _acct_row(
    *,
    sj_div: str,
    account_id: str,
    account_nm: str,
    ord_value: str,
    thstrm_amount: str = "",
    frmtrm_amount: str = "",
    bfefrmtrm_amount: str = "",
) -> dict[str, str]:
    return {
        "sj_div": sj_div,
        "account_id": account_id,
        "account_nm": account_nm,
        "ord": ord_value,
        "thstrm_amount": thstrm_amount,
        "frmtrm_amount": frmtrm_amount,
        "bfefrmtrm_amount": bfefrmtrm_amount,
    }


class StubDartApiClient:
    def __init__(
        self,
        *,
        reports: list[dict[str, Any]] | None = None,
        list_reports_error: DartApiError | None = None,
        cfs_rows: list[dict[str, Any]] | None = None,
        ofs_rows: list[dict[str, Any]] | None = None,
        cfs_error: DartApiError | None = None,
        ofs_error: DartApiError | None = None,
    ) -> None:
        self._reports = reports or []
        self._list_reports_error = list_reports_error
        self._cfs_rows = cfs_rows or []
        self._ofs_rows = ofs_rows or []
        self._cfs_error = cfs_error
        self._ofs_error = ofs_error

        self.list_calls: list[dict[str, str]] = []
        self.fetch_calls: list[str] = []

    def list_reports(self, *, corp_code: str, bgn_de: str, end_de: str, pblntf_ty: str, page_count: int = 100):
        self.list_calls.append(
            {
                "corp_code": corp_code,
                "bgn_de": bgn_de,
                "end_de": end_de,
                "pblntf_ty": pblntf_ty,
                "page_count": str(page_count),
            }
        )
        if self._list_reports_error is not None:
            raise self._list_reports_error
        return [dict(row) for row in self._reports]

    def fetch_fnltt_singl_acnt_all(
        self,
        *,
        corp_code: str,
        bsns_year: str,
        reprt_code: str,
        fs_div: str,
    ):
        self.fetch_calls.append(fs_div)
        if fs_div == "CFS":
            if self._cfs_error is not None:
                raise self._cfs_error
            return [dict(row) for row in self._cfs_rows]

        if self._ofs_error is not None:
            raise self._ofs_error
        return [dict(row) for row in self._ofs_rows]


def test_build_track_a_snapshot_deterministic_latest_selection_and_ordering() -> None:
    client = StubDartApiClient(
        reports=[
            _report(reprt_code="11014", rcept_dt="20241231", rcept_no="20241231000001"),
            _report(reprt_code="11011", rcept_dt="20240301", rcept_no="20240301000002"),
            _report(reprt_code="11011", rcept_dt="20240220", rcept_no="20240220000001"),
            _report(reprt_code="11011", rcept_dt="20240301", rcept_no="20240301000003"),
        ],
        cfs_rows=[
            _acct_row(
                sj_div="IS",
                account_id="ifrs-full_Revenue",
                account_nm="Revenue",
                ord_value="2",
                thstrm_amount="10",
            ),
            _acct_row(
                sj_div="BS",
                account_id="ifrs-full_CurrentAssets",
                account_nm="Current Assets",
                ord_value="1",
                thstrm_amount="123456789012345678901234567890",
                frmtrm_amount="1,234.50",
            ),
            _acct_row(
                sj_div="BS",
                account_id="ifrs-full_Assets",
                account_nm="Assets",
                ord_value="1",
                thstrm_amount="5",
            ),
        ],
    )

    snapshot = build_track_a_snapshot(
        dart_api_client=client,
        corp_code="00126380",
        bsns_year="2024",
    )

    assert snapshot.reprt_code == ANNUAL_REPRT_CODE
    assert snapshot.rcept_dt == "20240301"
    assert snapshot.rcept_no == "20240301000003"
    assert snapshot.fs_div == "CFS"

    assert [row.account_id for row in snapshot.rows] == [
        "ifrs-full_Assets",
        "ifrs-full_CurrentAssets",
        "ifrs-full_Revenue",
    ]

    assert snapshot.rows[0].thstrm_amount == 5
    assert snapshot.rows[1].thstrm_amount == 123456789012345678901234567890
    assert snapshot.rows[1].frmtrm_amount == Decimal("1234.50")


def test_build_track_a_snapshot_uses_report_name_when_reprt_code_missing() -> None:
    client = StubDartApiClient(
        reports=[
            {
                "report_nm": "사업보고서 (2024.12)",
                "rcept_dt": "20250307",
                "rcept_no": "20250307000001",
            }
        ],
        cfs_rows=[
            _acct_row(
                sj_div="BS",
                account_id="ifrs-full_Assets",
                account_nm="Assets",
                ord_value="1",
                thstrm_amount="100",
            )
        ],
    )

    snapshot = build_track_a_snapshot(
        dart_api_client=client,
        corp_code="00126380",
        bsns_year="2024",
    )

    assert snapshot.rcept_no == "20250307000001"
    assert len(snapshot.rows) == 1


def test_build_track_a_snapshot_no_report_found_for_strict_reprt_code_filter() -> None:
    client = StubDartApiClient(
        reports=[
            _report(reprt_code="11014", rcept_dt="20240301", rcept_no="20240301000001"),
        ],
        cfs_rows=[],
    )

    with pytest.raises(DartApiError) as exc_info:
        build_track_a_snapshot(
            dart_api_client=client,
            corp_code="00126380",
            bsns_year="2024",
        )

    assert exc_info.value.code == DartApiErrorCode.NO_REPORT_FOUND


def test_build_track_a_snapshot_requires_annual_period_token() -> None:
    client = StubDartApiClient(
        reports=[
            _report(
                reprt_code="11011",
                rcept_dt="20250307",
                rcept_no="20250307000001",
                report_nm="사업보고서 (2023.12)",
            ),
            {
                "reprt_code": "11011",
                "rcept_dt": "20250308",
                "rcept_no": "20250308000001",
            },
        ],
        cfs_rows=[],
    )

    with pytest.raises(DartApiError) as exc_info:
        build_track_a_snapshot(
            dart_api_client=client,
            corp_code="00126380",
            bsns_year="2024",
        )

    assert exc_info.value.code == DartApiErrorCode.NO_REPORT_FOUND


def test_build_track_a_snapshot_malformed_rcept_dt_is_invalid_response_schema() -> None:
    client = StubDartApiClient(
        reports=[
            _report(reprt_code="11011", rcept_dt="2024-03-01", rcept_no="20240301000001"),
        ],
        cfs_rows=[],
    )

    with pytest.raises(DartApiError) as exc_info:
        build_track_a_snapshot(
            dart_api_client=client,
            corp_code="00126380",
            bsns_year="2024",
        )

    assert exc_info.value.code == DartApiErrorCode.INVALID_RESPONSE_SCHEMA


def test_build_track_a_snapshot_no_data_maps_to_no_report_found() -> None:
    client = StubDartApiClient(
        list_reports_error=DartApiError(
            DartApiErrorCode.NO_DATA,
            "no reports",
            status="013",
        )
    )

    with pytest.raises(DartApiError) as exc_info:
        build_track_a_snapshot(
            dart_api_client=client,
            corp_code="00126380",
            bsns_year="2024",
        )

    assert exc_info.value.code == DartApiErrorCode.NO_REPORT_FOUND


def test_build_track_a_snapshot_fallback_to_ofs_on_cfs_no_data() -> None:
    client = StubDartApiClient(
        reports=[
            _report(reprt_code="11011", rcept_dt="20240301", rcept_no="20240301000001"),
        ],
        cfs_error=DartApiError(DartApiErrorCode.NO_DATA, "no cfs", status="013"),
        ofs_rows=[
            _acct_row(
                sj_div="BS",
                account_id="ifrs-full_Assets",
                account_nm="Assets",
                ord_value="1",
                thstrm_amount="100",
            )
        ],
    )

    snapshot = build_track_a_snapshot(
        dart_api_client=client,
        corp_code="00126380",
        bsns_year="2024",
        allow_ofs_fallback=True,
    )

    assert snapshot.fs_div == "OFS"
    assert client.fetch_calls == ["CFS", "OFS"]


def test_build_track_a_snapshot_no_fallback_when_disabled() -> None:
    client = StubDartApiClient(
        reports=[
            _report(reprt_code="11011", rcept_dt="20240301", rcept_no="20240301000001"),
        ],
        cfs_error=DartApiError(DartApiErrorCode.NO_DATA, "no cfs", status="013"),
        ofs_rows=[
            _acct_row(
                sj_div="BS",
                account_id="ifrs-full_Assets",
                account_nm="Assets",
                ord_value="1",
                thstrm_amount="100",
            )
        ],
    )

    with pytest.raises(DartApiError) as exc_info:
        build_track_a_snapshot(
            dart_api_client=client,
            corp_code="00126380",
            bsns_year="2024",
            allow_ofs_fallback=False,
        )

    assert exc_info.value.code == DartApiErrorCode.NO_DATA
    assert client.fetch_calls == ["CFS"]


def test_build_track_a_snapshot_non_no_data_cfs_failure_does_not_fallback() -> None:
    client = StubDartApiClient(
        reports=[
            _report(reprt_code="11011", rcept_dt="20240301", rcept_no="20240301000001"),
        ],
        cfs_error=DartApiError(DartApiErrorCode.DART_ERROR, "bad request", status="010"),
        ofs_rows=[
            _acct_row(
                sj_div="BS",
                account_id="ifrs-full_Assets",
                account_nm="Assets",
                ord_value="1",
                thstrm_amount="100",
            )
        ],
    )

    with pytest.raises(DartApiError) as exc_info:
        build_track_a_snapshot(
            dart_api_client=client,
            corp_code="00126380",
            bsns_year="2024",
            allow_ofs_fallback=True,
        )

    assert exc_info.value.code == DartApiErrorCode.DART_ERROR
    assert client.fetch_calls == ["CFS"]


def test_build_track_a_snapshot_rejects_non_annual_report_code() -> None:
    client = StubDartApiClient(reports=[], cfs_rows=[])

    with pytest.raises(ValueError, match="reprt_code=11011"):
        build_track_a_snapshot(
            dart_api_client=client,
            corp_code="00126380",
            bsns_year="2024",
            reprt_code="11012",
        )
