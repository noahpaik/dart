from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from dart_pipeline.contracts import (
    Route,
    RoutingReasonCode,
    TrackBHandoffExecutionStatus,
    TrackBHandoffExecutorOutcome,
)
from dart_pipeline.dart_api import DartApiError, DartApiErrorCode
from dart_pipeline.pipeline_step6 import (
    ANNUAL_REPRT_CODE,
    build_step6_track_c_integration,
    build_track_a_snapshot,
    build_track_b_handoff_request,
    execute_step6_with_track_b_handoff,
)
from dart_pipeline.track_c import parse_xbrl_notes

TRACK_C_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "track_c" / "basic_bundle"


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


def _integration_client() -> StubDartApiClient:
    return StubDartApiClient(
        reports=[
            _report(reprt_code="11011", rcept_dt="20240301", rcept_no="20240301000001"),
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


def test_build_step6_track_c_integration_routes_track_c_on_sufficient_coverage() -> None:
    result = build_step6_track_c_integration(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D822105", "D831150", "D838000"],
        critical_roles=[],
        threshold=1.0,
    )

    assert result.routing_decision.route == Route.TRACK_C
    assert result.routing_decision.reason_code == RoutingReasonCode.COVERAGE_PASS
    assert result.fallback_required is False
    assert result.coverage_report is not None
    assert result.coverage_report.coverage_score == 1.0
    assert [note.role_code for note in result.track_c_notes] == [
        "D822105",
        "D831150",
        "D838000",
    ]


def test_build_step6_track_c_integration_routes_track_b_on_critical_missing_role() -> None:
    result = build_step6_track_c_integration(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D831150", "D851100"],
        critical_roles=["D851100"],
        threshold=0.5,
    )

    assert result.routing_decision.route == Route.TRACK_B_FALLBACK
    assert result.routing_decision.reason_code == RoutingReasonCode.CRITICAL_ROLE_MISSING
    assert result.fallback_required is True
    assert result.coverage_report is not None
    assert result.coverage_report.coverage_score == 0.5
    assert result.coverage_report.critical_missing_roles == ["d851100"]


def test_build_step6_track_c_integration_sorts_notes_deterministically(monkeypatch) -> None:
    unsorted_notes = list(reversed(parse_xbrl_notes(TRACK_C_FIXTURE_DIR)))
    monkeypatch.setattr(
        "dart_pipeline.pipeline_step6.parse_xbrl_notes",
        lambda *args, **kwargs: unsorted_notes,
    )

    result = build_step6_track_c_integration(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D822105", "D831150", "D838000"],
        critical_roles=[],
        threshold=1.0,
    )

    assert [note.role_code for note in result.track_c_notes] == [
        "D822105",
        "D831150",
        "D838000",
    ]


def test_execute_step6_with_track_b_handoff_builds_deterministic_request() -> None:
    result = execute_step6_with_track_b_handoff(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D831150", "D851100"],
        critical_roles=["D851100"],
        threshold=0.5,
    )

    assert result.integration_result.routing_decision.route == Route.TRACK_B_FALLBACK
    assert result.integration_result.fallback_required is True
    assert result.track_b_handoff_triggered is False
    assert result.track_b_handoff_execution_result is None
    assert result.track_b_handoff_request is not None
    request_payload = result.track_b_handoff_request.model_dump(mode="json")
    assert isinstance(request_payload["idempotency_key"], str)
    assert len(request_payload["idempotency_key"]) == 64
    request_payload.pop("idempotency_key")
    assert request_payload == {
        "corp_code": "00126380",
        "bsns_year": "2024",
        "reprt_code": "11011",
        "rcept_no": "20240301000001",
        "rcept_dt": "20240301",
        "fs_div": "CFS",
        "reason_code": RoutingReasonCode.CRITICAL_ROLE_MISSING.value,
        "missing_roles": ["d851100"],
        "critical_missing_roles": ["d851100"],
        "coverage_score": 0.5,
    }


def test_build_track_b_handoff_request_succeeds_on_fallback_integration() -> None:
    integration_result = build_step6_track_c_integration(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D831150", "D851100"],
        critical_roles=["D851100"],
        threshold=0.5,
    )

    request = build_track_b_handoff_request(integration_result=integration_result)

    assert request.reason_code == RoutingReasonCode.CRITICAL_ROLE_MISSING
    assert request.missing_roles == ["d851100"]
    assert request.critical_missing_roles == ["d851100"]
    assert request.coverage_score == 0.5
    assert isinstance(request.idempotency_key, str)
    assert len(request.idempotency_key) == 64


def test_build_track_b_handoff_request_rejects_track_c_integration() -> None:
    integration_result = build_step6_track_c_integration(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D822105", "D831150", "D838000"],
        critical_roles=[],
        threshold=1.0,
    )

    with pytest.raises(
        ValueError,
        match="Track B handoff request requires TRACK_B_FALLBACK route and fallback_required=true",
    ):
        build_track_b_handoff_request(integration_result=integration_result)


def test_execute_step6_with_track_b_handoff_calls_executor_once() -> None:
    captured_requests: list[dict[str, Any]] = []

    def _executor(payload) -> None:
        captured_requests.append(payload.model_dump(mode="json"))

    result = execute_step6_with_track_b_handoff(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D831150", "D851100"],
        critical_roles=["D851100"],
        threshold=0.5,
        track_b_handoff_executor=_executor,
    )

    assert result.track_b_handoff_triggered is True
    assert result.track_b_handoff_execution_result is not None
    assert result.track_b_handoff_execution_result.attempts == 1
    assert result.track_b_handoff_execution_result.max_attempts == 1
    assert (
        result.track_b_handoff_execution_result.outcome.status
        == TrackBHandoffExecutionStatus.SUCCESS
    )
    assert result.track_b_handoff_request is not None
    assert len(captured_requests) == 1
    assert captured_requests[0] == result.track_b_handoff_request.model_dump(mode="json")
    assert (
        result.track_b_handoff_execution_result.idempotency_key
        == result.track_b_handoff_request.idempotency_key
    )


def test_execute_step6_with_track_b_handoff_does_not_build_or_call_on_track_c() -> None:
    call_count = 0

    def _executor(payload) -> None:
        nonlocal call_count
        call_count += 1

    result = execute_step6_with_track_b_handoff(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D822105", "D831150", "D838000"],
        critical_roles=[],
        threshold=1.0,
        track_b_handoff_executor=_executor,
    )

    assert result.integration_result.routing_decision.route == Route.TRACK_C
    assert result.integration_result.fallback_required is False
    assert result.track_b_handoff_request is None
    assert result.track_b_handoff_triggered is False
    assert result.track_b_handoff_execution_result is None
    assert call_count == 0


def test_execute_step6_with_track_b_handoff_invalid_input_uses_empty_coverage_payload() -> None:
    result = execute_step6_with_track_b_handoff(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=None,  # type: ignore[arg-type]
        critical_roles=["D851100"],
        threshold=0.67,
    )

    assert result.integration_result.routing_decision.route == Route.TRACK_B_FALLBACK
    assert result.integration_result.routing_decision.reason_code == RoutingReasonCode.INVALID_INPUT
    assert result.integration_result.coverage_report is None
    assert result.track_b_handoff_triggered is False
    assert result.track_b_handoff_execution_result is None
    assert result.track_b_handoff_request is not None
    request_payload = result.track_b_handoff_request.model_dump(mode="json")
    assert isinstance(request_payload["idempotency_key"], str)
    assert len(request_payload["idempotency_key"]) == 64
    request_payload.pop("idempotency_key")
    assert request_payload == {
        "corp_code": "00126380",
        "bsns_year": "2024",
        "reprt_code": "11011",
        "rcept_no": "20240301000001",
        "rcept_dt": "20240301",
        "fs_div": "CFS",
        "reason_code": RoutingReasonCode.INVALID_INPUT.value,
        "missing_roles": [],
        "critical_missing_roles": [],
        "coverage_score": 0.0,
    }


def test_execute_step6_with_track_b_handoff_idempotency_key_is_deterministic() -> None:
    first = execute_step6_with_track_b_handoff(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D831150", "D851100"],
        critical_roles=["D851100"],
        threshold=0.5,
    )
    second = execute_step6_with_track_b_handoff(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D831150", "D851100"],
        critical_roles=["D851100"],
        threshold=0.5,
    )

    assert first.track_b_handoff_request is not None
    assert second.track_b_handoff_request is not None
    assert (
        first.track_b_handoff_request.idempotency_key
        == second.track_b_handoff_request.idempotency_key
    )


def test_execute_step6_with_track_b_handoff_retries_then_succeeds() -> None:
    attempt_statuses: list[TrackBHandoffExecutionStatus] = []

    def _executor(_payload):
        if not attempt_statuses:
            attempt_statuses.append(TrackBHandoffExecutionStatus.RETRYABLE_ERROR)
            return TrackBHandoffExecutorOutcome(
                status=TrackBHandoffExecutionStatus.RETRYABLE_ERROR,
                error_code="TEMP_DOWNSTREAM",
                error_message="temporary failure",
                retry_after_seconds=1.0,
            )

        attempt_statuses.append(TrackBHandoffExecutionStatus.SUCCESS)
        return None

    result = execute_step6_with_track_b_handoff(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D831150", "D851100"],
        critical_roles=["D851100"],
        threshold=0.5,
        track_b_handoff_executor=_executor,
        max_handoff_attempts=3,
    )

    assert attempt_statuses == [
        TrackBHandoffExecutionStatus.RETRYABLE_ERROR,
        TrackBHandoffExecutionStatus.SUCCESS,
    ]
    assert result.track_b_handoff_execution_result is not None
    assert result.track_b_handoff_execution_result.attempts == 2
    assert result.track_b_handoff_execution_result.max_attempts == 3
    assert (
        result.track_b_handoff_execution_result.outcome.status
        == TrackBHandoffExecutionStatus.SUCCESS
    )


def test_execute_step6_with_track_b_handoff_stops_after_retryable_error_exhaustion() -> None:
    call_count = 0

    def _executor(_payload):
        nonlocal call_count
        call_count += 1
        return TrackBHandoffExecutorOutcome(
            status=TrackBHandoffExecutionStatus.RETRYABLE_ERROR,
            error_code="TEMP_DOWNSTREAM",
            error_message="still failing",
            retry_after_seconds=0.5,
        )

    result = execute_step6_with_track_b_handoff(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D831150", "D851100"],
        critical_roles=["D851100"],
        threshold=0.5,
        track_b_handoff_executor=_executor,
        max_handoff_attempts=3,
    )

    assert call_count == 3
    assert result.track_b_handoff_execution_result is not None
    assert result.track_b_handoff_execution_result.attempts == 3
    assert result.track_b_handoff_execution_result.max_attempts == 3
    assert (
        result.track_b_handoff_execution_result.outcome.status
        == TrackBHandoffExecutionStatus.RETRYABLE_ERROR
    )


def test_execute_step6_with_track_b_handoff_stops_on_permanent_error() -> None:
    call_count = 0

    def _executor(_payload):
        nonlocal call_count
        call_count += 1
        return TrackBHandoffExecutorOutcome(
            status=TrackBHandoffExecutionStatus.PERMANENT_ERROR,
            error_code="INVALID_PAYLOAD",
            error_message="schema mismatch",
        )

    result = execute_step6_with_track_b_handoff(
        dart_api_client=_integration_client(),
        corp_code="00126380",
        bsns_year="2024",
        xbrl_dir=TRACK_C_FIXTURE_DIR,
        required_roles=["D831150", "D851100"],
        critical_roles=["D851100"],
        threshold=0.5,
        track_b_handoff_executor=_executor,
        max_handoff_attempts=5,
    )

    assert call_count == 1
    assert result.track_b_handoff_execution_result is not None
    assert result.track_b_handoff_execution_result.attempts == 1
    assert result.track_b_handoff_execution_result.max_attempts == 5
    assert (
        result.track_b_handoff_execution_result.outcome.status
        == TrackBHandoffExecutionStatus.PERMANENT_ERROR
    )


def test_execute_step6_with_track_b_handoff_rejects_invalid_max_attempts() -> None:
    with pytest.raises(ValueError, match="max_handoff_attempts must be >= 1"):
        execute_step6_with_track_b_handoff(
            dart_api_client=_integration_client(),
            corp_code="00126380",
            bsns_year="2024",
            xbrl_dir=TRACK_C_FIXTURE_DIR,
            required_roles=["D831150", "D851100"],
            critical_roles=["D851100"],
            threshold=0.5,
            track_b_handoff_executor=lambda _payload: None,
            max_handoff_attempts=0,
        )
