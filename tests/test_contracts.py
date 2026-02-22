import pytest
from pydantic import ValidationError

from dart_pipeline.contracts import (
    CoverageReport,
    Route,
    RoutingDecision,
    RoutingReasonCode,
    Step6ExecutionResult,
    Step6TrackCIntegrationResult,
    TrackBHandoffExecutionResult,
    TrackBHandoffExecutionStatus,
    TrackBHandoffExecutorOutcome,
    TrackBHandoffRequest,
    TieOutItem,
    TieOutReasonCode,
    TieOutResult,
    TieOutStatus,
    TieOutSummary,
    TrackARow,
    TrackASnapshot,
    TimeSeriesView,
    ViewType,
    XbrlNote,
    canonical_identity_key,
)


def test_tieout_result_roundtrip_and_identity_key_order_invariance() -> None:
    item_pass = TieOutItem(
        metric="revenue",
        period="2024Q4",
        unit="KRW",
        dimensions={"segment": "A", "region": "KR"},
        expected=10.0,
        observed=10.0,
        diff=0.0,
        abs_diff=0.0,
        tolerance=0.1,
        status=TieOutStatus.PASS,
        reason_code=TieOutReasonCode.OK,
    )
    item_warn = TieOutItem(
        metric="assets",
        period="2024Q4",
        unit="KRW",
        dimensions={"segment": "B"},
        expected=20.0,
        observed=20.3,
        diff=0.3,
        abs_diff=0.3,
        tolerance=0.1,
        status=TieOutStatus.WARN,
        reason_code=TieOutReasonCode.OUT_OF_TOLERANCE_WARN,
    )
    item_fail = TieOutItem(
        metric="liabilities",
        period="2024Q4",
        unit="KRW",
        dimensions={"segment": "C"},
        expected=30.0,
        observed=31.0,
        diff=1.0,
        abs_diff=1.0,
        tolerance=0.1,
        status=TieOutStatus.FAIL,
        reason_code=TieOutReasonCode.OUT_OF_TOLERANCE_FAIL,
    )

    result = TieOutResult(
        summary=TieOutSummary(pass_count=1, warn_count=1, fail_count=1),
        items=[item_pass, item_warn, item_fail],
    )

    raw = result.model_dump_json()
    restored = TieOutResult.model_validate_json(raw)
    assert restored == result

    reordered = TieOutItem(
        metric="revenue",
        period="2024Q4",
        unit="KRW",
        dimensions={"region": "KR", "segment": "A"},
        expected=10.0,
        observed=10.0,
        diff=0.0,
        abs_diff=0.0,
        tolerance=0.1,
        status=TieOutStatus.PASS,
        reason_code=TieOutReasonCode.OK,
    )
    assert reordered.identity_key == item_pass.identity_key
    assert reordered.identity_key == canonical_identity_key(
        metric="revenue",
        period="2024Q4",
        unit="KRW",
        dimensions={"segment": "A", "region": "KR"},
    )


def test_coverage_report_roundtrip() -> None:
    report = CoverageReport(
        required_roles=["income_statement", "balance_sheet"],
        found_roles=["income_statement"],
        missing_roles=["balance_sheet"],
        critical_missing_roles=["balance_sheet"],
        coverage_score=0.5,
    )

    raw = report.model_dump_json()
    restored = CoverageReport.model_validate_json(raw)
    assert restored == report


def test_routing_decision_roundtrip_and_invalid_input_behavior() -> None:
    passed = RoutingDecision(
        route=Route.TRACK_C,
        reason_code=RoutingReasonCode.COVERAGE_PASS,
    )
    raw = passed.model_dump_json()
    restored = RoutingDecision.model_validate_json(raw)
    assert restored == passed

    invalid_input = RoutingDecision.invalid_input()
    assert invalid_input.route == Route.TRACK_B_FALLBACK
    assert invalid_input.reason_code == RoutingReasonCode.INVALID_INPUT

    with pytest.raises(ValidationError):
        RoutingDecision(
            route=Route.TRACK_C,
            reason_code=RoutingReasonCode.INVALID_INPUT,
        )


def test_time_series_view_roundtrip() -> None:
    view = TimeSeriesView(
        view_type=ViewType.AS_LATEST,
        records=[
            {
                "metric": "revenue",
                "period": "2024Q4",
                "unit": "KRW",
                "value": 100.0,
                "dimensions": {"segment": "A"},
            }
        ],
    )

    raw = view.model_dump_json()
    restored = TimeSeriesView.model_validate_json(raw)
    assert restored == view


def test_strict_validation_errors() -> None:
    with pytest.raises(ValidationError):
        CoverageReport(
            required_roles=["income_statement"],
            found_roles=["income_statement"],
            missing_roles=[],
            critical_missing_roles=[],
            coverage_score="1.0",  # type: ignore[arg-type]
        )

    with pytest.raises(ValidationError):
        CoverageReport(
            required_roles=["income_statement"],
            found_roles=["income_statement"],
            missing_roles=[],
            critical_missing_roles=[],
            coverage_score=1.2,
        )

    with pytest.raises(ValidationError):
        TieOutItem(
            metric="revenue",
            period="2024Q4",
            unit="KRW",
            dimensions={"segment": "A"},
            expected=10.0,
            observed=10.0,
            diff=0.0,
            abs_diff=0.0,
            tolerance=0.1,
            status=TieOutStatus.PASS,
            reason_code=TieOutReasonCode.MISSING_OBSERVED,
        )

    with pytest.raises(ValidationError):
        TieOutResult(
            summary=TieOutSummary(pass_count=0, warn_count=1, fail_count=0),
            items=[
                TieOutItem(
                    metric="revenue",
                    period="2024Q4",
                    unit="KRW",
                    dimensions={"segment": "A"},
                    expected=10.0,
                    observed=10.0,
                    diff=0.0,
                    abs_diff=0.0,
                    tolerance=0.1,
                    status=TieOutStatus.PASS,
                    reason_code=TieOutReasonCode.OK,
                )
            ],
        )


def test_track_a_snapshot_roundtrip_and_deterministic_row_sorting() -> None:
    row_b = TrackARow(
        corp_code="00126380",
        rcept_no="20240301000001",
        rcept_dt="20240301",
        bsns_year="2024",
        reprt_code="11011",
        fs_div="CFS",
        sj_div="IS",
        account_id="ifrs-full_Revenue",
        account_nm="Revenue",
        ord=2,
        source_row_idx=0,
        thstrm_amount_raw="1000",
        thstrm_amount=1000,
    )
    row_a = TrackARow(
        corp_code="00126380",
        rcept_no="20240301000001",
        rcept_dt="20240301",
        bsns_year="2024",
        reprt_code="11011",
        fs_div="CFS",
        sj_div="BS",
        account_id="ifrs-full_Assets",
        account_nm="Assets",
        ord=1,
        source_row_idx=1,
        thstrm_amount_raw="1,234.50",
        thstrm_amount=1234,
    )

    snapshot = TrackASnapshot(
        corp_code="00126380",
        rcept_no="20240301000001",
        rcept_dt="20240301",
        bsns_year="2024",
        reprt_code="11011",
        fs_div="CFS",
        rows=[row_b, row_a],
    )

    assert [row.account_id for row in snapshot.rows] == [
        "ifrs-full_Assets",
        "ifrs-full_Revenue",
    ]

    raw = snapshot.model_dump_json()
    restored = TrackASnapshot.model_validate_json(raw)
    assert restored == snapshot


def test_step6_execution_handoff_contract_consistency() -> None:
    row = TrackARow(
        corp_code="00126380",
        rcept_no="20240301000001",
        rcept_dt="20240301",
        bsns_year="2024",
        reprt_code="11011",
        fs_div="CFS",
        sj_div="BS",
        account_id="ifrs-full_Assets",
        account_nm="Assets",
        ord=1,
        source_row_idx=0,
    )
    snapshot = TrackASnapshot(
        corp_code="00126380",
        rcept_no="20240301000001",
        rcept_dt="20240301",
        bsns_year="2024",
        reprt_code="11011",
        fs_div="CFS",
        rows=[row],
    )
    coverage_report = CoverageReport(
        required_roles=["d831150", "d851100"],
        found_roles=["d831150"],
        missing_roles=["d851100"],
        critical_missing_roles=["d851100"],
        coverage_score=0.5,
    )
    fallback_integration = Step6TrackCIntegrationResult(
        track_a_snapshot=snapshot,
        track_c_notes=[],
        routing_decision=RoutingDecision(
            route=Route.TRACK_B_FALLBACK,
            reason_code=RoutingReasonCode.CRITICAL_ROLE_MISSING,
        ),
        coverage_report=coverage_report,
        fallback_required=True,
    )
    handoff_request = TrackBHandoffRequest(
        corp_code="00126380",
        bsns_year="2024",
        reprt_code="11011",
        rcept_no="20240301000001",
        rcept_dt="20240301",
        fs_div="CFS",
        idempotency_key="idem-key-1",
        reason_code=RoutingReasonCode.CRITICAL_ROLE_MISSING,
        missing_roles=["d851100"],
        critical_missing_roles=["d851100"],
        coverage_score=0.5,
    )
    execution_result = TrackBHandoffExecutionResult(
        idempotency_key="idem-key-1",
        attempts=1,
        max_attempts=3,
        outcome=TrackBHandoffExecutorOutcome(
            status=TrackBHandoffExecutionStatus.RETRYABLE_ERROR,
            error_code="TEMP_DOWNSTREAM",
            error_message="temporary failure",
            retry_after_seconds=2.0,
        ),
    )

    execution = Step6ExecutionResult(
        integration_result=fallback_integration,
        track_b_handoff_request=handoff_request,
        track_b_handoff_triggered=True,
        track_b_handoff_execution_result=execution_result,
    )
    assert execution.track_b_handoff_triggered is True

    with pytest.raises(ValidationError):
        Step6ExecutionResult(
            integration_result=fallback_integration,
            track_b_handoff_request=None,
            track_b_handoff_triggered=False,
        )

    with pytest.raises(ValidationError):
        Step6ExecutionResult(
            integration_result=fallback_integration,
            track_b_handoff_request=handoff_request,
            track_b_handoff_triggered=True,
            track_b_handoff_execution_result=None,
        )

    with pytest.raises(ValidationError):
        Step6ExecutionResult(
            integration_result=fallback_integration,
            track_b_handoff_request=handoff_request,
            track_b_handoff_triggered=False,
            track_b_handoff_execution_result=execution_result,
        )

    with pytest.raises(ValidationError):
        Step6ExecutionResult(
            integration_result=fallback_integration,
            track_b_handoff_request=TrackBHandoffRequest(
                corp_code="00126380",
                bsns_year="2024",
                reprt_code="11011",
                rcept_no="20240301000001",
                rcept_dt="20240301",
                fs_div="CFS",
                idempotency_key="idem-key-2",
                reason_code=RoutingReasonCode.CRITICAL_ROLE_MISSING,
                missing_roles=["d851100"],
                critical_missing_roles=["d851100"],
                coverage_score=0.5,
            ),
            track_b_handoff_triggered=True,
            track_b_handoff_execution_result=execution_result,
        )

    track_c_integration = Step6TrackCIntegrationResult(
        track_a_snapshot=snapshot,
        track_c_notes=[XbrlNote(role_code="D831150", role_name="note", accounts=[], members=[])],
        routing_decision=RoutingDecision(
            route=Route.TRACK_C,
            reason_code=RoutingReasonCode.COVERAGE_PASS,
        ),
        coverage_report=CoverageReport(
            required_roles=["d831150"],
            found_roles=["d831150"],
            missing_roles=[],
            critical_missing_roles=[],
            coverage_score=1.0,
        ),
        fallback_required=False,
    )
    with pytest.raises(ValidationError):
        Step6ExecutionResult(
            integration_result=track_c_integration,
            track_b_handoff_request=handoff_request,
            track_b_handoff_triggered=False,
        )

    with pytest.raises(ValidationError):
        TrackBHandoffRequest(
            corp_code="00126380",
            bsns_year="2024",
            reprt_code="11011",
            rcept_no="20240301000001",
            rcept_dt="20240301",
            fs_div="CFS",
            idempotency_key="idem-key-1",
            reason_code=RoutingReasonCode.COVERAGE_PASS,
            missing_roles=[],
            critical_missing_roles=[],
            coverage_score=1.0,
        )


def test_track_b_handoff_executor_outcome_contracts() -> None:
    success = TrackBHandoffExecutorOutcome(
        status=TrackBHandoffExecutionStatus.SUCCESS
    )
    assert success.error_code is None

    with pytest.raises(ValidationError):
        TrackBHandoffExecutorOutcome(
            status=TrackBHandoffExecutionStatus.SUCCESS,
            error_code="ERR_SHOULD_NOT_EXIST",
        )

    retryable = TrackBHandoffExecutorOutcome(
        status=TrackBHandoffExecutionStatus.RETRYABLE_ERROR,
        error_code="TEMP_FAIL",
        retry_after_seconds=1.5,
    )
    assert retryable.retry_after_seconds == 1.5

    with pytest.raises(ValidationError):
        TrackBHandoffExecutorOutcome(
            status=TrackBHandoffExecutionStatus.RETRYABLE_ERROR,
        )

    permanent = TrackBHandoffExecutorOutcome(
        status=TrackBHandoffExecutionStatus.PERMANENT_ERROR,
        error_code="INVALID_PAYLOAD",
        error_message="cannot deserialize",
    )
    assert permanent.error_code == "INVALID_PAYLOAD"

    with pytest.raises(ValidationError):
        TrackBHandoffExecutorOutcome(
            status=TrackBHandoffExecutionStatus.PERMANENT_ERROR,
            error_code="INVALID_PAYLOAD",
            retry_after_seconds=2.0,
        )


def test_track_b_handoff_execution_result_contracts() -> None:
    result = TrackBHandoffExecutionResult(
        idempotency_key="idem-key-1",
        attempts=2,
        max_attempts=3,
        outcome=TrackBHandoffExecutorOutcome(
            status=TrackBHandoffExecutionStatus.RETRYABLE_ERROR,
            error_code="TEMP_FAIL",
            retry_after_seconds=0.5,
        ),
    )
    assert result.attempts == 2

    with pytest.raises(ValidationError):
        TrackBHandoffExecutionResult(
            idempotency_key="idem-key-1",
            attempts=4,
            max_attempts=3,
            outcome=TrackBHandoffExecutorOutcome(
                status=TrackBHandoffExecutionStatus.RETRYABLE_ERROR,
                error_code="TEMP_FAIL",
                retry_after_seconds=0.5,
            ),
        )

    with pytest.raises(ValidationError):
        TrackBHandoffExecutionResult(
            idempotency_key="",
            attempts=1,
            max_attempts=1,
            outcome=TrackBHandoffExecutorOutcome(
                status=TrackBHandoffExecutionStatus.SUCCESS,
            ),
        )
