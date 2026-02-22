import pytest
from pydantic import ValidationError

from dart_pipeline.contracts import (
    CoverageReport,
    Route,
    RoutingDecision,
    RoutingReasonCode,
    TieOutItem,
    TieOutReasonCode,
    TieOutResult,
    TieOutStatus,
    TieOutSummary,
    TrackARow,
    TrackASnapshot,
    TimeSeriesView,
    ViewType,
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
