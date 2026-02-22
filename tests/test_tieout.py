from typing import Dict, Optional

import pytest

from dart_pipeline.contracts import TieOutReasonCode, TieOutStatus
from dart_pipeline.validation.tieout import run_tieout


def _row(
    *,
    metric: str,
    period: str = "2024Q4",
    unit: str = "KRW",
    dimensions: Optional[Dict[str, str]] = None,
    value: float,
):
    return {
        "metric": metric,
        "period": period,
        "unit": unit,
        "dimensions": dimensions or {},
        "value": value,
    }


def test_tieout_exact_match() -> None:
    result = run_tieout(
        expected=[_row(metric="revenue", value=100.0)],
        observed=[_row(metric="revenue", value=100.0)],
        abs_tol=0.5,
        rel_tol=0.0,
        warn_multiplier=2.0,
    )

    assert result.summary.pass_count == 1
    assert result.summary.warn_count == 0
    assert result.summary.fail_count == 0
    assert result.items[0].status == TieOutStatus.PASS
    assert result.items[0].reason_code == TieOutReasonCode.OK


def test_tieout_boundary_equality_is_pass() -> None:
    result = run_tieout(
        expected=[_row(metric="assets", value=100.0)],
        observed=[_row(metric="assets", value=100.5)],
        abs_tol=0.5,
        rel_tol=0.0,
        warn_multiplier=3.0,
    )

    assert result.items[0].abs_diff == 0.5
    assert result.items[0].tolerance == 0.5
    assert result.items[0].status == TieOutStatus.PASS


def test_tieout_negative_values_use_absolute_expected_for_tolerance() -> None:
    result = run_tieout(
        expected=[_row(metric="expense", value=-100.0)],
        observed=[_row(metric="expense", value=-100.15)],
        abs_tol=0.01,
        rel_tol=0.001,
        warn_multiplier=2.0,
    )

    assert result.items[0].tolerance == pytest.approx(0.1)
    assert result.items[0].abs_diff == pytest.approx(0.15)
    assert result.items[0].status == TieOutStatus.WARN
    assert result.items[0].reason_code == TieOutReasonCode.OUT_OF_TOLERANCE_WARN


def test_tieout_zero_baseline_uses_minimum_relative_base() -> None:
    result = run_tieout(
        expected=[_row(metric="tiny", value=0.0)],
        observed=[_row(metric="tiny", value=5e-13)],
        abs_tol=0.0,
        rel_tol=1.0,
        warn_multiplier=2.0,
    )

    assert result.items[0].tolerance == 1e-12
    assert result.items[0].status == TieOutStatus.PASS


def test_tieout_missing_observed_is_fail() -> None:
    result = run_tieout(
        expected=[_row(metric="liability", value=50.0)],
        observed=[],
        abs_tol=0.1,
        rel_tol=0.0,
        warn_multiplier=2.0,
    )

    assert result.summary.fail_count == 1
    assert result.items[0].status == TieOutStatus.FAIL
    assert result.items[0].reason_code == TieOutReasonCode.MISSING_OBSERVED
    assert result.items[0].observed is None


def test_tieout_extra_observed_is_warn() -> None:
    result = run_tieout(
        expected=[],
        observed=[_row(metric="equity", value=75.0)],
        abs_tol=0.1,
        rel_tol=0.0,
        warn_multiplier=2.0,
    )

    assert result.summary.warn_count == 1
    assert result.items[0].status == TieOutStatus.WARN
    assert result.items[0].reason_code == TieOutReasonCode.EXTRA_OBSERVED
    assert result.items[0].expected is None


def test_tieout_warn_and_fail_boundaries() -> None:
    warn_result = run_tieout(
        expected=[_row(metric="cash", value=100.0)],
        observed=[_row(metric="cash", value=101.0)],
        abs_tol=0.5,
        rel_tol=0.0,
        warn_multiplier=2.0,
    )
    fail_result = run_tieout(
        expected=[_row(metric="cash", value=100.0)],
        observed=[_row(metric="cash", value=101.0001)],
        abs_tol=0.5,
        rel_tol=0.0,
        warn_multiplier=2.0,
    )

    assert warn_result.items[0].abs_diff == pytest.approx(1.0)
    assert warn_result.items[0].status == TieOutStatus.WARN
    assert fail_result.items[0].status == TieOutStatus.FAIL
    assert fail_result.items[0].reason_code == TieOutReasonCode.OUT_OF_TOLERANCE_FAIL


def test_tieout_dimensions_order_invariance_and_output_ordering() -> None:
    expected = [
        _row(metric="b_metric", dimensions={"z": "2", "a": "1"}, value=2.0),
        _row(metric="a_metric", dimensions={"region": "KR", "segment": "A"}, value=1.0),
    ]
    observed = [
        _row(metric="a_metric", dimensions={"segment": "A", "region": "KR"}, value=1.0),
        _row(metric="b_metric", dimensions={"a": "1", "z": "2"}, value=2.0),
    ]

    result = run_tieout(
        expected=expected,
        observed=observed,
        abs_tol=0.0,
        rel_tol=0.0,
        warn_multiplier=2.0,
    )

    assert result.summary.pass_count == 2
    assert [item.metric for item in result.items] == ["a_metric", "b_metric"]


@pytest.mark.parametrize(
    ("abs_tol", "rel_tol", "warn_multiplier", "expected_message"),
    [
        (float("nan"), 0.0, 2.0, "abs_tol must be finite"),
        (0.0, float("inf"), 2.0, "rel_tol must be finite"),
        (0.0, 0.0, float("nan"), "warn_multiplier must be finite"),
        (0.0, 0.0, float("inf"), "warn_multiplier must be finite"),
    ],
)
def test_tieout_rejects_non_finite_tolerances(
    abs_tol: float,
    rel_tol: float,
    warn_multiplier: float,
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        run_tieout(
            expected=[],
            observed=[],
            abs_tol=abs_tol,
            rel_tol=rel_tol,
            warn_multiplier=warn_multiplier,
        )


@pytest.mark.parametrize(
    ("expected", "observed", "error_message"),
    [
        (None, [], "expected must be an iterable of mappings"),
        ([], None, "observed must be an iterable of mappings"),
        ([None], [], r"expected\[0\] must be a mapping"),
        (
            [{"metric": 1, "period": "2024Q4", "unit": "KRW", "dimensions": {}, "value": 1.0}],
            [],
            r"expected\[0\]\.metric must be str",
        ),
        (
            [{"metric": "revenue", "period": "2024Q4", "unit": "KRW", "dimensions": [], "value": 1.0}],
            [],
            r"expected\[0\]\.dimensions must be a mapping of str to str",
        ),
        (
            [{"metric": "revenue", "period": "2024Q4", "unit": "KRW", "dimensions": {}, "value": None}],
            [],
            r"expected\[0\] must include numeric value",
        ),
    ],
)
def test_tieout_rejects_null_or_invalid_input_shapes(
    expected,
    observed,
    error_message: str,
) -> None:
    with pytest.raises(ValueError, match=error_message):
        run_tieout(
            expected=expected,
            observed=observed,
            abs_tol=0.1,
            rel_tol=0.0,
            warn_multiplier=2.0,
        )


@pytest.mark.parametrize("role", ["expected", "observed"])
def test_tieout_rejects_duplicate_identity_keys(role: str) -> None:
    duplicate_rows = [_row(metric="revenue", value=10.0), _row(metric="revenue", value=11.0)]
    expected = duplicate_rows if role == "expected" else [_row(metric="assets", value=20.0)]
    observed = duplicate_rows if role == "observed" else [_row(metric="assets", value=20.0)]

    with pytest.raises(ValueError, match=f"duplicate identity key in {role}:"):
        run_tieout(
            expected=expected,
            observed=observed,
            abs_tol=0.1,
            rel_tol=0.0,
            warn_multiplier=2.0,
        )


def test_tieout_mixed_presence_ordering_is_deterministic() -> None:
    result = run_tieout(
        expected=[
            _row(metric="b_match", value=10.0),
            _row(metric="c_missing", value=20.0),
        ],
        observed=[
            _row(metric="a_extra", value=7.0),
            _row(metric="b_match", value=10.0),
        ],
        abs_tol=0.0,
        rel_tol=0.0,
        warn_multiplier=2.0,
    )

    assert [item.metric for item in result.items] == ["a_extra", "b_match", "c_missing"]
    assert [item.reason_code for item in result.items] == [
        TieOutReasonCode.EXTRA_OBSERVED,
        TieOutReasonCode.OK,
        TieOutReasonCode.MISSING_OBSERVED,
    ]
