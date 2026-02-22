import pytest

from dart_pipeline.contracts import Route, RoutingReasonCode
from dart_pipeline.routing import route_by_coverage


def _assert_invalid_input(decision, report) -> None:
    assert report is None
    assert decision.route == Route.TRACK_B_FALLBACK
    assert decision.reason_code == RoutingReasonCode.INVALID_INPUT


def test_route_by_coverage_empty_required_roles() -> None:
    decision, report = route_by_coverage(
        required_roles=[],
        found_roles=["income_statement"],
        critical_roles=["balance_sheet"],
        threshold=0.9,
    )

    assert report is not None
    assert report.coverage_score == 1.0
    assert report.required_roles == []
    assert report.missing_roles == []
    assert report.critical_missing_roles == []
    assert decision.route == Route.TRACK_C
    assert decision.reason_code == RoutingReasonCode.COVERAGE_PASS


def test_route_by_coverage_critical_override_even_when_coverage_high() -> None:
    decision, report = route_by_coverage(
        required_roles=["income_statement", "balance_sheet", "cash_flow"],
        found_roles=["income_statement", "balance_sheet"],
        critical_roles=["cash_flow"],
        threshold=0.5,
    )

    assert report is not None
    assert report.coverage_score == 2 / 3
    assert report.critical_missing_roles == ["cash_flow"]
    assert decision.route == Route.TRACK_B_FALLBACK
    assert decision.reason_code == RoutingReasonCode.CRITICAL_ROLE_MISSING


def test_route_by_coverage_threshold_boundary_equality_passes() -> None:
    decision, report = route_by_coverage(
        required_roles=["income_statement", "balance_sheet"],
        found_roles=["income_statement"],
        critical_roles=[],
        threshold=0.5,
    )

    assert report is not None
    assert report.coverage_score == 0.5
    assert decision.route == Route.TRACK_C
    assert decision.reason_code == RoutingReasonCode.COVERAGE_PASS


def test_route_by_coverage_non_critical_below_threshold_routes_fallback() -> None:
    decision, report = route_by_coverage(
        required_roles=["income_statement", "balance_sheet"],
        found_roles=["income_statement"],
        critical_roles=["income_statement"],
        threshold=0.75,
    )

    assert report is not None
    assert report.coverage_score == 0.5
    assert report.critical_missing_roles == []
    assert decision.route == Route.TRACK_B_FALLBACK
    assert decision.reason_code == RoutingReasonCode.COVERAGE_BELOW_THRESHOLD


def test_route_by_coverage_alias_normalization_applies_to_required_and_found() -> None:
    decision, report = route_by_coverage(
        required_roles=[" Balance_Sheet ", "INCOME_STATEMENT"],
        found_roles=["  bs", "is  "],
        critical_roles=[],
        threshold=1.0,
        role_aliases={"bs": "balance_sheet", "is": "income_statement"},
    )

    assert report is not None
    assert report.required_roles == ["balance_sheet", "income_statement"]
    assert report.found_roles == ["balance_sheet", "income_statement"]
    assert report.missing_roles == []
    assert report.coverage_score == 1.0
    assert decision.route == Route.TRACK_C
    assert decision.reason_code == RoutingReasonCode.COVERAGE_PASS


def test_route_by_coverage_invalid_threshold_returns_invalid_decision_with_report() -> None:
    decision, report = route_by_coverage(
        required_roles=["income_statement"],
        found_roles=["income_statement"],
        critical_roles=[],
        threshold=1.5,
    )

    assert report is not None
    assert report.coverage_score == 1.0
    assert decision.route == Route.TRACK_B_FALLBACK
    assert decision.reason_code == RoutingReasonCode.INVALID_INPUT


def test_route_by_coverage_non_string_role_entries_return_invalid_decision() -> None:
    decision, report = route_by_coverage(
        required_roles=["income_statement", 1],  # type: ignore[list-item]
        found_roles=["income_statement"],
        critical_roles=[],
        threshold=0.5,
    )

    _assert_invalid_input(decision, report)


@pytest.mark.parametrize(
    "payload",
    [
        {
            "found_roles": ["income_statement"],
            "critical_roles": [],
            "threshold": 0.5,
        },
        {
            "required_roles": None,
            "found_roles": ["income_statement"],
            "critical_roles": [],
            "threshold": 0.5,
        },
        {
            "required_roles": ("income_statement",),
            "found_roles": ["income_statement"],
            "critical_roles": [],
            "threshold": 0.5,
        },
    ],
    ids=["required_roles_missing", "required_roles_none", "required_roles_tuple"],
)
def test_route_by_coverage_invalid_required_roles_matrix_returns_invalid_input(
    payload: dict[str, object],
) -> None:
    decision, report = route_by_coverage(**payload)
    _assert_invalid_input(decision, report)


def _invalid_non_list_value(kind: str):
    if kind == "tuple":
        return ("income_statement",)
    if kind == "set":
        return {"income_statement"}
    if kind == "generator":
        return (role for role in ["income_statement"])
    if kind == "str":
        return "income_statement"
    if kind == "none":
        return None
    raise ValueError(f"unsupported invalid kind: {kind}")


@pytest.mark.parametrize("role_field", ["required_roles", "found_roles", "critical_roles"])
@pytest.mark.parametrize("invalid_kind", ["tuple", "set", "generator", "str", "none"])
def test_route_by_coverage_non_list_role_inputs_are_invalid_input(
    role_field: str,
    invalid_kind: str,
) -> None:
    payload: dict[str, object] = {
        "required_roles": ["income_statement"],
        "found_roles": ["income_statement"],
        "critical_roles": [],
        "threshold": 0.5,
    }
    payload[role_field] = _invalid_non_list_value(invalid_kind)

    decision, report = route_by_coverage(**payload)
    _assert_invalid_input(decision, report)
