from __future__ import annotations

from dart_pipeline.contracts import (
    Route,
    RoutingReasonCode,
    XbrlAccountRef,
    XbrlMemberRef,
    XbrlNote,
    XbrlSource,
)
from dart_pipeline.routing.track_c_routing import route_from_track_c_roles


def _note(role_code: str, role_name: str) -> XbrlNote:
    return XbrlNote(
        role_code=role_code,
        role_name=role_name,
        accounts=[
            XbrlAccountRef(
                account_id="dart_SalariesWages",
                label_ko="급여",
                label_en="Salaries and wages",
                source=XbrlSource.DART,
            )
        ],
        members=[
            XbrlMemberRef(
                account_id="entity00134477_DomesticMember",
                label_ko="국내",
                source=XbrlSource.COMPANY,
            )
        ],
    )


def test_route_from_track_c_roles_critical_missing_override() -> None:
    decision, report = route_from_track_c_roles(
        parsed_notes=[_note("D831150", "수익분해")],
        required_roles=["D831150", "D851100"],
        critical_roles=["D851100"],
        threshold=0.5,
    )

    assert report is not None
    assert report.coverage_score == 0.5
    assert report.critical_missing_roles == ["d851100"]
    assert decision.route == Route.TRACK_B_FALLBACK
    assert decision.reason_code == RoutingReasonCode.CRITICAL_ROLE_MISSING


def test_route_from_track_c_roles_threshold_boundary_passes() -> None:
    decision, report = route_from_track_c_roles(
        parsed_notes=[_note("D831150", "수익분해")],
        required_roles=["D831150", "D851100"],
        critical_roles=[],
        threshold=0.5,
    )

    assert report is not None
    assert report.coverage_score == 0.5
    assert report.missing_roles == ["d851100"]
    assert decision.route == Route.TRACK_C
    assert decision.reason_code == RoutingReasonCode.COVERAGE_PASS


def test_route_from_track_c_roles_alias_support() -> None:
    decision, report = route_from_track_c_roles(
        parsed_notes=[_note("D831150", "수익분해")],
        required_roles=["revenue_note"],
        critical_roles=[],
        threshold=1.0,
        role_aliases={"revenue_note": "d831150"},
    )

    assert report is not None
    assert report.required_roles == ["d831150"]
    assert report.found_roles == ["d831150"]
    assert decision.route == Route.TRACK_C
    assert decision.reason_code == RoutingReasonCode.COVERAGE_PASS


def test_route_from_track_c_roles_invalid_input() -> None:
    decision, report = route_from_track_c_roles(
        parsed_notes=None,
        required_roles=["D831150"],
        critical_roles=[],
        threshold=0.5,
    )

    assert report is None
    assert decision.route == Route.TRACK_B_FALLBACK
    assert decision.reason_code == RoutingReasonCode.INVALID_INPUT
