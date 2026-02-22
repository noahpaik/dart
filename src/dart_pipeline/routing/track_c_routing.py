from __future__ import annotations

from collections.abc import Mapping

from dart_pipeline.contracts import CoverageReport, RoutingDecision, XbrlNote
from dart_pipeline.routing.coverage_router import route_by_coverage


def _extract_found_roles(parsed_notes: list[XbrlNote] | None) -> list[str] | None:
    if not isinstance(parsed_notes, list):
        return None

    found_roles: set[str] = set()
    for note in parsed_notes:
        if not isinstance(note, XbrlNote):
            return None
        found_roles.add(note.role_code)

    return sorted(found_roles)


def route_from_track_c_roles(
    parsed_notes: list[XbrlNote] | None,
    required_roles: list[str] | None,
    critical_roles: list[str] | None,
    threshold: float | None,
    role_aliases: Mapping[str, str] | None = None,
) -> tuple[RoutingDecision, CoverageReport | None]:
    found_roles = _extract_found_roles(parsed_notes)
    return route_by_coverage(
        required_roles=required_roles,
        found_roles=found_roles,
        critical_roles=critical_roles,
        threshold=threshold,
        role_aliases=role_aliases,
    )


__all__ = ["route_from_track_c_roles"]
