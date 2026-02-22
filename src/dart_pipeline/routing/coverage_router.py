from __future__ import annotations

import math
from collections.abc import Mapping

from dart_pipeline.contracts import (
    CoverageReport,
    Route,
    RoutingDecision,
    RoutingReasonCode,
)


def _normalize_alias_map(role_aliases: Mapping[str, str] | None) -> dict[str, str]:
    if role_aliases is None:
        return {}
    if not isinstance(role_aliases, Mapping):
        raise ValueError("role_aliases must be a mapping of str to str")

    normalized_aliases: dict[str, str] = {}
    for raw_alias, raw_canonical in role_aliases.items():
        if not isinstance(raw_alias, str) or not isinstance(raw_canonical, str):
            raise ValueError("role_aliases must be a mapping of str to str")
        alias = raw_alias.strip().lower()
        canonical = raw_canonical.strip().lower()
        normalized_aliases[alias] = canonical

    resolved_aliases: dict[str, str] = {}

    def resolve(alias: str) -> str:
        seen: set[str] = set()
        current = alias
        while current in normalized_aliases:
            if current in seen:
                raise ValueError("role_aliases contains a cycle")
            seen.add(current)
            current = normalized_aliases[current]
        return current

    for alias in normalized_aliases:
        resolved_aliases[alias] = resolve(alias)

    return resolved_aliases


def _normalize_role(role_id: str, alias_map: Mapping[str, str]) -> str:
    normalized = role_id.strip().lower()
    return alias_map.get(normalized, normalized)


def _normalize_roles(
    name: str,
    values: list[str] | None,
    alias_map: Mapping[str, str],
) -> set[str]:
    if not isinstance(values, list):
        raise ValueError(f"{name} must be a list of str")

    normalized_roles: set[str] = set()
    for index, value in enumerate(values):
        if not isinstance(value, str):
            raise ValueError(f"{name}[{index}] must be str")
        normalized_roles.add(_normalize_role(value, alias_map))
    return normalized_roles


def _is_valid_threshold(threshold: object) -> bool:
    if not isinstance(threshold, (int, float)) or isinstance(threshold, bool):
        return False
    threshold_value = float(threshold)
    if not math.isfinite(threshold_value):
        return False
    return 0.0 <= threshold_value <= 1.0


def _build_coverage_report(
    required: set[str],
    found: set[str],
    critical: set[str],
) -> CoverageReport:
    missing = required - found
    critical_missing = missing & critical

    coverage_score = 1.0
    if required:
        coverage_score = len(required & found) / len(required)

    return CoverageReport(
        required_roles=sorted(required),
        found_roles=sorted(found),
        missing_roles=sorted(missing),
        critical_missing_roles=sorted(critical_missing),
        coverage_score=coverage_score,
    )


def route_by_coverage(
    required_roles: list[str] | None = None,
    found_roles: list[str] | None = None,
    critical_roles: list[str] | None = None,
    threshold: float | None = None,
    role_aliases: Mapping[str, str] | None = None,
) -> tuple[RoutingDecision, CoverageReport | None]:
    """Route to Track C or Track B fallback from role-coverage and criticality."""
    invalid_decision = RoutingDecision.invalid_input()

    try:
        alias_map = _normalize_alias_map(role_aliases)
        required = _normalize_roles("required_roles", required_roles, alias_map)
        found = _normalize_roles("found_roles", found_roles, alias_map)
        critical = _normalize_roles("critical_roles", critical_roles, alias_map)
    except ValueError:
        return invalid_decision, None

    try:
        report = _build_coverage_report(required=required, found=found, critical=critical)
    except ValueError:
        return invalid_decision, None

    if not _is_valid_threshold(threshold):
        return invalid_decision, report

    threshold_value = float(threshold)
    if report.critical_missing_roles:
        decision = RoutingDecision(
            route=Route.TRACK_B_FALLBACK,
            reason_code=RoutingReasonCode.CRITICAL_ROLE_MISSING,
        )
    elif report.coverage_score >= threshold_value:
        decision = RoutingDecision(
            route=Route.TRACK_C,
            reason_code=RoutingReasonCode.COVERAGE_PASS,
        )
    else:
        decision = RoutingDecision(
            route=Route.TRACK_B_FALLBACK,
            reason_code=RoutingReasonCode.COVERAGE_BELOW_THRESHOLD,
        )
    return decision, report


__all__ = ["route_by_coverage"]
