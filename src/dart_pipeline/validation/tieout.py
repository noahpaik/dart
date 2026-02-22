from __future__ import annotations

import math
from typing import Any, Dict, Iterable, Mapping, Tuple

from dart_pipeline.contracts import (
    CanonicalIdentityKey,
    TieOutItem,
    TieOutReasonCode,
    TieOutResult,
    TieOutStatus,
    TieOutSummary,
    canonical_identity_key,
)

RowData = Mapping[str, Any]


def _validate_numeric_parameter(name: str, value: float) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} must be a real number")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    if value < 0:
        raise ValueError(f"{name} must be >= 0")


def _validate_records_parameter(name: str, value: Iterable[RowData] | None) -> None:
    if value is None:
        raise ValueError(f"{name} must be an iterable of mappings")


def _extract_dimensions(raw_dimensions: Any, role: str, index: int) -> Dict[str, str]:
    if not isinstance(raw_dimensions, Mapping):
        raise ValueError(
            f"{role}[{index}].dimensions must be a mapping of str to str"
        )

    normalized_dimensions: Dict[str, str] = {}
    for dim_key, dim_value in raw_dimensions.items():
        if not isinstance(dim_key, str) or not isinstance(dim_value, str):
            raise ValueError(
                f"{role}[{index}].dimensions must be a mapping of str to str"
            )
        normalized_dimensions[dim_key] = dim_value
    return normalized_dimensions


def _extract_value(record: RowData, role: str, index: int) -> float:
    if role == "expected":
        raw_value = record.get("value", record.get("expected"))
    else:
        raw_value = record.get("value", record.get("observed"))

    if raw_value is None:
        raise ValueError(f"{role}[{index}] must include numeric value")
    if not isinstance(raw_value, (int, float)) or isinstance(raw_value, bool):
        raise ValueError(f"{role}[{index}] value must be numeric")
    return float(raw_value)


def _extract_identity(record: RowData, role: str, index: int) -> Tuple[CanonicalIdentityKey, float]:
    metric = record.get("metric")
    period = record.get("period")
    unit = record.get("unit")

    if not isinstance(metric, str):
        raise ValueError(f"{role}[{index}].metric must be str")
    if not isinstance(period, str):
        raise ValueError(f"{role}[{index}].period must be str")
    if not isinstance(unit, str):
        raise ValueError(f"{role}[{index}].unit must be str")

    dimensions = _extract_dimensions(record.get("dimensions", {}), role=role, index=index)
    value = _extract_value(record, role=role, index=index)

    key = canonical_identity_key(
        metric=metric,
        period=period,
        unit=unit,
        dimensions=dimensions,
    )
    return key, value


def _build_index(records: Iterable[RowData], role: str) -> Dict[CanonicalIdentityKey, float]:
    index_by_key: Dict[CanonicalIdentityKey, float] = {}
    for idx, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ValueError(f"{role}[{idx}] must be a mapping")

        key, value = _extract_identity(record, role=role, index=idx)
        if key in index_by_key:
            raise ValueError(f"duplicate identity key in {role}: {key}")
        index_by_key[key] = value
    return index_by_key


def run_tieout(
    expected: Iterable[RowData],
    observed: Iterable[RowData],
    *,
    abs_tol: float,
    rel_tol: float,
    warn_multiplier: float,
) -> TieOutResult:
    """Run deterministic tie-out validation between expected and observed values."""
    _validate_numeric_parameter("abs_tol", abs_tol)
    _validate_numeric_parameter("rel_tol", rel_tol)
    _validate_numeric_parameter("warn_multiplier", warn_multiplier)
    _validate_records_parameter("expected", expected)
    _validate_records_parameter("observed", observed)
    if warn_multiplier < 1:
        raise ValueError("warn_multiplier must be >= 1")

    expected_index = _build_index(expected, role="expected")
    observed_index = _build_index(observed, role="observed")

    items = []
    all_keys = sorted(set(expected_index) | set(observed_index))

    for key in all_keys:
        metric, period, unit, dim_items = key
        dimensions = dict(dim_items)

        if key in expected_index and key in observed_index:
            expected_value = expected_index[key]
            observed_value = observed_index[key]
            tolerance = max(abs_tol, rel_tol * max(abs(expected_value), 1e-12))
            diff = observed_value - expected_value
            abs_diff = abs(diff)

            if abs_diff <= tolerance:
                status = TieOutStatus.PASS
                reason_code = TieOutReasonCode.OK
            elif abs_diff <= warn_multiplier * tolerance:
                status = TieOutStatus.WARN
                reason_code = TieOutReasonCode.OUT_OF_TOLERANCE_WARN
            else:
                status = TieOutStatus.FAIL
                reason_code = TieOutReasonCode.OUT_OF_TOLERANCE_FAIL

            items.append(
                TieOutItem(
                    metric=metric,
                    period=period,
                    unit=unit,
                    dimensions=dimensions,
                    expected=expected_value,
                    observed=observed_value,
                    diff=diff,
                    abs_diff=abs_diff,
                    tolerance=tolerance,
                    status=status,
                    reason_code=reason_code,
                )
            )
            continue

        if key in expected_index:
            items.append(
                TieOutItem(
                    metric=metric,
                    period=period,
                    unit=unit,
                    dimensions=dimensions,
                    expected=expected_index[key],
                    observed=None,
                    diff=None,
                    abs_diff=None,
                    tolerance=None,
                    status=TieOutStatus.FAIL,
                    reason_code=TieOutReasonCode.MISSING_OBSERVED,
                )
            )
            continue

        items.append(
            TieOutItem(
                metric=metric,
                period=period,
                unit=unit,
                dimensions=dimensions,
                expected=None,
                observed=observed_index[key],
                diff=None,
                abs_diff=None,
                tolerance=None,
                status=TieOutStatus.WARN,
                reason_code=TieOutReasonCode.EXTRA_OBSERVED,
            )
        )

    pass_count = sum(1 for item in items if item.status == TieOutStatus.PASS)
    warn_count = sum(1 for item in items if item.status == TieOutStatus.WARN)
    fail_count = sum(1 for item in items if item.status == TieOutStatus.FAIL)

    return TieOutResult(
        summary=TieOutSummary(
            pass_count=pass_count,
            warn_count=warn_count,
            fail_count=fail_count,
        ),
        items=items,
    )


__all__ = ["run_tieout"]
