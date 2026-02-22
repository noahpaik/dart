from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Mapping, Tuple

from dart_pipeline.contracts import (
    CanonicalIdentityKey,
    TimeSeriesView,
    ViewType,
    canonical_identity_key,
)

RowData = Mapping[str, Any]
FilingOrderKey = Tuple[datetime, str, int]


@dataclass(frozen=True)
class _NormalizedRecord:
    order_key: FilingOrderKey
    identity_key: CanonicalIdentityKey
    record: Dict[str, Any]


def _validate_reports_parameter(reports: Iterable[RowData] | None) -> Iterable[RowData]:
    if reports is None:
        raise ValueError("reports must be an iterable of mappings")
    try:
        iter(reports)
    except TypeError as exc:
        raise ValueError("reports must be an iterable of mappings") from exc
    return reports


def _extract_dimensions(raw_dimensions: Any, index: int) -> Dict[str, str]:
    if raw_dimensions is None:
        return {}
    if not isinstance(raw_dimensions, Mapping):
        raise ValueError(
            f"reports[{index}].dimensions must be a mapping of str to str"
        )

    dimensions: Dict[str, str] = {}
    for dim_key, dim_value in raw_dimensions.items():
        if not isinstance(dim_key, str) or not isinstance(dim_value, str):
            raise ValueError(
                f"reports[{index}].dimensions must be a mapping of str to str"
            )
        dimensions[dim_key] = dim_value
    return dimensions


def _extract_str_field(record: RowData, field: str, index: int) -> str:
    value = record.get(field)
    if not isinstance(value, str):
        raise ValueError(f"reports[{index}].{field} must be str")
    return value


def _extract_source_row_idx(record: RowData, index: int) -> int:
    value = record.get("source_row_idx")
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"reports[{index}].source_row_idx must be int")
    if value < 0:
        raise ValueError(f"reports[{index}].source_row_idx must be >= 0")
    return value


def _parse_filing_datetime_utc(value: Any, index: int) -> datetime:
    if not isinstance(value, str):
        raise ValueError(
            f"reports[{index}].filing_datetime_utc must be a UTC timestamp string"
        )

    raw = value.strip()
    if not raw:
        raise ValueError(
            f"reports[{index}].filing_datetime_utc must be a UTC timestamp string"
        )

    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(
            f"reports[{index}].filing_datetime_utc must be a valid ISO-8601 timestamp"
        ) from exc

    if parsed.tzinfo is None:
        raise ValueError(
            f"reports[{index}].filing_datetime_utc must include UTC timezone"
        )

    if parsed.utcoffset() != timedelta(0):
        raise ValueError(f"reports[{index}].filing_datetime_utc must be in UTC")

    return parsed.astimezone(timezone.utc)


def _normalize_record(record: RowData, index: int) -> _NormalizedRecord:
    if not isinstance(record, Mapping):
        raise ValueError(f"reports[{index}] must be a mapping")

    metric = _extract_str_field(record, "metric", index=index)
    period = _extract_str_field(record, "period", index=index)
    unit = _extract_str_field(record, "unit", index=index)
    filing_datetime_utc_raw = _extract_str_field(
        record, "filing_datetime_utc", index=index
    )
    filing_datetime_utc = _parse_filing_datetime_utc(
        filing_datetime_utc_raw, index=index
    )
    rcept_no = _extract_str_field(record, "rcept_no", index=index)
    source_row_idx = _extract_source_row_idx(record, index=index)

    dimensions = _extract_dimensions(record.get("dimensions", {}), index=index)
    identity_key = canonical_identity_key(
        metric=metric,
        period=period,
        unit=unit,
        dimensions=dimensions,
    )

    normalized_record = dict(record)
    normalized_record["metric"] = metric
    normalized_record["period"] = period
    normalized_record["unit"] = unit
    normalized_record["dimensions"] = dict(sorted(dimensions.items()))
    normalized_record["filing_datetime_utc"] = filing_datetime_utc_raw.strip()
    normalized_record["rcept_no"] = rcept_no
    normalized_record["source_row_idx"] = source_row_idx

    return _NormalizedRecord(
        order_key=(filing_datetime_utc, rcept_no, source_row_idx),
        identity_key=identity_key,
        record=normalized_record,
    )


def build_dual_views(
    reports: Iterable[RowData],
) -> tuple[TimeSeriesView, TimeSeriesView]:
    """Build deterministic AS_REPORTED and AS_LATEST restatement views."""
    validated_reports = _validate_reports_parameter(reports)
    normalized_records = [
        _normalize_record(report, index=index)
        for index, report in enumerate(validated_reports)
    ]
    as_reported_rows = sorted(normalized_records, key=lambda row: row.order_key)

    as_latest_by_identity: Dict[CanonicalIdentityKey, Dict[str, Any]] = {}
    for row in as_reported_rows:
        as_latest_by_identity[row.identity_key] = row.record

    as_reported_view = TimeSeriesView(
        view_type=ViewType.AS_REPORTED,
        records=[row.record for row in as_reported_rows],
    )
    as_latest_view = TimeSeriesView(
        view_type=ViewType.AS_LATEST,
        records=[as_latest_by_identity[key] for key in sorted(as_latest_by_identity)],
    )
    return as_reported_view, as_latest_view


__all__ = ["build_dual_views"]
