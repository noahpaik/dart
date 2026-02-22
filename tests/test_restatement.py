from typing import Dict, Optional

import pytest

from dart_pipeline.contracts import ViewType
from dart_pipeline.timeseries import build_dual_views


def _row(
    *,
    metric: str,
    value: float,
    filing_datetime_utc: str,
    rcept_no: str,
    source_row_idx: int,
    period: str = "2024Q4",
    unit: str = "KRW",
    dimensions: Optional[Dict[str, str]] = None,
):
    return {
        "metric": metric,
        "period": period,
        "unit": unit,
        "dimensions": dimensions or {},
        "value": value,
        "filing_datetime_utc": filing_datetime_utc,
        "rcept_no": rcept_no,
        "source_row_idx": source_row_idx,
    }


def test_restatement_out_of_order_filings_and_latest_overwrite() -> None:
    reports = [
        _row(
            metric="revenue",
            value=110.0,
            filing_datetime_utc="2024-03-05T00:00:00Z",
            rcept_no="20240305000001",
            source_row_idx=0,
        ),
        _row(
            metric="assets",
            value=200.0,
            filing_datetime_utc="2024-03-01T00:00:00Z",
            rcept_no="20240301000002",
            source_row_idx=1,
        ),
        _row(
            metric="revenue",
            value=100.0,
            filing_datetime_utc="2024-03-01T00:00:00Z",
            rcept_no="20240301000001",
            source_row_idx=0,
        ),
    ]

    as_reported, as_latest = build_dual_views(reports)

    assert as_reported.view_type == ViewType.AS_REPORTED
    assert as_latest.view_type == ViewType.AS_LATEST

    assert [row["value"] for row in as_reported.records] == [100.0, 200.0, 110.0]

    assert len(as_latest.records) == 2
    assert [row["metric"] for row in as_latest.records] == ["assets", "revenue"]
    assert [row["value"] for row in as_latest.records] == [200.0, 110.0]


def test_restatement_same_timestamp_tie_break_rcept_no_then_source_row_idx() -> None:
    reports = [
        _row(
            metric="cash",
            value=10.0,
            filing_datetime_utc="2024-03-01T00:00:00Z",
            rcept_no="20240301000002",
            source_row_idx=0,
        ),
        _row(
            metric="cash",
            value=9.0,
            filing_datetime_utc="2024-03-01T00:00:00Z",
            rcept_no="20240301000001",
            source_row_idx=1,
        ),
        _row(
            metric="cash",
            value=11.0,
            filing_datetime_utc="2024-03-01T00:00:00Z",
            rcept_no="20240301000002",
            source_row_idx=1,
        ),
    ]

    as_reported, as_latest = build_dual_views(reports)

    assert [row["value"] for row in as_reported.records] == [9.0, 10.0, 11.0]
    assert len(as_latest.records) == 1
    assert as_latest.records[0]["value"] == 11.0


def test_restatement_as_latest_output_order_is_deterministic() -> None:
    reports = [
        _row(
            metric="b_metric",
            value=2.0,
            filing_datetime_utc="2024-03-01T00:00:00Z",
            rcept_no="20240301000001",
            source_row_idx=0,
            dimensions={"z": "2", "a": "1"},
        ),
        _row(
            metric="a_metric",
            value=1.0,
            filing_datetime_utc="2024-04-01T00:00:00Z",
            rcept_no="20240401000001",
            source_row_idx=0,
            dimensions={"segment": "A"},
        ),
    ]

    _, as_latest = build_dual_views(reports)
    assert [row["metric"] for row in as_latest.records] == ["a_metric", "b_metric"]


@pytest.mark.parametrize(
    "record_override",
    [
        {"filing_datetime_utc": "not-a-timestamp"},
        {"filing_datetime_utc": "2024-03-01T00:00:00"},
        {"filing_datetime_utc": "2024-03-01T00:00:00+09:00"},
        {"filing_datetime_utc": None},
        {"remove_filing_datetime_utc": True},
    ],
)
def test_restatement_strict_utc_parse_invalid_or_missing_timestamp_fails(
    record_override: Dict[str, object],
) -> None:
    report = _row(
        metric="revenue",
        value=100.0,
        filing_datetime_utc="2024-03-01T00:00:00Z",
        rcept_no="20240301000001",
        source_row_idx=0,
    )
    remove_filing = bool(record_override.pop("remove_filing_datetime_utc", False))
    if remove_filing:
        report.pop("filing_datetime_utc")
    else:
        report.update(record_override)

    with pytest.raises(ValueError, match="filing_datetime_utc"):
        build_dual_views([report])


def test_restatement_dimensions_order_invariance_for_overwrite_key() -> None:
    reports = [
        _row(
            metric="revenue",
            value=100.0,
            filing_datetime_utc="2024-03-01T00:00:00Z",
            rcept_no="20240301000001",
            source_row_idx=0,
            dimensions={"segment": "A", "region": "KR"},
        ),
        _row(
            metric="revenue",
            value=120.0,
            filing_datetime_utc="2024-04-01T00:00:00Z",
            rcept_no="20240401000001",
            source_row_idx=0,
            dimensions={"region": "KR", "segment": "A"},
        ),
    ]

    _, as_latest = build_dual_views(reports)
    assert len(as_latest.records) == 1
    assert as_latest.records[0]["value"] == 120.0
    assert as_latest.records[0]["dimensions"] == {"region": "KR", "segment": "A"}
