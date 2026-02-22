from __future__ import annotations

from collections import Counter
from decimal import Decimal
from enum import Enum
from typing import Any, Annotated, Dict, Mapping, Optional, Set, Tuple, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    StrictStr,
    model_validator,
)

NonNegativeStrictInt = Annotated[StrictInt, Field(ge=0)]
NonNegativeStrictFloat = Annotated[StrictFloat, Field(ge=0)]
UnitIntervalStrictFloat = Annotated[StrictFloat, Field(ge=0, le=1)]
TrackAAmount = Union[StrictInt, Decimal]


class TieOutStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class TieOutReasonCode(str, Enum):
    OK = "OK"
    MISSING_OBSERVED = "MISSING_OBSERVED"
    EXTRA_OBSERVED = "EXTRA_OBSERVED"
    OUT_OF_TOLERANCE_WARN = "OUT_OF_TOLERANCE_WARN"
    OUT_OF_TOLERANCE_FAIL = "OUT_OF_TOLERANCE_FAIL"


class Route(str, Enum):
    TRACK_C = "TRACK_C"
    TRACK_B_FALLBACK = "TRACK_B_FALLBACK"


class RoutingReasonCode(str, Enum):
    CRITICAL_ROLE_MISSING = "CRITICAL_ROLE_MISSING"
    COVERAGE_PASS = "COVERAGE_PASS"
    COVERAGE_BELOW_THRESHOLD = "COVERAGE_BELOW_THRESHOLD"
    INVALID_INPUT = "INVALID_INPUT"


class ViewType(str, Enum):
    AS_REPORTED = "AS_REPORTED"
    AS_LATEST = "AS_LATEST"


class StrictContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


CanonicalIdentityKey = Tuple[str, str, str, Tuple[Tuple[str, str], ...]]


def canonical_identity_key(
    metric: str,
    period: str,
    unit: str,
    dimensions: Mapping[str, str],
) -> CanonicalIdentityKey:
    """Canonicalized identity used across tie-out and restatement logic."""
    return (
        metric,
        period,
        unit,
        tuple(sorted(dimensions.items())),
    )


_ALLOWED_TIEOUT_REASON_BY_STATUS: Dict[TieOutStatus, Set[TieOutReasonCode]] = {
    TieOutStatus.PASS: {TieOutReasonCode.OK},
    TieOutStatus.WARN: {
        TieOutReasonCode.EXTRA_OBSERVED,
        TieOutReasonCode.OUT_OF_TOLERANCE_WARN,
    },
    TieOutStatus.FAIL: {
        TieOutReasonCode.MISSING_OBSERVED,
        TieOutReasonCode.OUT_OF_TOLERANCE_FAIL,
    },
}


class TieOutItem(StrictContractModel):
    metric: StrictStr
    period: StrictStr
    unit: StrictStr
    dimensions: Dict[StrictStr, StrictStr]
    expected: Optional[StrictFloat] = None
    observed: Optional[StrictFloat] = None
    diff: Optional[StrictFloat] = None
    abs_diff: Optional[NonNegativeStrictFloat] = None
    tolerance: Optional[NonNegativeStrictFloat] = None
    status: TieOutStatus
    reason_code: TieOutReasonCode

    @property
    def identity_key(self) -> CanonicalIdentityKey:
        return canonical_identity_key(
            metric=self.metric,
            period=self.period,
            unit=self.unit,
            dimensions=self.dimensions,
        )

    @model_validator(mode="after")
    def validate_status_reason_pair(self) -> "TieOutItem":
        allowed_reasons = _ALLOWED_TIEOUT_REASON_BY_STATUS[self.status]
        if self.reason_code not in allowed_reasons:
            raise ValueError(
                f"reason_code {self.reason_code.value} is invalid for status {self.status.value}"
            )
        return self


class TieOutSummary(StrictContractModel):
    pass_count: NonNegativeStrictInt
    warn_count: NonNegativeStrictInt
    fail_count: NonNegativeStrictInt


class TieOutResult(StrictContractModel):
    summary: TieOutSummary
    items: list[TieOutItem]

    @model_validator(mode="after")
    def validate_summary_counts(self) -> "TieOutResult":
        counts = Counter(item.status for item in self.items)
        expected_pass = counts.get(TieOutStatus.PASS, 0)
        expected_warn = counts.get(TieOutStatus.WARN, 0)
        expected_fail = counts.get(TieOutStatus.FAIL, 0)

        if self.summary.pass_count != expected_pass:
            raise ValueError("summary.pass_count does not match items")
        if self.summary.warn_count != expected_warn:
            raise ValueError("summary.warn_count does not match items")
        if self.summary.fail_count != expected_fail:
            raise ValueError("summary.fail_count does not match items")
        return self


class CoverageReport(StrictContractModel):
    required_roles: list[StrictStr]
    found_roles: list[StrictStr]
    missing_roles: list[StrictStr]
    critical_missing_roles: list[StrictStr]
    coverage_score: UnitIntervalStrictFloat

    @model_validator(mode="after")
    def validate_role_relationships(self) -> "CoverageReport":
        required = set(self.required_roles)
        missing = set(self.missing_roles)
        critical = set(self.critical_missing_roles)

        if not missing.issubset(required):
            raise ValueError("missing_roles must be a subset of required_roles")
        if not critical.issubset(missing):
            raise ValueError("critical_missing_roles must be a subset of missing_roles")
        return self


_ROUTE_BY_REASON: Dict[RoutingReasonCode, Route] = {
    RoutingReasonCode.CRITICAL_ROLE_MISSING: Route.TRACK_B_FALLBACK,
    RoutingReasonCode.COVERAGE_PASS: Route.TRACK_C,
    RoutingReasonCode.COVERAGE_BELOW_THRESHOLD: Route.TRACK_B_FALLBACK,
    RoutingReasonCode.INVALID_INPUT: Route.TRACK_B_FALLBACK,
}


class RoutingDecision(StrictContractModel):
    route: Route
    reason_code: RoutingReasonCode

    @model_validator(mode="after")
    def validate_reason_route_mapping(self) -> "RoutingDecision":
        expected_route = _ROUTE_BY_REASON[self.reason_code]
        if self.route != expected_route:
            raise ValueError(
                f"route {self.route.value} is invalid for reason_code {self.reason_code.value}"
            )
        return self

    @classmethod
    def invalid_input(cls) -> "RoutingDecision":
        return cls(
            route=Route.TRACK_B_FALLBACK,
            reason_code=RoutingReasonCode.INVALID_INPUT,
        )


class TimeSeriesView(StrictContractModel):
    view_type: ViewType
    records: list[Dict[StrictStr, Any]]


class TrackARow(StrictContractModel):
    corp_code: StrictStr
    rcept_no: StrictStr
    rcept_dt: StrictStr
    bsns_year: StrictStr
    reprt_code: StrictStr
    fs_div: StrictStr
    sj_div: StrictStr
    account_id: StrictStr
    account_nm: StrictStr
    ord: NonNegativeStrictInt
    source_row_idx: NonNegativeStrictInt
    thstrm_amount_raw: Optional[StrictStr] = None
    frmtrm_amount_raw: Optional[StrictStr] = None
    bfefrmtrm_amount_raw: Optional[StrictStr] = None
    thstrm_amount: Optional[TrackAAmount] = None
    frmtrm_amount: Optional[TrackAAmount] = None
    bfefrmtrm_amount: Optional[TrackAAmount] = None

    @model_validator(mode="after")
    def validate_rcept_dt_format(self) -> "TrackARow":
        if len(self.rcept_dt) != 8 or not self.rcept_dt.isdigit():
            raise ValueError("rcept_dt must be an 8-digit YYYYMMDD string")
        return self


class TrackASnapshot(StrictContractModel):
    corp_code: StrictStr
    rcept_no: StrictStr
    rcept_dt: StrictStr
    bsns_year: StrictStr
    reprt_code: StrictStr
    fs_div: StrictStr
    rows: list[TrackARow]

    @model_validator(mode="after")
    def validate_and_sort_rows(self) -> "TrackASnapshot":
        if len(self.rcept_dt) != 8 or not self.rcept_dt.isdigit():
            raise ValueError("rcept_dt must be an 8-digit YYYYMMDD string")

        for index, row in enumerate(self.rows):
            if row.corp_code != self.corp_code:
                raise ValueError(f"rows[{index}].corp_code does not match snapshot")
            if row.rcept_no != self.rcept_no:
                raise ValueError(f"rows[{index}].rcept_no does not match snapshot")
            if row.rcept_dt != self.rcept_dt:
                raise ValueError(f"rows[{index}].rcept_dt does not match snapshot")
            if row.bsns_year != self.bsns_year:
                raise ValueError(f"rows[{index}].bsns_year does not match snapshot")
            if row.reprt_code != self.reprt_code:
                raise ValueError(f"rows[{index}].reprt_code does not match snapshot")
            if row.fs_div != self.fs_div:
                raise ValueError(f"rows[{index}].fs_div does not match snapshot")

        self.rows = sorted(
            self.rows,
            key=lambda row: (
                row.sj_div,
                row.ord,
                row.account_id,
                row.account_nm,
                row.source_row_idx,
            ),
        )
        return self


__all__ = [
    "CanonicalIdentityKey",
    "CoverageReport",
    "Route",
    "RoutingDecision",
    "RoutingReasonCode",
    "TieOutItem",
    "TieOutReasonCode",
    "TieOutResult",
    "TieOutStatus",
    "TieOutSummary",
    "TrackAAmount",
    "TrackARow",
    "TrackASnapshot",
    "TimeSeriesView",
    "ViewType",
    "canonical_identity_key",
]
