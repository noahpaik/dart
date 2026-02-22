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
PositiveStrictInt = Annotated[StrictInt, Field(ge=1)]
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


class XbrlSource(str, Enum):
    DART = "dart"
    COMPANY = "company"
    IFRS = "ifrs"


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


class XbrlAccountRef(StrictContractModel):
    account_id: StrictStr
    label_ko: StrictStr
    label_en: StrictStr
    source: XbrlSource

    @model_validator(mode="after")
    def validate_account_id(self) -> "XbrlAccountRef":
        if not self.account_id.strip():
            raise ValueError("account_id must be non-empty")
        return self


class XbrlMemberRef(StrictContractModel):
    account_id: StrictStr
    label_ko: StrictStr
    source: XbrlSource

    @model_validator(mode="after")
    def validate_account_id(self) -> "XbrlMemberRef":
        if not self.account_id.strip():
            raise ValueError("account_id must be non-empty")
        return self


class XbrlNote(StrictContractModel):
    role_code: StrictStr
    role_name: StrictStr
    accounts: list[XbrlAccountRef]
    members: list[XbrlMemberRef]

    @model_validator(mode="after")
    def validate_and_sort_references(self) -> "XbrlNote":
        if not self.role_code.strip():
            raise ValueError("role_code must be non-empty")
        if not self.role_name.strip():
            raise ValueError("role_name must be non-empty")

        seen_account_ids: set[str] = set()
        for index, ref in enumerate(self.accounts):
            if ref.account_id in seen_account_ids:
                raise ValueError(
                    f"accounts[{index}].account_id is duplicated within the same note"
                )
            seen_account_ids.add(ref.account_id)

        seen_member_ids: set[str] = set()
        for index, ref in enumerate(self.members):
            if ref.account_id in seen_member_ids:
                raise ValueError(
                    f"members[{index}].account_id is duplicated within the same note"
                )
            seen_member_ids.add(ref.account_id)

        self.accounts = sorted(
            self.accounts,
            key=lambda ref: (ref.account_id, ref.source.value, ref.label_ko, ref.label_en),
        )
        self.members = sorted(
            self.members,
            key=lambda ref: (ref.account_id, ref.source.value, ref.label_ko),
        )
        return self


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


class Step6TrackCIntegrationResult(StrictContractModel):
    track_a_snapshot: TrackASnapshot
    track_c_notes: list[XbrlNote]
    routing_decision: RoutingDecision
    coverage_report: Optional[CoverageReport] = None
    fallback_required: bool

    @model_validator(mode="after")
    def validate_determinism_and_fallback_consistency(self) -> "Step6TrackCIntegrationResult":
        self.track_c_notes = sorted(
            self.track_c_notes,
            key=lambda note: (note.role_code, note.role_name),
        )

        expected_fallback_required = self.routing_decision.route == Route.TRACK_B_FALLBACK
        if self.fallback_required != expected_fallback_required:
            raise ValueError(
                "fallback_required must match routing_decision.route == TRACK_B_FALLBACK"
            )
        return self


class TrackBHandoffRequest(StrictContractModel):
    corp_code: StrictStr
    bsns_year: StrictStr
    reprt_code: StrictStr
    rcept_no: StrictStr
    rcept_dt: StrictStr
    fs_div: StrictStr
    idempotency_key: StrictStr
    reason_code: RoutingReasonCode
    missing_roles: list[StrictStr]
    critical_missing_roles: list[StrictStr]
    coverage_score: UnitIntervalStrictFloat

    @model_validator(mode="after")
    def validate_and_sort_fields(self) -> "TrackBHandoffRequest":
        if not self.corp_code.strip():
            raise ValueError("corp_code must be a non-empty string")
        if len(self.bsns_year) != 4 or not self.bsns_year.isdigit():
            raise ValueError("bsns_year must be a 4-digit string")
        if not self.reprt_code.strip():
            raise ValueError("reprt_code must be a non-empty string")
        if not self.rcept_no.strip():
            raise ValueError("rcept_no must be a non-empty string")
        if len(self.rcept_dt) != 8 or not self.rcept_dt.isdigit():
            raise ValueError("rcept_dt must be an 8-digit YYYYMMDD string")
        if not self.fs_div.strip():
            raise ValueError("fs_div must be a non-empty string")
        if not self.idempotency_key.strip():
            raise ValueError("idempotency_key must be a non-empty string")
        if _ROUTE_BY_REASON[self.reason_code] != Route.TRACK_B_FALLBACK:
            raise ValueError("reason_code must map to TRACK_B_FALLBACK")

        missing_roles: set[str] = set()
        for index, role_id in enumerate(self.missing_roles):
            if not role_id.strip():
                raise ValueError(f"missing_roles[{index}] must be a non-empty string")
            missing_roles.add(role_id)

        critical_missing_roles: set[str] = set()
        for index, role_id in enumerate(self.critical_missing_roles):
            if not role_id.strip():
                raise ValueError(
                    f"critical_missing_roles[{index}] must be a non-empty string"
                )
            critical_missing_roles.add(role_id)

        if not critical_missing_roles.issubset(missing_roles):
            raise ValueError("critical_missing_roles must be a subset of missing_roles")

        self.missing_roles = sorted(missing_roles)
        self.critical_missing_roles = sorted(critical_missing_roles)
        return self


class TrackBHandoffExecutionStatus(str, Enum):
    SUCCESS = "SUCCESS"
    RETRYABLE_ERROR = "RETRYABLE_ERROR"
    PERMANENT_ERROR = "PERMANENT_ERROR"


class TrackBHandoffExecutorOutcome(StrictContractModel):
    status: TrackBHandoffExecutionStatus
    error_code: Optional[StrictStr] = None
    error_message: Optional[StrictStr] = None
    retry_after_seconds: Optional[NonNegativeStrictFloat] = None

    @model_validator(mode="after")
    def validate_status_payload(self) -> "TrackBHandoffExecutorOutcome":
        if self.error_code is not None and not self.error_code.strip():
            raise ValueError("error_code must be non-empty when provided")
        if self.error_message is not None and not self.error_message.strip():
            raise ValueError("error_message must be non-empty when provided")

        if self.status == TrackBHandoffExecutionStatus.SUCCESS:
            if (
                self.error_code is not None
                or self.error_message is not None
                or self.retry_after_seconds is not None
            ):
                raise ValueError(
                    "SUCCESS requires error_code, error_message, and retry_after_seconds to be None"
                )
            return self

        if self.error_code is None:
            raise ValueError("error_code is required for non-success outcomes")

        if self.status == TrackBHandoffExecutionStatus.PERMANENT_ERROR:
            if self.retry_after_seconds is not None:
                raise ValueError(
                    "PERMANENT_ERROR requires retry_after_seconds to be None"
                )
            return self

        return self


class TrackBHandoffExecutionResult(StrictContractModel):
    idempotency_key: StrictStr
    attempts: PositiveStrictInt
    max_attempts: PositiveStrictInt
    outcome: TrackBHandoffExecutorOutcome

    @model_validator(mode="after")
    def validate_consistency(self) -> "TrackBHandoffExecutionResult":
        if not self.idempotency_key.strip():
            raise ValueError("idempotency_key must be a non-empty string")
        if self.attempts > self.max_attempts:
            raise ValueError("attempts must be less than or equal to max_attempts")
        return self


class Step6ExecutionResult(StrictContractModel):
    integration_result: Step6TrackCIntegrationResult
    track_b_handoff_request: Optional[TrackBHandoffRequest] = None
    track_b_handoff_triggered: bool = False
    track_b_handoff_execution_result: Optional[TrackBHandoffExecutionResult] = None

    @model_validator(mode="after")
    def validate_handoff_consistency(self) -> "Step6ExecutionResult":
        request = self.track_b_handoff_request
        execution_result = self.track_b_handoff_execution_result
        fallback_required = self.integration_result.fallback_required

        if fallback_required and request is None:
            raise ValueError(
                "track_b_handoff_request is required when fallback_required is true"
            )
        if not fallback_required and request is not None:
            raise ValueError(
                "track_b_handoff_request must be omitted when fallback_required is false"
            )
        if self.track_b_handoff_triggered and request is None:
            raise ValueError(
                "track_b_handoff_triggered requires track_b_handoff_request to be present"
            )
        if not fallback_required and self.track_b_handoff_triggered:
            raise ValueError("track_b_handoff_triggered cannot be true on TRACK_C route")
        if not self.track_b_handoff_triggered and execution_result is not None:
            raise ValueError(
                "track_b_handoff_execution_result must be omitted when track_b_handoff_triggered is false"
            )
        if self.track_b_handoff_triggered and execution_result is None:
            raise ValueError(
                "track_b_handoff_execution_result is required when track_b_handoff_triggered is true"
            )

        if request is None:
            return self

        snapshot = self.integration_result.track_a_snapshot
        if request.corp_code != snapshot.corp_code:
            raise ValueError("track_b_handoff_request.corp_code must match snapshot")
        if request.bsns_year != snapshot.bsns_year:
            raise ValueError("track_b_handoff_request.bsns_year must match snapshot")
        if request.reprt_code != snapshot.reprt_code:
            raise ValueError("track_b_handoff_request.reprt_code must match snapshot")
        if request.rcept_no != snapshot.rcept_no:
            raise ValueError("track_b_handoff_request.rcept_no must match snapshot")
        if request.rcept_dt != snapshot.rcept_dt:
            raise ValueError("track_b_handoff_request.rcept_dt must match snapshot")
        if request.fs_div != snapshot.fs_div:
            raise ValueError("track_b_handoff_request.fs_div must match snapshot")

        if request.reason_code != self.integration_result.routing_decision.reason_code:
            raise ValueError(
                "track_b_handoff_request.reason_code must match integration_result.routing_decision.reason_code"
            )

        report = self.integration_result.coverage_report
        expected_missing_roles = sorted(report.missing_roles) if report is not None else []
        expected_critical_missing_roles = (
            sorted(report.critical_missing_roles) if report is not None else []
        )
        expected_coverage_score = report.coverage_score if report is not None else 0.0

        if request.missing_roles != expected_missing_roles:
            raise ValueError(
                "track_b_handoff_request.missing_roles must match integration_result.coverage_report"
            )
        if request.critical_missing_roles != expected_critical_missing_roles:
            raise ValueError(
                "track_b_handoff_request.critical_missing_roles must match integration_result.coverage_report"
            )
        if request.coverage_score != expected_coverage_score:
            raise ValueError(
                "track_b_handoff_request.coverage_score must match integration_result.coverage_report"
            )
        if (
            execution_result is not None
            and execution_result.idempotency_key != request.idempotency_key
        ):
            raise ValueError(
                "track_b_handoff_execution_result.idempotency_key must match track_b_handoff_request.idempotency_key"
            )
        return self


__all__ = [
    "CanonicalIdentityKey",
    "CoverageReport",
    "TrackBHandoffExecutionResult",
    "TrackBHandoffExecutionStatus",
    "TrackBHandoffExecutorOutcome",
    "Route",
    "RoutingDecision",
    "RoutingReasonCode",
    "Step6ExecutionResult",
    "TrackBHandoffRequest",
    "TieOutItem",
    "TieOutReasonCode",
    "TieOutResult",
    "TieOutStatus",
    "TieOutSummary",
    "Step6TrackCIntegrationResult",
    "TrackAAmount",
    "TrackARow",
    "TrackASnapshot",
    "TimeSeriesView",
    "ViewType",
    "XbrlAccountRef",
    "XbrlMemberRef",
    "XbrlNote",
    "XbrlSource",
    "canonical_identity_key",
]
