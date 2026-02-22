"""DART pipeline run-1 package scaffold."""

from .contracts import (
    CanonicalIdentityKey,
    CoverageReport,
    Route,
    RoutingDecision,
    RoutingReasonCode,
    TieOutItem,
    TieOutReasonCode,
    TieOutResult,
    TieOutStatus,
    TieOutSummary,
    TrackAAmount,
    TrackARow,
    TrackASnapshot,
    TimeSeriesView,
    ViewType,
    canonical_identity_key,
)
from .routing import route_by_coverage
from .timeseries import build_dual_views
from .validation import run_tieout

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
    "build_dual_views",
    "canonical_identity_key",
    "route_by_coverage",
    "run_tieout",
]

__version__ = "0.1.0"
