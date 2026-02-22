from __future__ import annotations

import dart_pipeline as dp
from dart_pipeline.contracts import (
    Step6ExecutionResult,
    Step6TrackCIntegrationResult,
    TrackBHandoffExecutionResult,
    TrackBHandoffExecutionStatus,
    TrackBHandoffExecutorOutcome,
    TrackBHandoffRequest,
)
from dart_pipeline.pipeline_step6 import (
    ANNUAL_REPRT_CODE,
    build_step6_track_c_integration,
    build_track_a_snapshot,
    build_track_b_handoff_request,
    execute_step6_with_track_b_handoff,
)
from dart_pipeline.track_c import extract_segment_members, extract_sga_accounts


def test_package_reexports_step6_contracts_and_functions() -> None:
    assert dp.Step6ExecutionResult is Step6ExecutionResult
    assert dp.Step6TrackCIntegrationResult is Step6TrackCIntegrationResult
    assert dp.TrackBHandoffExecutionResult is TrackBHandoffExecutionResult
    assert dp.TrackBHandoffExecutionStatus is TrackBHandoffExecutionStatus
    assert dp.TrackBHandoffExecutorOutcome is TrackBHandoffExecutorOutcome
    assert dp.TrackBHandoffRequest is TrackBHandoffRequest

    assert dp.ANNUAL_REPRT_CODE == ANNUAL_REPRT_CODE
    assert dp.build_track_a_snapshot is build_track_a_snapshot
    assert dp.build_step6_track_c_integration is build_step6_track_c_integration
    assert dp.build_track_b_handoff_request is build_track_b_handoff_request
    assert dp.execute_step6_with_track_b_handoff is execute_step6_with_track_b_handoff
    assert dp.extract_sga_accounts is extract_sga_accounts
    assert dp.extract_segment_members is extract_segment_members


def test_package_all_includes_step6_exports() -> None:
    expected = {
        "ANNUAL_REPRT_CODE",
        "Step6ExecutionResult",
        "Step6TrackCIntegrationResult",
        "TrackBHandoffExecutionResult",
        "TrackBHandoffExecutionStatus",
        "TrackBHandoffExecutorOutcome",
        "TrackBHandoffRequest",
        "build_track_a_snapshot",
        "build_step6_track_c_integration",
        "build_track_b_handoff_request",
        "execute_step6_with_track_b_handoff",
        "extract_sga_accounts",
        "extract_segment_members",
    }
    assert expected.issubset(set(dp.__all__))
