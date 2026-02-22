# Step6 API Usage (Draft)

This document describes minimal, deterministic usage for Step6 Track-A ingestion.

## Environment policy

- `DART_API_KEY` is read from environment only.
- Never hardcode or commit API keys.
- Exception messages redact API key values.
- Tests must not call the network; mock HTTP instead.

Example:

```bash
export DART_API_KEY="your-opendart-key"
```

## Minimal usage

```python
from pathlib import Path

from dart_pipeline.corp_code_db import CorpCodeDB
from dart_pipeline.dart_api import DartApiClient
from dart_pipeline.pipeline_step6 import build_track_a_snapshot

client = DartApiClient(timeout_seconds=10.0, max_response_bytes=10_000_000)

# Explicit corp DB refresh (no implicit writes during reads)
with CorpCodeDB(Path("data/corp_code.sqlite3")) as corp_db:
    corp_db.refresh_from_api(client)
    corp = corp_db.find_best_name_match("삼성전자")

if corp is None:
    raise RuntimeError("corp code not found")

snapshot = build_track_a_snapshot(
    dart_api_client=client,
    corp_code=corp.corp_code,
    bsns_year="2024",
    allow_ofs_fallback=False,
)

print(snapshot.model_dump(mode="json"))
```

## Step6 + Track C routing integration

```python
from pathlib import Path

from dart_pipeline.pipeline_step6 import build_step6_track_c_integration

result = build_step6_track_c_integration(
    dart_api_client=client,
    corp_code=corp.corp_code,
    bsns_year="2024",
    xbrl_dir=Path("tests/fixtures/track_c/basic_bundle"),
    required_roles=["D822105", "D831150", "D838000"],
    critical_roles=["D851100"],
    threshold=0.67,
)

if result.fallback_required:
    # Keep Track B handling explicit/minimal in a separate path.
    print("Track B fallback required")
else:
    print("Track C selected")

print(result.routing_decision.model_dump(mode="json"))
print(result.coverage_report.model_dump(mode="json") if result.coverage_report else None)
```

## Track C helper extraction (LLM-free)

```python
from pathlib import Path

from dart_pipeline.track_c import (
    extract_segment_members,
    extract_sga_accounts,
    parse_xbrl_notes,
)

notes = parse_xbrl_notes(Path("tests/fixtures/track_c/basic_bundle"))

# dart_ 기반 판관비 상세 계정 추출
print(extract_sga_accounts(notes))

# 회사 고유 member 추출 (영업부문 role이 있으면 우선 사용, 없으면 전체 notes fallback)
print([member.model_dump(mode="json") for member in extract_segment_members(notes)])
```

## Step6 explicit Track B handoff

```python
from pathlib import Path

from dart_pipeline.contracts import (
    TrackBHandoffExecutionStatus,
    TrackBHandoffExecutorOutcome,
    TrackBHandoffRequest,
)
from dart_pipeline.pipeline_step6 import execute_step6_with_track_b_handoff


def run_track_b_fallback(request: TrackBHandoffRequest) -> TrackBHandoffExecutorOutcome | None:
    # request.idempotency_key is deterministic for the same semantic payload.
    print(f"handoff: {request.idempotency_key}")
    return TrackBHandoffExecutorOutcome(
        status=TrackBHandoffExecutionStatus.RETRYABLE_ERROR,
        error_code="DOWNSTREAM_TEMP_UNAVAILABLE",
        retry_after_seconds=2.0,
    )


execution = execute_step6_with_track_b_handoff(
    dart_api_client=client,
    corp_code=corp.corp_code,
    bsns_year="2024",
    xbrl_dir=Path("tests/fixtures/track_c/basic_bundle"),
    required_roles=["D822105", "D831150", "D838000"],
    critical_roles=["D851100"],
    threshold=0.67,
    track_b_handoff_executor=run_track_b_fallback,  # Optional
    max_handoff_attempts=3,  # Optional, default=1
)

print(execution.integration_result.routing_decision.model_dump(mode="json"))
print(execution.track_b_handoff_triggered)
print(
    execution.track_b_handoff_request.model_dump(mode="json")
    if execution.track_b_handoff_request is not None
    else None
)
print(
    execution.track_b_handoff_execution_result.model_dump(mode="json")
    if execution.track_b_handoff_execution_result is not None
    else None
)
```

If `track_b_handoff_executor` is omitted, fallback still returns a deterministic
`track_b_handoff_request` for downstream explicit handoff, and
`track_b_handoff_triggered` remains `false`.

Executor contract:
- Return `None` or `TrackBHandoffExecutorOutcome(status=SUCCESS)` to mark success.
- Return `RETRYABLE_ERROR` with `error_code` to allow retry (until `max_handoff_attempts`).
- Return `PERMANENT_ERROR` with `error_code` to stop immediately.
- No sleep/backoff is applied in Step6; only attempt counting and stop conditions are enforced.

Execution result contract:
- When handoff is triggered (`track_b_handoff_triggered=true`), `track_b_handoff_execution_result` is always present.
- `track_b_handoff_execution_result.idempotency_key` always equals `track_b_handoff_request.idempotency_key`.
- When handoff is not triggered, execution result is always `None`.

## CLI: build Track B handoff request

Use the CLI when you already have a serialized `Step6TrackCIntegrationResult` and
want only the deterministic Track B request payload.

```bash
PYTHONPATH=src python3 -m dart_pipeline.cli \
  handoff-request \
  --integration-json out/integration_result.json
```

Behavior:
- Prints `TrackBHandoffRequest` JSON to stdout.
- Exits with status `2` and an argparse-style error message when input JSON is invalid.
- Exits with status `2` when integration route is `TRACK_C` (`fallback_required=false`).

## CLI: Track C helper outputs

Use the CLI to parse local XBRL note files and emit deterministic helper payloads.

```bash
PYTHONPATH=src python3 -m dart_pipeline.cli \
  track-c-helpers \
  --xbrl-dir tests/fixtures/track_c/basic_bundle
```

Behavior:
- Prints JSON with `sga_accounts` and `segment_members`.
- Exits with status `2` and an argparse-style error message for invalid `--xbrl-dir` inputs.

## CLI: Track C deterministic routing from XBRL roles

Use the CLI to parse local XBRL notes and route deterministically from real role coverage.

```bash
PYTHONPATH=src python3 -m dart_pipeline.cli \
  track-c-route \
  --xbrl-dir tests/fixtures/track_c/basic_bundle \
  --required-role D822105 \
  --required-role D831150 \
  --required-role D838000 \
  --critical-role D851100 \
  --threshold 0.67
```

Behavior:
- Prints JSON with `decision`, `report`, and `fallback_required`.
- `fallback_required=true` when `decision.route` is `TRACK_B_FALLBACK`.
- Optional `--role-alias-json <path>` supports alias map JSON (`{"role_sga":"D831150"}`) for required/critical roles.
- Exits with status `2` and an argparse-style error for invalid `--xbrl-dir`, invalid `--threshold` (`[0,1]`), invalid `--role-alias-json`, or invalid routing input.

## Behavior policy (Step6)

- Scope is annual reports only: `reprt_code=11011`.
- Latest report selection is deterministic: sort by `(rcept_dt, rcept_no)` and take the last.
- Report filter uses API field `reprt_code` first; when omitted by `list.json`, deterministic fallback infers code from `report_nm` pattern.
- `fs_div` policy: `CFS` first, optional `OFS` fallback only when CFS fails with `NO_DATA`.
- Output rows are deterministically sorted by `(sj_div, ord, account_id, account_nm, source_row_idx)`.
- Step6+TrackC integration output includes: `track_a_snapshot`, `track_c_notes`, `routing_decision`, `coverage_report`, and `fallback_required`.
- If routing is `TRACK_B_FALLBACK`, `fallback_required` is `true` and Track B parsing remains out-of-scope for this step.
- Explicit execution path (`execute_step6_with_track_b_handoff`) returns integration result plus optional deterministic Track B handoff payload and trigger status.
