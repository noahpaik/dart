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

## Behavior policy (Step6)

- Scope is annual reports only: `reprt_code=11011`.
- Latest report selection is deterministic: sort by `(rcept_dt, rcept_no)` and take the last.
- Report filter uses API field `reprt_code` first; when omitted by `list.json`, deterministic fallback infers code from `report_nm` pattern.
- `fs_div` policy: `CFS` first, optional `OFS` fallback only when CFS fails with `NO_DATA`.
- Output rows are deterministically sorted by `(sj_div, ord, account_id, account_nm, source_row_idx)`.
