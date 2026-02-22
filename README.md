# dart-pipeline (Run 1, Step 1)

Run-1 Step-1 scaffold with strict Pydantic v2 contracts and serialization tests.

## Status (2026-02-22)
- Step6 baseline + Track C routing integration path: release-ready.
- Track C Phase-C1: started and completed in this run.
- Track B explicit fallback handoff path is now wired (deterministic request + optional executor trigger).
- Phase-C1 delivered:
  - Track C contracts (`XbrlAccountRef`, `XbrlMemberRef`, `XbrlNote`)
  - XBRL parser (`src/dart_pipeline/track_c/xbrl_parser.py`)
  - Track C role routing helper (`route_from_track_c_roles`)
  - local fixture-based tests for parser/routing
  - Phase-C1 notes doc (`docs/TRACK_C_PHASE1.md`)
- Track B remains minimal fallback only for routing outcomes that fail Track C coverage/critical checks.

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

Supported runtime: Python `>=3.9`.

## Included in Step 1
- package scaffold under `src/dart_pipeline/`
- strict contract models in `src/dart_pipeline/contracts.py`
- contract roundtrip tests in `tests/test_contracts.py`
- frozen contract/output examples in `docs/CONTRACTS_AND_OUTPUTS.md`
- tie-out engine implementation in `src/dart_pipeline/validation/tieout.py`
- tie-out behavior tests in `tests/test_tieout.py`

## Not included yet
- restatement logic implementation
- coverage router implementation
- CLI implementation
