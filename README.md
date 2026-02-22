# dart-pipeline (Run 1, Step 1)

Run-1 Step-1 scaffold with strict Pydantic v2 contracts and serialization tests.

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
