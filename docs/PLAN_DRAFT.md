# Plan Draft — Worker/Reviewer Run 1 (Revised)

## Objective
Implement MVP core for 3 critical improvements:
1) Tie-out validator with tolerance/severity
2) Restatement-aware dual-view time series (As-reported, As-latest)
3) Coverage-scored Track C→Track B routing

## Scope Guardrail (Run 1)
- ✅ In scope: core deterministic logic, contracts, tests, demo CLI
- ❌ Out of scope: live DART API calls, full XBRL/HTML parsers, full Excel production writer
- No network calls in tests or demo CLI path

## Package Scaffold (required deliverable)

```text
dart_pipeline/
  src/dart_pipeline/
    __init__.py
    contracts.py
    validation/tieout.py
    timeseries/restatement.py
    routing/coverage_router.py
    cli.py
  tests/
    test_contracts.py
    test_tieout.py
    test_restatement.py
    test_coverage_router.py
    test_cli_smoke.py
  docs/
    CONTRACTS_AND_OUTPUTS.md
  pyproject.toml
  README.md
```

## Data Contracts (minimal, run-1 only)
- `TieOutItem`:
  - keys: `metric`, `period`, `unit`, `dimensions` (dict[str,str])
  - values: `expected`, `observed`, `diff`, `abs_diff`, `tolerance`, `status`, `reason_code`
- `TieOutResult`:
  - `summary` (pass/warn/fail counts), `items` list
- `TimeSeriesView`:
  - `view_type`: `AS_REPORTED | AS_LATEST`
  - normalized records list with deterministic order
- `CoverageReport`:
  - `required_roles`, `found_roles`, `missing_roles`, `critical_missing_roles`, `coverage_score`
- `RoutingDecision`:
  - `route`: `TRACK_C | TRACK_B_FALLBACK`
  - `reason_code`: enum

## Deterministic Rules

### 1) Tie-out
- Identity key: `(metric, period, unit, sorted(dimensions.items()))`
- Relative base: `max(abs(expected), 1e-12)`
- Tolerance: `max(abs_tol, rel_tol * base)`
- Status boundaries:
  - `PASS` if `abs_diff <= tolerance`
  - `WARN` if `tolerance < abs_diff <= warn_multiplier * tolerance`
  - `FAIL` otherwise
- Missing/extra handling:
  - expected missing in observed => `FAIL / MISSING_OBSERVED`
  - observed extra key => `WARN / EXTRA_OBSERVED`

### 2) Restatement dual view
- Record identity: `(metric, period, unit, dimensions)`
- Latest ordering key: `(filing_datetime_utc, rcept_no)` ascending; last wins for AS_LATEST
- `AS_REPORTED`: keep all filing rows sorted by above key
- `AS_LATEST`: deduplicated latest snapshot sorted by `(metric, period, unit, dimensions)`

### 3) Coverage routing
- Normalize role ids: lowercase + trim + canonical map aliases
- Coverage formula: `len(required ∩ found) / len(required)`
  - if `required` is empty: coverage = `1.0`
- Decision:
  - if `critical_missing_roles` not empty => `TRACK_B_FALLBACK / CRITICAL_ROLE_MISSING`
  - elif `coverage_score >= threshold` => `TRACK_C / COVERAGE_PASS`
  - else => `TRACK_B_FALLBACK / COVERAGE_BELOW_THRESHOLD`
- Boundary: threshold uses `>=`

## Reason Code Enums
- Tie-out: `OK`, `MISSING_OBSERVED`, `EXTRA_OBSERVED`, `OUT_OF_TOLERANCE_WARN`, `OUT_OF_TOLERANCE_FAIL`
- Routing: `CRITICAL_ROLE_MISSING`, `COVERAGE_PASS`, `COVERAGE_BELOW_THRESHOLD`, `INVALID_INPUT`

## Test Strategy (required)
- Tie-out:
  - exact match, boundary equality, zero expected, negative values, null/invalid input
  - missing observed, extra observed
- Restatement:
  - out-of-order filings
  - same timestamp tie-break by `rcept_no`
  - deterministic output ordering
- Router:
  - empty required set
  - critical missing override with high score
  - threshold boundary (`==`)
- Contracts/CLI:
  - serialization round-trip
  - CLI smoke tests (no network)

## Security / Side-effect Guardrails
- No external network IO in run-1 implementation path
- File output only for explicit demo path (`./out/`)
- strict validation errors, no silent coercion

## Acceptance Criteria
- `pytest` all green
- reviewer verdict `PASS`
- deterministic outputs and documented JSON examples in `docs/CONTRACTS_AND_OUTPUTS.md`
