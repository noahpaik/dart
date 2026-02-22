# Contracts and Outputs (Run 1)

This document freezes run-1 JSON contracts for tie-out, coverage routing, and restatement views.

## Contract Models

### `TieOutItem`
- keys: `metric`, `period`, `unit`, `dimensions`
- values: `expected`, `observed`, `diff`, `abs_diff`, `tolerance`, `status`, `reason_code`
- canonical identity key for deterministic behavior (shared utility):
  - `(metric, period, unit, tuple(sorted(dimensions.items())))`
- this canonical identity key is shared by:
  - tie-out matching/order
  - restatement dedupe/sort (`AS_LATEST` overwrite key)

### `TieOutSummary`
- `pass_count`, `warn_count`, `fail_count`

### `TieOutResult`
- `summary`: `TieOutSummary`
- `items`: list[`TieOutItem`]

### `CoverageReport`
- `required_roles`, `found_roles`, `missing_roles`, `critical_missing_roles`
- `coverage_score` in `[0.0, 1.0]`

### `RoutingDecision`
- `route`: `TRACK_C | TRACK_B_FALLBACK`
- `reason_code`: routing reason enum

### `TimeSeriesView`
- `view_type`: `AS_REPORTED | AS_LATEST`
- `records`: normalized record list (deterministic ordering is handled by restatement logic)

## Restatement Ordering Freeze

`filing_datetime_utc` ordering semantics are fixed for deterministic replay:

- strict UTC parse is required for `filing_datetime_utc` (must parse as UTC timestamp)
- deterministic tie-break key is frozen as:
  - `(filing_datetime_utc, rcept_no, source_row_idx)`
- `AS_REPORTED`: sort by the key above, ascending
- `AS_LATEST`: overwrite by canonical identity key after applying the same filing order

## Enums

### Tie-out status
- `PASS`
- `WARN`
- `FAIL`

### Tie-out reason code
- `OK`
- `MISSING_OBSERVED`
- `EXTRA_OBSERVED`
- `OUT_OF_TOLERANCE_WARN`
- `OUT_OF_TOLERANCE_FAIL`

### Routing reason code
- `CRITICAL_ROLE_MISSING`
- `COVERAGE_PASS`
- `COVERAGE_BELOW_THRESHOLD`
- `INVALID_INPUT`

## Invalid-input behavior (router contract)

Deterministic behavior is fixed as:
- `INVALID_INPUT` must map to `TRACK_B_FALLBACK`

Invalid input examples that must produce `INVALID_INPUT` in routing logic:
- threshold is outside `[0.0, 1.0]`
- required/found role inputs are not list[str]
- required fields are missing

Invalid-input matrix test coverage notes:
- current coverage: contract-level check that `INVALID_INPUT` deterministically maps to `TRACK_B_FALLBACK`
- placeholder for router matrix tests (Step 4 module): threshold bounds, role-list type validation, missing required fields
- pending tests are expected to be added in `tests/test_coverage_router.py` when router implementation lands

## Example Outputs

### Tie-out result
```json
{
  "summary": {
    "pass_count": 1,
    "warn_count": 1,
    "fail_count": 1
  },
  "items": [
    {
      "metric": "revenue",
      "period": "2024Q4",
      "unit": "KRW",
      "dimensions": {
        "region": "KR",
        "segment": "A"
      },
      "expected": 100.0,
      "observed": 100.0,
      "diff": 0.0,
      "abs_diff": 0.0,
      "tolerance": 0.5,
      "status": "PASS",
      "reason_code": "OK"
    },
    {
      "metric": "assets",
      "period": "2024Q4",
      "unit": "KRW",
      "dimensions": {
        "segment": "B"
      },
      "expected": 200.0,
      "observed": 200.8,
      "diff": 0.8,
      "abs_diff": 0.8,
      "tolerance": 0.5,
      "status": "WARN",
      "reason_code": "OUT_OF_TOLERANCE_WARN"
    },
    {
      "metric": "liabilities",
      "period": "2024Q4",
      "unit": "KRW",
      "dimensions": {
        "segment": "C"
      },
      "expected": 300.0,
      "observed": 302.0,
      "diff": 2.0,
      "abs_diff": 2.0,
      "tolerance": 0.5,
      "status": "FAIL",
      "reason_code": "OUT_OF_TOLERANCE_FAIL"
    }
  ]
}
```

### Coverage report
```json
{
  "required_roles": ["income_statement", "balance_sheet"],
  "found_roles": ["income_statement"],
  "missing_roles": ["balance_sheet"],
  "critical_missing_roles": ["balance_sheet"],
  "coverage_score": 0.5
}
```

### Routing decision
```json
{
  "route": "TRACK_C",
  "reason_code": "COVERAGE_PASS"
}
```

### Routing decision for invalid input
```json
{
  "route": "TRACK_B_FALLBACK",
  "reason_code": "INVALID_INPUT"
}
```
