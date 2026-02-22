# Step6 Plan Draft — DART API Track A Integration (Post Run-1 Extension)

## Positioning / scope alignment
- Step6 is a **post-Run-1 extension**.
- Run-1 guarantees stay intact:
  - existing core modules behavior unchanged unless explicitly extended
  - existing no-network regression tests must continue to pass
- Run-1 scope note in `docs/IMPLEMENTATION_SCOPE.md` remains valid.

## Goal
Add deterministic, production-safe Track A ingestion from OpenDART and expose a normalized snapshot for downstream validation/routing.

## Scope guard (avoid overengineering)
- No ORM/repository abstraction
- No DataFrame layer
- No background daemon/hidden scheduler
- No broad retry framework
- Only minimal modules required for Step6

## Deliverables
1. `src/dart_pipeline/dart_api.py`
   - `DartApiClient`
   - `download_corp_code_zip(...)`
   - `list_reports(...)` with pagination
   - `fetch_fnltt_singl_acnt_all(...)`
2. `src/dart_pipeline/corp_code_db.py`
   - sqlite schema/create
   - explicit refresh API (no implicit rebuild)
   - exact/partial name lookup + stock lookup
3. `src/dart_pipeline/pipeline_step6.py`
   - `build_track_a_snapshot(...)`
4. contracts extension in `src/dart_pipeline/contracts.py`
   - `TrackARow`
   - `TrackASnapshot`
5. docs
   - `docs/STEP6_API_USAGE.md`
6. tests
   - `tests/test_dart_api.py`
   - `tests/test_corp_code_db.py`
   - `tests/test_pipeline_step6.py`

## Contract definition (new)
### TrackARow
- required: `corp_code`, `rcept_no`, `rcept_dt`, `bsns_year`, `reprt_code`, `fs_div`, `sj_div`, `account_id`, `account_nm`, `ord`
- optional amounts:
  - raw strings: `thstrm_amount_raw`, `frmtrm_amount_raw`, `bfefrmtrm_amount_raw`
  - normalized numerics: `thstrm_amount`, `frmtrm_amount`, `bfefrmtrm_amount`
- numeric normalization rule:
  - keep raw strings unchanged
  - parse normalized numeric as `Decimal` (or exact integer when possible), not float

### TrackASnapshot
- required: `corp_code`, `rcept_no`, `rcept_dt`, `bsns_year`, `reprt_code`, `fs_div`, `rows`
- deterministic row sort key:
  `(sj_div, ord, account_id, account_nm, source_row_idx)`

## Deterministic algorithms
### Report list pagination
- call pages until `page_no == total_page`
- if status `013` or empty list => explicit no-report path

### Report filtering and latest selection
- primary filter: API field `reprt_code` when present
- deterministic fallback when `reprt_code` is omitted by `list.json`: infer code from `report_nm` pattern map (`사업보고서/반기보고서/분기보고서`)
- annual (`11011`) requires `report_nm` period token `({bsns_year}.12)`
- enforce regular disclosure filter `pblntf_ty=A`
- latest pick key: `(rcept_dt, rcept_no)` ascending then take last
- malformed/missing `rcept_dt` => typed `INVALID_RESPONSE_SCHEMA`

### Period policy (Step6)
- Step6 scope fixed to annual report `reprt_code=11011`
- quarterly (`11012/11013/11014`) cumulative-field handling deferred to Step6.2

### fs_div policy
- default CFS-first
- fallback to OFS only when:
  - fallback enabled, and
  - CFS returns `NO_DATA` only
- non-`NO_DATA` CFS failures must NOT fallback

## Error matrix (typed)
- `HTTP_ERROR` (non-200)
- `TIMEOUT`
- `MALFORMED_JSON`
- `MALFORMED_ZIP`
- `MALFORMED_XML`
- `DART_ERROR`
- `NO_DATA` (status 013 / empty result)
- `NO_REPORT_FOUND`
- `INVALID_RESPONSE_SCHEMA`
- `MISSING_API_KEY`
All exceptions redact API key.

## Security / side-effect guardrails
- request timeout mandatory
- response-size guard mandatory (max bytes)
- corp zip hardening mandatory:
  - expected xml entry exists
  - reject unsafe zip members
  - max uncompressed xml bytes
- API key from `DART_API_KEY` env only; never logged/persisted
- hardened XML parser: `defusedxml`
- corp DB writes only via explicit refresh call
- refresh atomicity: transaction + temp table/swap
- tests use tmp_path only
- no network in tests (mocked HTTP)

## Dependency/config changes
- `pyproject.toml` add:
  - `requests`
  - `defusedxml`
- document env usage in `docs/STEP6_API_USAGE.md`

## Test strategy
1) API client
- success
- status != 000
- status 013 -> NO_DATA
- HTTP error
- timeout
- malformed JSON
- oversize response guard
- malformed/unsafe zip handling
- API-key redaction in exception text
- missing `DART_API_KEY`

2) corp code DB
- exact name lookup
- ambiguous partial name deterministic ordering
- stock lookup
- explicit refresh only (no implicit write)
- atomic refresh rollback behavior
- malicious XML entity rejection

3) pipeline orchestration
- no report found
- deterministic latest report selection
- malformed `rcept_dt`
- strict reprt_code filter
- CFS->OFS fallback enabled/disabled branches
- non-`NO_DATA` CFS failure no fallback
- deterministic snapshot row ordering
- large-amount precision regression

4) regression
- existing Run-1 full suite stays green
- existing no-network regression tests stay green

## Acceptance criteria
- `PYTHONPATH=src python3 -m pytest -q` green
- Reviewer verdict `PASS`
- Final gate `RELEASE_READINESS: READY`
