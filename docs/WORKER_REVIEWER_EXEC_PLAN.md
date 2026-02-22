# Worker/Reviewer Codex CLI 실행 계획 (Run 1)

## 상태 요약
- Plan validation 2회 수행 결과: `REVISE`
- 남은 must-fix 5개 반영 후 구현 시작 권장

## Must-fix 반영 포인트 (구현 전 고정)
1. **Identity canonicalization 통일**
   - 키 규칙: `(metric, period, unit, tuple(sorted(dimensions.items())))`
   - tie-out, restatement dedupe/sort 모두 동일 키 사용
2. **INVALID_INPUT 결정표 확정**
   - threshold 범위(0~1), role list 타입(str), 필수 필드 누락 처리 명시
   - invalid 시 deterministic route: `TRACK_B_FALLBACK / INVALID_INPUT`
3. **Restatement 정렬 완전 고정**
   - `filing_datetime_utc` strict UTC parse
   - `(filing_datetime_utc, rcept_no, source_row_idx)` tie-break
4. **CLI side-effect 제한**
   - 기본 no-write
   - 출력 시 `./out/` 하위만 허용 (`../` 차단)
5. **테스트 보강**
   - dimensions 순서불변성, duplicate-key tie, role alias normalize 양측, invalid-input branch, no-network, path restriction

---

## 단계별 실행 (Codex CLI)

### Step 1) Plan freeze + contracts skeleton
**목표:** 데이터 계약과 결정 규칙 고정

**Worker prompt**
```bash
codex exec --full-auto "ROLE: Worker (Implementer)
Implement plan freeze artifacts:
- docs/CONTRACTS_AND_OUTPUTS.md
- src/dart_pipeline/contracts.py (minimal run-1 fields only)
Include explicit enums and examples for: tieout statuses/reason codes, routing reason codes, invalid input behavior.
No network code.
Add tests for contract serialization roundtrip.
"
```

**Reviewer prompt**
```bash
codex exec "ROLE: Reviewer (Validator)
Review contract/docs changes only.
Check: deterministic keys, enum completeness, overengineering risk.
Output format:
VERDICT: PASS|CHANGES_REQUIRED
BLOCKERS:
NICE_TO_HAVE:
REGRESSION_RISK:
RELEASE_READINESS:
"
```

**Gate:** `VERDICT: PASS`

---

### Step 2) Tie-out 엔진 구현
**목표:** tolerance 기반 PASS/WARN/FAIL

**Worker prompt**
```bash
codex exec --full-auto "ROLE: Worker (Implementer)
Implement src/dart_pipeline/validation/tieout.py
Rules:
- identity canonicalization key shared util
- tolerance = max(abs_tol, rel_tol*max(abs(expected),1e-12))
- PASS <= tol, WARN <= warn_multiplier*tol, else FAIL
- missing expected->FAIL(MISSING_OBSERVED), extra observed->WARN(EXTRA_OBSERVED)
Add unit tests incl. boundary equality, negatives, zero baseline, null/invalid inputs.
"
```

**Reviewer prompt**
```bash
codex exec "ROLE: Reviewer (Validator)
Review tieout module + tests for correctness/security/side-effects.
Do not implement.
Use PASS/CHANGES_REQUIRED format.
"
```

**Gate:** boundary test + missing/extra test 포함 `pytest -q`

---

### Step 3) Restatement dual view
**목표:** As-reported / As-latest 일관 출력

**Worker prompt**
```bash
codex exec --full-auto "ROLE: Worker (Implementer)
Implement src/dart_pipeline/timeseries/restatement.py
- strict UTC parse for filing_datetime_utc
- deterministic ordering by (filing_datetime_utc, rcept_no, source_row_idx)
- As-reported: preserve filing sequence
- As-latest: latest overwrite by canonical identity key
Add tests for out-of-order filings, same timestamp tie-break, deterministic ordering.
"
```

**Reviewer prompt**
```bash
codex exec "ROLE: Reviewer (Validator)
Review restatement logic for deterministic overwrite and edge cases.
Return strict verdict format.
"
```

**Gate:** replay twice 결과 identical

---

### Step 4) Coverage router
**목표:** Track C↔B fallback 라우팅 명확화

**Worker prompt**
```bash
codex exec --full-auto "ROLE: Worker (Implementer)
Implement src/dart_pipeline/routing/coverage_router.py
- normalize role ids (strip/lower/alias map)
- coverage = |required∩found|/|required| ; required empty => 1.0
- precedence: critical missing > threshold pass >= threshold > below threshold
- invalid input => TRACK_B_FALLBACK/INVALID_INPUT
Add tests for threshold boundary, critical override, empty required, alias normalize both sets.
"
```

**Reviewer prompt**
```bash
codex exec "ROLE: Reviewer (Validator)
Review routing decision matrix and invalid-input branches.
Return verdict format.
"
```

**Gate:** decision matrix test 100% 통과

---

### Step 5) CLI demo + side-effect guard
**목표:** 실행 확인 가능한 최소 CLI

**Worker prompt**
```bash
codex exec --full-auto "ROLE: Worker (Implementer)
Implement src/dart_pipeline/cli.py as thin wrapper only.
- default no-write
- optional --output only under ./out (resolved path check)
- no network usage
Add test_cli_smoke.py and path restriction tests.
"
```

**Reviewer prompt**
```bash
codex exec "ROLE: Reviewer (Validator)
Review CLI side-effect controls and UX clarity.
Return verdict format.
"
```

**Gate:** path traversal blocked test PASS

---

### Step 6) 최종 릴리즈 게이트
**명령:**
```bash
pytest -q
```

**Reviewer final checklist:**
- Requirement fit
- Correctness
- Risk/security/side-effect
- Code quality
- User flow

최종 출력:
- `VERDICT: PASS | CHANGES_REQUIRED`
- `RELEASE_READINESS: READY | NOT_READY`

---

## 실행 원칙
- CHANGES_REQUIRED loop 최대 2회
- 2회 초과 시 사용자 의사결정 요청
- commit/PR은 Reviewer `PASS` 이후만
