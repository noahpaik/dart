# Implementation Scope (Worker/Reviewer Run 1)

Goal: Build a production-ready MVP skeleton for DART financial modeling pipeline focused on 3 critical improvements:

1. Tie-out validation with tolerance rules (PASS/WARN/FAIL)
2. Restatement-safe time series with dual views: As-reported + As-latest
3. Coverage-scored routing between Track C (XBRL) and Track B (HTML+LLM fallback)

Out of scope for run 1:
- Full DART API integration
- Full HTML/XBRL parser implementation
- Full Excel writer for all sheets

Required deliverables in this run:
- Package/module scaffold
- Core logic modules for 1~3
- Unit tests for above modules
- Simple CLI entrypoint to execute validation/routing demo
- Documentation for contracts and expected JSON outputs
