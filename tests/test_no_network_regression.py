from __future__ import annotations

import os
import socket
import urllib.request
from pathlib import Path
from typing import Any

from dart_pipeline.cli import main as cli_main
from dart_pipeline.routing import route_by_coverage
from dart_pipeline.timeseries import build_dual_views
from dart_pipeline.validation import run_tieout


def _deny_network(*args: Any, **kwargs: Any) -> Any:
    raise AssertionError("network access is forbidden in run-1 regression tests")


def _patch_network_off(monkeypatch) -> None:
    monkeypatch.setattr(socket, "create_connection", _deny_network)
    monkeypatch.setattr(socket.socket, "connect", _deny_network, raising=False)
    monkeypatch.setattr(urllib.request, "urlopen", _deny_network)


def test_core_run1_modules_do_not_use_network(monkeypatch) -> None:
    _patch_network_off(monkeypatch)

    tieout = run_tieout(
        expected=[
            {
                "metric": "revenue",
                "period": "2024Q4",
                "unit": "KRW",
                "dimensions": {},
                "value": 100.0,
            }
        ],
        observed=[
            {
                "metric": "revenue",
                "period": "2024Q4",
                "unit": "KRW",
                "dimensions": {},
                "value": 100.0,
            }
        ],
        abs_tol=0.1,
        rel_tol=0.0,
        warn_multiplier=2.0,
    )
    assert tieout.summary.fail_count == 0

    as_reported, as_latest = build_dual_views(
        [
            {
                "metric": "revenue",
                "period": "2024Q4",
                "unit": "KRW",
                "dimensions": {},
                "value": 100.0,
                "filing_datetime_utc": "2024-03-01T00:00:00Z",
                "rcept_no": "20240301000001",
                "source_row_idx": 0,
            },
            {
                "metric": "revenue",
                "period": "2024Q4",
                "unit": "KRW",
                "dimensions": {},
                "value": 110.0,
                "filing_datetime_utc": "2024-04-01T00:00:00Z",
                "rcept_no": "20240401000001",
                "source_row_idx": 0,
            },
        ]
    )
    assert len(as_reported.records) == 2
    assert len(as_latest.records) == 1

    decision, report = route_by_coverage(
        required_roles=["income_statement", "balance_sheet"],
        found_roles=["income_statement"],
        critical_roles=[],
        threshold=0.8,
    )
    assert report is not None
    assert decision.reason_code.value in {"COVERAGE_BELOW_THRESHOLD", "COVERAGE_PASS", "CRITICAL_ROLE_MISSING"}


def test_cli_demo_commands_do_not_use_network(monkeypatch, tmp_path: Path) -> None:
    _patch_network_off(monkeypatch)

    cwd = Path.cwd()
    try:
        os.chdir(tmp_path)
        assert cli_main(["tieout"]) == 0
        assert cli_main(["restatement"]) == 0
        assert cli_main(["coverage"]) == 0
    finally:
        os.chdir(cwd)

    assert not (tmp_path / "out").exists()
