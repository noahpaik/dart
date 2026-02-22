from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _cli_env() -> dict[str, str]:
    env = os.environ.copy()
    src_path = str(REPO_ROOT / "src")
    current_pythonpath = env.get("PYTHONPATH")
    if current_pythonpath:
        env["PYTHONPATH"] = f"{src_path}{os.pathsep}{current_pythonpath}"
    else:
        env["PYTHONPATH"] = src_path
    return env


def _run_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "dart_pipeline.cli", *args],
        cwd=cwd,
        env=_cli_env(),
        capture_output=True,
        text=True,
        check=False,
    )


def _integration_payload(*, fallback_required: bool) -> dict[str, object]:
    if fallback_required:
        route = "TRACK_B_FALLBACK"
        reason_code = "CRITICAL_ROLE_MISSING"
        required_roles = ["d831150", "d851100"]
        missing_roles = ["d851100"]
        critical_missing_roles = ["d851100"]
        coverage_score = 0.5
    else:
        route = "TRACK_C"
        reason_code = "COVERAGE_PASS"
        required_roles = ["d831150"]
        missing_roles = []
        critical_missing_roles = []
        coverage_score = 1.0

    return {
        "track_a_snapshot": {
            "corp_code": "00126380",
            "rcept_no": "20240301000001",
            "rcept_dt": "20240301",
            "bsns_year": "2024",
            "reprt_code": "11011",
            "fs_div": "CFS",
            "rows": [
                {
                    "corp_code": "00126380",
                    "rcept_no": "20240301000001",
                    "rcept_dt": "20240301",
                    "bsns_year": "2024",
                    "reprt_code": "11011",
                    "fs_div": "CFS",
                    "sj_div": "BS",
                    "account_id": "ifrs-full_Assets",
                    "account_nm": "Assets",
                    "ord": 1,
                    "source_row_idx": 0,
                    "thstrm_amount_raw": "100",
                    "thstrm_amount": 100,
                    "frmtrm_amount_raw": "90",
                    "frmtrm_amount": 90,
                    "bfefrmtrm_amount_raw": "80",
                    "bfefrmtrm_amount": 80,
                }
            ],
        },
        "track_c_notes": [
            {
                "role_code": "D831150",
                "role_name": "Balance Sheet",
                "accounts": [],
                "members": [],
            }
        ],
        "routing_decision": {
            "route": route,
            "reason_code": reason_code,
        },
        "coverage_report": {
            "required_roles": required_roles,
            "found_roles": ["d831150"],
            "missing_roles": missing_roles,
            "critical_missing_roles": critical_missing_roles,
            "coverage_score": coverage_score,
        },
        "fallback_required": fallback_required,
    }


def test_cli_help_returns_success() -> None:
    result = _run_cli("--help", cwd=REPO_ROOT)

    assert result.returncode == 0
    assert "tieout" in result.stdout
    assert "restatement" in result.stdout
    assert "coverage" in result.stdout
    assert "handoff-request" in result.stdout


@pytest.mark.parametrize("command", ["tieout", "restatement", "coverage"])
def test_cli_demo_commands_smoke(command: str, tmp_path: Path) -> None:
    result = _run_cli(command, cwd=tmp_path)

    assert result.returncode == 0
    parsed = json.loads(result.stdout)
    assert isinstance(parsed, dict)


def test_cli_default_no_write_behavior(tmp_path: Path) -> None:
    result = _run_cli("tieout", cwd=tmp_path)

    assert result.returncode == 0
    assert not (tmp_path / "out").exists()
    assert list(tmp_path.iterdir()) == []


def test_cli_output_path_under_out_writes_successfully(tmp_path: Path) -> None:
    result = _run_cli("coverage", "--output", "out/demo_coverage.json", cwd=tmp_path)

    assert result.returncode == 0
    output_file = tmp_path / "out" / "demo_coverage.json"
    assert output_file.exists()

    payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert "decision" in payload
    assert "report" in payload


@pytest.mark.parametrize(
    "unsafe_output",
    [
        "../escape.json",
        "out/../escape.json",
    ],
)
def test_cli_rejects_traversal_paths(tmp_path: Path, unsafe_output: str) -> None:
    result = _run_cli("tieout", "--output", unsafe_output, cwd=tmp_path)

    assert result.returncode != 0
    assert "error:" in result.stderr.lower()
    assert "output" in result.stderr.lower()


def test_cli_rejects_absolute_path_outside_repo(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside.json"
    result = _run_cli("restatement", "--output", str(outside), cwd=tmp_path)

    assert result.returncode != 0
    assert "under ./out" in result.stderr


def test_cli_rejects_symlink_escape(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()

    link_path = out_dir / "link"
    try:
        link_path.symlink_to(outside_dir, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation unsupported in this test environment")

    result = _run_cli("tieout", "--output", "out/link/escaped.json", cwd=tmp_path)

    assert result.returncode != 0
    assert "under ./out" in result.stderr
    assert not (outside_dir / "escaped.json").exists()


def test_cli_handoff_request_success(tmp_path: Path) -> None:
    integration_path = tmp_path / "integration_fallback.json"
    integration_path.write_text(
        json.dumps(_integration_payload(fallback_required=True), indent=2),
        encoding="utf-8",
    )

    result = _run_cli(
        "handoff-request",
        "--integration-json",
        str(integration_path),
        cwd=tmp_path,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["corp_code"] == "00126380"
    assert payload["bsns_year"] == "2024"
    assert payload["reason_code"] == "CRITICAL_ROLE_MISSING"
    assert payload["missing_roles"] == ["d851100"]
    assert payload["critical_missing_roles"] == ["d851100"]
    assert payload["coverage_score"] == 0.5
    assert isinstance(payload["idempotency_key"], str)
    assert len(payload["idempotency_key"]) == 64


def test_cli_handoff_request_rejects_track_c_integration(tmp_path: Path) -> None:
    integration_path = tmp_path / "integration_track_c.json"
    integration_path.write_text(
        json.dumps(_integration_payload(fallback_required=False), indent=2),
        encoding="utf-8",
    )

    result = _run_cli(
        "handoff-request",
        "--integration-json",
        str(integration_path),
        cwd=tmp_path,
    )

    assert result.returncode == 2
    assert "Track B handoff request requires TRACK_B_FALLBACK route" in result.stderr
