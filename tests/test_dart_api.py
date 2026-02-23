from __future__ import annotations

import io
import json
import zipfile

import pytest
import requests

from dart_pipeline.dart_api import DartApiClient, DartApiError, DartApiErrorCode


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        payload: dict | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        if content is not None:
            self.content = content
        elif payload is not None:
            self.content = json.dumps(payload).encode("utf-8")
        else:
            self.content = b""

    def iter_content(self, chunk_size: int = 16_384):
        size = max(1, int(chunk_size))
        for offset in range(0, len(self.content), size):
            yield self.content[offset : offset + size]

    def close(self) -> None:
        return None


def _zip_bytes(entries: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in entries.items():
            archive.writestr(name, data)
    return buffer.getvalue()


@pytest.fixture(autouse=True)
def _set_default_api_key(monkeypatch) -> None:
    monkeypatch.setenv("DART_API_KEY", "demo-key")


def test_client_requires_api_key(monkeypatch) -> None:
    monkeypatch.delenv("DART_API_KEY", raising=False)
    with pytest.raises(DartApiError) as exc_info:
        DartApiClient()

    assert exc_info.value.code == DartApiErrorCode.MISSING_API_KEY


def test_list_reports_paginates_until_total_page(monkeypatch) -> None:
    calls: list[int] = []

    def fake_get(url: str, params: dict, timeout: float, stream: bool = False) -> FakeResponse:
        assert timeout == 3.0
        assert "crtfc_key" in params
        page_no = int(params["page_no"])
        calls.append(page_no)
        if page_no == 1:
            return FakeResponse(
                payload={
                    "status": "000",
                    "total_page": "2",
                    "list": [
                        {
                            "corp_code": "00126380",
                            "rcept_no": "20240301000001",
                            "rcept_dt": "20240301",
                            "reprt_code": "11011",
                        }
                    ],
                }
            )
        return FakeResponse(
            payload={
                "status": "000",
                "total_page": "2",
                "list": [
                    {
                        "corp_code": "00126380",
                        "rcept_no": "20240315000001",
                        "rcept_dt": "20240315",
                        "reprt_code": "11011",
                    }
                ],
            }
        )

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)

    client = DartApiClient(timeout_seconds=3.0)
    reports = client.list_reports(
        corp_code="00126380",
        bgn_de="20240101",
        end_de="20241231",
    )

    assert calls == [1, 2]
    assert [row["rcept_no"] for row in reports] == ["20240301000001", "20240315000001"]


def test_list_reports_status_013_raises_no_data(monkeypatch) -> None:
    def fake_get(url: str, params: dict, timeout: float, stream: bool = False) -> FakeResponse:
        return FakeResponse(payload={"status": "013", "message": "no data"})

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)

    client = DartApiClient()
    with pytest.raises(DartApiError) as exc_info:
        client.list_reports(corp_code="00126380", bgn_de="20240101", end_de="20241231")

    assert exc_info.value.code == DartApiErrorCode.NO_DATA


def test_list_reports_status_000_with_empty_list_raises_no_data(monkeypatch) -> None:
    def fake_get(url: str, params: dict, timeout: float, stream: bool = False) -> FakeResponse:
        return FakeResponse(payload={"status": "000", "total_page": "1", "list": []})

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)

    client = DartApiClient()
    with pytest.raises(DartApiError) as exc_info:
        client.list_reports(corp_code="00126380", bgn_de="20240101", end_de="20241231")

    assert exc_info.value.code == DartApiErrorCode.NO_DATA


def test_fetch_fnltt_status_error_and_malformed_json(monkeypatch) -> None:
    responses = [
        FakeResponse(payload={"status": "010", "message": "invalid key"}),
        FakeResponse(content=b"{bad json"),
    ]

    def fake_get(url: str, params: dict, timeout: float, stream: bool = False) -> FakeResponse:
        return responses.pop(0)

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)
    client = DartApiClient()

    with pytest.raises(DartApiError) as exc_info_1:
        client.fetch_fnltt_singl_acnt_all(
            corp_code="00126380",
            bsns_year="2024",
            reprt_code="11011",
            fs_div="CFS",
        )
    assert exc_info_1.value.code == DartApiErrorCode.DART_ERROR

    with pytest.raises(DartApiError) as exc_info_2:
        client.fetch_fnltt_singl_acnt_all(
            corp_code="00126380",
            bsns_year="2024",
            reprt_code="11011",
            fs_div="CFS",
        )
    assert exc_info_2.value.code == DartApiErrorCode.MALFORMED_JSON


def test_timeout_redacts_api_key_in_error_message(monkeypatch) -> None:
    monkeypatch.setenv("DART_API_KEY", "demo-secret-key")

    def fake_get(url: str, params: dict, timeout: float, stream: bool = False):
        raise requests.Timeout("timeout for demo-secret-key")

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)

    client = DartApiClient()
    with pytest.raises(DartApiError) as exc_info:
        client.list_reports(corp_code="00126380", bgn_de="20240101", end_de="20241231")

    assert exc_info.value.code == DartApiErrorCode.TIMEOUT
    rendered = str(exc_info.value)
    assert "demo-secret-key" not in rendered
    assert "[REDACTED_API_KEY]" in rendered


def test_stream_read_timeout_maps_to_typed_timeout(monkeypatch) -> None:
    monkeypatch.setenv("DART_API_KEY", "demo-secret-key")

    class StreamTimeoutResponse(FakeResponse):
        def iter_content(self, chunk_size: int = 16_384):
            raise requests.Timeout("stream timeout for demo-secret-key")

    def fake_get(url: str, params: dict, timeout: float, stream: bool = False):
        return StreamTimeoutResponse(content=b"")

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)

    client = DartApiClient()
    with pytest.raises(DartApiError) as exc_info:
        client.download_corp_code_zip()

    assert exc_info.value.code == DartApiErrorCode.TIMEOUT
    rendered = str(exc_info.value)
    assert "demo-secret-key" not in rendered
    assert "[REDACTED_API_KEY]" in rendered


def test_response_size_guard(monkeypatch) -> None:
    def fake_get(url: str, params: dict, timeout: float, stream: bool = False) -> FakeResponse:
        return FakeResponse(content=b"x" * 100)

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)

    client = DartApiClient(max_response_bytes=32)
    with pytest.raises(DartApiError) as exc_info:
        client.download_corp_code_zip()

    assert exc_info.value.code == DartApiErrorCode.HTTP_ERROR
    assert "max_response_bytes" in str(exc_info.value)


def test_http_non_200_maps_to_http_error(monkeypatch) -> None:
    def fake_get(url: str, params: dict, timeout: float, stream: bool = False) -> FakeResponse:
        return FakeResponse(status_code=500, payload={"status": "999"})

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)

    client = DartApiClient()
    with pytest.raises(DartApiError) as exc_info:
        client.list_reports(corp_code="00126380", bgn_de="20240101", end_de="20241231")

    assert exc_info.value.code == DartApiErrorCode.HTTP_ERROR


def test_content_length_header_oversize_guard(monkeypatch) -> None:
    def fake_get(url: str, params: dict, timeout: float, stream: bool = False) -> FakeResponse:
        return FakeResponse(
            content=b"x",
            headers={"Content-Length": "2048"},
        )

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)

    client = DartApiClient(max_response_bytes=32)
    with pytest.raises(DartApiError) as exc_info:
        client.download_corp_code_zip()

    assert exc_info.value.code == DartApiErrorCode.HTTP_ERROR
    assert "max_response_bytes" in str(exc_info.value)


def test_download_corp_code_zip_rejects_malformed_and_unsafe_zip(monkeypatch) -> None:
    malformed_zip = FakeResponse(content=b"not-a-zip")
    unsafe_zip = FakeResponse(
        content=_zip_bytes(
            {
                "../CORPCODE.xml": b"<result></result>",
            }
        )
    )
    missing_xml_zip = FakeResponse(content=_zip_bytes({"other.xml": b"<result></result>"}))

    responses = [malformed_zip, unsafe_zip, missing_xml_zip]

    def fake_get(url: str, params: dict, timeout: float, stream: bool = False) -> FakeResponse:
        return responses.pop(0)

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)
    client = DartApiClient()

    for _ in range(3):
        with pytest.raises(DartApiError) as exc_info:
            client.download_corp_code_zip()
        assert exc_info.value.code == DartApiErrorCode.MALFORMED_ZIP


def test_download_corp_code_zip_success(monkeypatch) -> None:
    corp_zip = _zip_bytes({"CORPCODE.xml": b"<result></result>"})

    def fake_get(url: str, params: dict, timeout: float, stream: bool = False) -> FakeResponse:
        return FakeResponse(content=corp_zip)

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)
    client = DartApiClient()

    downloaded = client.download_corp_code_zip()
    assert downloaded == corp_zip


def test_download_fnltt_xbrl_zip_success(monkeypatch) -> None:
    xbrl_zip = _zip_bytes(
        {
            "entity00134477_2025-12-31_pre.xml": b"<result></result>",
            "entity00134477_2025-12-31_lab-ko.xml": b"<result></result>",
        }
    )

    def fake_get(url: str, params: dict, timeout: float, stream: bool = False) -> FakeResponse:
        assert url.endswith("/fnlttXbrl.xml")
        assert params["rcept_no"] == "20250331000001"
        assert params["reprt_code"] == "11011"
        assert "crtfc_key" in params
        return FakeResponse(content=xbrl_zip)

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)
    client = DartApiClient()

    downloaded = client.download_fnltt_xbrl_zip(
        rcept_no="20250331000001",
        reprt_code="11011",
    )
    assert downloaded == xbrl_zip


def test_download_fnltt_xbrl_zip_status_payload_raises_dart_error(monkeypatch) -> None:
    status_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<result>
  <status>014</status>
  <message>file does not exist</message>
</result>
"""

    def fake_get(url: str, params: dict, timeout: float, stream: bool = False) -> FakeResponse:
        return FakeResponse(content=status_xml)

    monkeypatch.setattr("dart_pipeline.dart_api.requests.get", fake_get)
    client = DartApiClient()

    with pytest.raises(DartApiError) as exc_info:
        client.download_fnltt_xbrl_zip(
            rcept_no="20250331000001",
            reprt_code="11011",
        )

    assert exc_info.value.code == DartApiErrorCode.DART_ERROR
    assert exc_info.value.status == "014"
    assert "file does not exist" in str(exc_info.value)
