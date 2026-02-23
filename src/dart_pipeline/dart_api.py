from __future__ import annotations

import io
import json
import os
import re
import zipfile
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, Mapping

import requests


class DartApiErrorCode(str, Enum):
    HTTP_ERROR = "HTTP_ERROR"
    TIMEOUT = "TIMEOUT"
    MALFORMED_JSON = "MALFORMED_JSON"
    MALFORMED_ZIP = "MALFORMED_ZIP"
    MALFORMED_XML = "MALFORMED_XML"
    DART_ERROR = "DART_ERROR"
    NO_DATA = "NO_DATA"
    NO_REPORT_FOUND = "NO_REPORT_FOUND"
    INVALID_RESPONSE_SCHEMA = "INVALID_RESPONSE_SCHEMA"
    MISSING_API_KEY = "MISSING_API_KEY"


class DartApiError(Exception):
    def __init__(
        self,
        code: DartApiErrorCode,
        message: str,
        *,
        status: str | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.status = status
        super().__init__(self.__str__())

    def __str__(self) -> str:
        if self.status is None:
            return f"{self.code.value}: {self.message}"
        return f"{self.code.value} (status={self.status}): {self.message}"


_REDACTED_API_KEY = "[REDACTED_API_KEY]"
_STATUS_TAG_PATTERN = re.compile(r"<status>\s*([^<]+)\s*</status>", re.IGNORECASE)
_MESSAGE_TAG_PATTERN = re.compile(r"<message>\s*([^<]+)\s*</message>", re.IGNORECASE)


def _is_safe_zip_member_name(name: str) -> bool:
    normalized = name.replace("\\", "/")
    path = PurePosixPath(normalized)
    if path.is_absolute():
        return False
    if ".." in path.parts:
        return False
    if path.parts and ":" in path.parts[0]:
        return False
    return True


class DartApiClient:
    BASE_URL = "https://opendart.fss.or.kr/api"

    def __init__(
        self,
        *,
        timeout_seconds: float = 10.0,
        max_response_bytes: int = 10_000_000,
        max_corp_xml_bytes: int = 50_000_000,
    ) -> None:
        resolved_api_key = os.environ.get("DART_API_KEY")
        if not isinstance(resolved_api_key, str) or not resolved_api_key.strip():
            raise DartApiError(
                DartApiErrorCode.MISSING_API_KEY,
                "DART_API_KEY is required",
            )

        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")
        if max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be > 0")
        if max_corp_xml_bytes <= 0:
            raise ValueError("max_corp_xml_bytes must be > 0")

        self._api_key = resolved_api_key.strip()
        self._timeout_seconds = float(timeout_seconds)
        self._max_response_bytes = int(max_response_bytes)
        self._max_corp_xml_bytes = int(max_corp_xml_bytes)

    def _redact(self, text: str) -> str:
        if not text:
            return text
        return text.replace(self._api_key, _REDACTED_API_KEY)

    def _raise(
        self,
        code: DartApiErrorCode,
        message: str,
        *,
        status: str | None = None,
    ) -> None:
        raise DartApiError(code=code, message=self._redact(message), status=status)

    def _request_bytes(self, endpoint: str, params: Mapping[str, Any]) -> bytes:
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        payload_params = dict(params)
        payload_params["crtfc_key"] = self._api_key

        try:
            response = requests.get(
                url,
                params=payload_params,
                timeout=self._timeout_seconds,
                stream=True,
            )
        except requests.Timeout as exc:
            self._raise(
                DartApiErrorCode.TIMEOUT,
                f"request timed out for endpoint {endpoint}: {exc}",
            )
        except requests.RequestException as exc:
            self._raise(
                DartApiErrorCode.HTTP_ERROR,
                f"request failed for endpoint {endpoint}: {exc}",
            )

        try:
            if response.status_code != 200:
                self._raise(
                    DartApiErrorCode.HTTP_ERROR,
                    f"endpoint {endpoint} returned HTTP {response.status_code}",
                )

            content_length_header = response.headers.get("Content-Length")
            if content_length_header:
                try:
                    content_length = int(content_length_header)
                except ValueError:
                    content_length = None
                if content_length is not None and content_length > self._max_response_bytes:
                    self._raise(
                        DartApiErrorCode.HTTP_ERROR,
                        (
                            "response exceeded max_response_bytes "
                            f"({content_length} > {self._max_response_bytes}) for endpoint {endpoint}"
                        ),
                    )

            chunks: list[bytes] = []
            total = 0
            try:
                for chunk in response.iter_content(chunk_size=16_384):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > self._max_response_bytes:
                        self._raise(
                            DartApiErrorCode.HTTP_ERROR,
                            (
                                "response exceeded max_response_bytes "
                                f"({total} > {self._max_response_bytes}) for endpoint {endpoint}"
                            ),
                        )
                    chunks.append(chunk)
            except requests.Timeout as exc:
                self._raise(
                    DartApiErrorCode.TIMEOUT,
                    f"response stream timed out for endpoint {endpoint}: {exc}",
                )
            except requests.RequestException as exc:
                self._raise(
                    DartApiErrorCode.HTTP_ERROR,
                    f"response stream failed for endpoint {endpoint}: {exc}",
                )

            return b"".join(chunks)
        finally:
            response.close()

    def _request_json(self, endpoint: str, params: Mapping[str, Any]) -> dict[str, Any]:
        body = self._request_bytes(endpoint=endpoint, params=params)
        try:
            payload = json.loads(body)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            self._raise(
                DartApiErrorCode.MALFORMED_JSON,
                f"invalid JSON response from endpoint {endpoint}: {exc}",
            )

        if not isinstance(payload, dict):
            self._raise(
                DartApiErrorCode.INVALID_RESPONSE_SCHEMA,
                f"response payload must be an object for endpoint {endpoint}",
            )
        return payload

    def _extract_xml_status_message(self, payload_bytes: bytes) -> tuple[str | None, str | None]:
        if not payload_bytes:
            return None, None

        decoded = payload_bytes.decode("utf-8", errors="ignore")
        status_match = _STATUS_TAG_PATTERN.search(decoded)
        if status_match is None:
            return None, None

        status = status_match.group(1).strip()
        if not status:
            return None, None

        message_match = _MESSAGE_TAG_PATTERN.search(decoded)
        if message_match is None:
            return status, None

        message = message_match.group(1).strip()
        if not message:
            return status, None
        return status, message

    def _extract_dart_status(
        self,
        payload: Mapping[str, Any],
        *,
        endpoint: str,
    ) -> str:
        raw_status = payload.get("status")
        if not isinstance(raw_status, str):
            self._raise(
                DartApiErrorCode.INVALID_RESPONSE_SCHEMA,
                f"missing or invalid status in endpoint {endpoint} response",
            )
        return raw_status

    def _validate_corp_zip(self, zip_bytes: bytes) -> None:
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
                infos = archive.infolist()
                if not infos:
                    self._raise(
                        DartApiErrorCode.MALFORMED_ZIP,
                        "corp code zip is empty",
                    )

                corp_xml_info: zipfile.ZipInfo | None = None
                for info in infos:
                    if not _is_safe_zip_member_name(info.filename):
                        self._raise(
                            DartApiErrorCode.MALFORMED_ZIP,
                            f"unsafe zip member path detected: {info.filename}",
                        )
                    member_name = PurePosixPath(info.filename.replace("\\", "/")).name
                    if member_name.upper() == "CORPCODE.XML":
                        corp_xml_info = info

                if corp_xml_info is None:
                    self._raise(
                        DartApiErrorCode.MALFORMED_ZIP,
                        "corp code zip missing CORPCODE.xml",
                    )

                if corp_xml_info.file_size > self._max_corp_xml_bytes:
                    self._raise(
                        DartApiErrorCode.MALFORMED_ZIP,
                        (
                            "CORPCODE.xml exceeds max_corp_xml_bytes "
                            f"({corp_xml_info.file_size} > {self._max_corp_xml_bytes})"
                        ),
                    )

                xml_bytes = archive.read(corp_xml_info)
                if len(xml_bytes) > self._max_corp_xml_bytes:
                    self._raise(
                        DartApiErrorCode.MALFORMED_ZIP,
                        (
                            "CORPCODE.xml extracted bytes exceed max_corp_xml_bytes "
                            f"({len(xml_bytes)} > {self._max_corp_xml_bytes})"
                        ),
                    )
        except zipfile.BadZipFile as exc:
            self._raise(DartApiErrorCode.MALFORMED_ZIP, f"invalid zip bytes: {exc}")
        except KeyError as exc:
            self._raise(DartApiErrorCode.MALFORMED_ZIP, f"invalid zip entry: {exc}")

    def _validate_fnltt_xbrl_zip(self, zip_bytes: bytes) -> None:
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
                infos = archive.infolist()
                if not infos:
                    self._raise(
                        DartApiErrorCode.MALFORMED_ZIP,
                        "fnltt xbrl zip is empty",
                    )

                file_member_count = 0
                for info in infos:
                    if not _is_safe_zip_member_name(info.filename):
                        self._raise(
                            DartApiErrorCode.MALFORMED_ZIP,
                            f"unsafe zip member path detected: {info.filename}",
                        )
                    if not info.is_dir():
                        file_member_count += 1

                if file_member_count == 0:
                    self._raise(
                        DartApiErrorCode.MALFORMED_ZIP,
                        "fnltt xbrl zip has no files",
                    )
        except zipfile.BadZipFile as exc:
            self._raise(DartApiErrorCode.MALFORMED_ZIP, f"invalid zip bytes: {exc}")
        except KeyError as exc:
            self._raise(DartApiErrorCode.MALFORMED_ZIP, f"invalid zip entry: {exc}")

    def download_corp_code_zip(self) -> bytes:
        zip_bytes = self._request_bytes(endpoint="corpCode.xml", params={})
        self._validate_corp_zip(zip_bytes)
        return zip_bytes

    def download_fnltt_xbrl_zip(self, *, rcept_no: str, reprt_code: str) -> bytes:
        if not isinstance(rcept_no, str) or not rcept_no.strip():
            raise ValueError("rcept_no must be a non-empty string")
        if not isinstance(reprt_code, str) or not reprt_code.strip():
            raise ValueError("reprt_code must be a non-empty string")

        zip_bytes = self._request_bytes(
            endpoint="fnlttXbrl.xml",
            params={
                "rcept_no": rcept_no.strip(),
                "reprt_code": reprt_code.strip(),
            },
        )

        if not zipfile.is_zipfile(io.BytesIO(zip_bytes)):
            status, message = self._extract_xml_status_message(zip_bytes)
            if status is not None:
                detail = "fnlttXbrl.xml returned status payload instead of zip bytes"
                if message is not None:
                    detail = f"{detail}: {message}"
                self._raise(
                    DartApiErrorCode.DART_ERROR,
                    detail,
                    status=status,
                )
            self._raise(
                DartApiErrorCode.MALFORMED_ZIP,
                "fnlttXbrl.xml did not return zip bytes",
            )

        self._validate_fnltt_xbrl_zip(zip_bytes)
        return zip_bytes

    def list_reports(
        self,
        *,
        corp_code: str,
        bgn_de: str,
        end_de: str,
        page_count: int = 100,
        pblntf_ty: str = "A",
    ) -> list[dict[str, Any]]:
        if not isinstance(corp_code, str) or not corp_code.strip():
            raise ValueError("corp_code must be a non-empty string")
        if not isinstance(bgn_de, str) or len(bgn_de) != 8 or not bgn_de.isdigit():
            raise ValueError("bgn_de must be an 8-digit YYYYMMDD string")
        if not isinstance(end_de, str) or len(end_de) != 8 or not end_de.isdigit():
            raise ValueError("end_de must be an 8-digit YYYYMMDD string")
        if not isinstance(page_count, int) or page_count <= 0:
            raise ValueError("page_count must be a positive integer")
        if pblntf_ty != "A":
            raise ValueError("pblntf_ty must be 'A' for regular disclosures")

        all_reports: list[dict[str, Any]] = []
        page_no = 1
        total_page = 1

        while page_no <= total_page:
            payload = self._request_json(
                endpoint="list.json",
                params={
                    "corp_code": corp_code.strip(),
                    "bgn_de": bgn_de,
                    "end_de": end_de,
                    "pblntf_ty": pblntf_ty,
                    "page_no": page_no,
                    "page_count": page_count,
                },
            )
            status = self._extract_dart_status(payload, endpoint="list.json")
            if status == "013":
                self._raise(DartApiErrorCode.NO_DATA, "no reports found", status=status)
            if status != "000":
                message = payload.get("message")
                self._raise(
                    DartApiErrorCode.DART_ERROR,
                    (
                        "list.json returned non-success status: "
                        f"{message}" if isinstance(message, str) else "list.json returned non-success status"
                    ),
                    status=status,
                )

            raw_total_page = payload.get("total_page", 1)
            if isinstance(raw_total_page, str) and raw_total_page.isdigit():
                parsed_total_page = int(raw_total_page)
            elif isinstance(raw_total_page, int):
                parsed_total_page = raw_total_page
            else:
                self._raise(
                    DartApiErrorCode.INVALID_RESPONSE_SCHEMA,
                    "invalid total_page in list.json response",
                )
            if parsed_total_page <= 0:
                self._raise(
                    DartApiErrorCode.INVALID_RESPONSE_SCHEMA,
                    "total_page must be >= 1 in list.json response",
                )
            total_page = parsed_total_page

            raw_list = payload.get("list", [])
            if raw_list is None:
                raw_list = []
            if not isinstance(raw_list, list):
                self._raise(
                    DartApiErrorCode.INVALID_RESPONSE_SCHEMA,
                    "list field must be an array in list.json response",
                )

            for index, row in enumerate(raw_list):
                if not isinstance(row, Mapping):
                    self._raise(
                        DartApiErrorCode.INVALID_RESPONSE_SCHEMA,
                        f"list.json list[{index}] must be an object",
                    )
                all_reports.append(dict(row))

            page_no += 1

        if not all_reports:
            self._raise(DartApiErrorCode.NO_DATA, "no reports found", status="013")

        return all_reports

    def fetch_fnltt_singl_acnt_all(
        self,
        *,
        corp_code: str,
        bsns_year: str,
        reprt_code: str,
        fs_div: str,
    ) -> list[dict[str, Any]]:
        if not isinstance(corp_code, str) or not corp_code.strip():
            raise ValueError("corp_code must be a non-empty string")
        if not isinstance(bsns_year, str) or len(bsns_year) != 4 or not bsns_year.isdigit():
            raise ValueError("bsns_year must be a 4-digit string")
        if not isinstance(reprt_code, str) or not reprt_code.strip():
            raise ValueError("reprt_code must be a non-empty string")
        if fs_div not in {"CFS", "OFS"}:
            raise ValueError("fs_div must be 'CFS' or 'OFS'")

        payload = self._request_json(
            endpoint="fnlttSinglAcntAll.json",
            params={
                "corp_code": corp_code.strip(),
                "bsns_year": bsns_year,
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            },
        )

        status = self._extract_dart_status(payload, endpoint="fnlttSinglAcntAll.json")
        if status == "013":
            self._raise(
                DartApiErrorCode.NO_DATA,
                "no account rows returned",
                status=status,
            )
        if status != "000":
            message = payload.get("message")
            self._raise(
                DartApiErrorCode.DART_ERROR,
                (
                    "fnlttSinglAcntAll.json returned non-success status: "
                    f"{message}"
                    if isinstance(message, str)
                    else "fnlttSinglAcntAll.json returned non-success status"
                ),
                status=status,
            )

        raw_list = payload.get("list", [])
        if raw_list is None:
            raw_list = []
        if not isinstance(raw_list, list):
            self._raise(
                DartApiErrorCode.INVALID_RESPONSE_SCHEMA,
                "list field must be an array in fnlttSinglAcntAll.json response",
            )

        if not raw_list:
            self._raise(
                DartApiErrorCode.NO_DATA,
                "no account rows returned",
                status="013",
            )

        rows: list[dict[str, Any]] = []
        for index, row in enumerate(raw_list):
            if not isinstance(row, Mapping):
                self._raise(
                    DartApiErrorCode.INVALID_RESPONSE_SCHEMA,
                    f"fnlttSinglAcntAll.json list[{index}] must be an object",
                )
            rows.append(dict(row))
        return rows


__all__ = [
    "DartApiClient",
    "DartApiError",
    "DartApiErrorCode",
]
