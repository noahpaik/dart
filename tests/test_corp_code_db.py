from __future__ import annotations

import io
import zipfile

import pytest

from dart_pipeline.corp_code_db import (
    CorpCodeDB,
    CorpCodeDbError,
    CorpCodeDbErrorCode,
)


def _zip_bytes(entries: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in entries.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def _corp_xml_bytes(rows: list[tuple[str, str, str, str]]) -> bytes:
    body = []
    for corp_code, corp_name, stock_code, modify_date in rows:
        body.append(
            """
            <list>
              <corp_code>{corp_code}</corp_code>
              <corp_name>{corp_name}</corp_name>
              <stock_code>{stock_code}</stock_code>
              <modify_date>{modify_date}</modify_date>
            </list>
            """.format(
                corp_code=corp_code,
                corp_name=corp_name,
                stock_code=stock_code,
                modify_date=modify_date,
            )
        )
    return ("<result>" + "".join(body) + "</result>").encode("utf-8")


def test_refresh_and_deterministic_name_stock_lookups(tmp_path) -> None:
    db_path = tmp_path / "corp.sqlite3"
    db = CorpCodeDB(db_path)
    try:
        xml_bytes = _corp_xml_bytes(
            [
                ("00100001", "Alpha", "123456", "20240201"),
                ("00100003", "Alpha", "", "20240202"),
                ("00100002", "Alpha Tech", "654321", "20240203"),
                ("00100004", "Alpha Labs", "", "20240204"),
            ]
        )
        inserted = db.refresh_from_zip_bytes(_zip_bytes({"CORPCODE.xml": xml_bytes}))
        assert inserted == 4

        exact = db.find_exact_name("Alpha")
        assert [row.corp_code for row in exact] == ["00100001", "00100003"]

        partial = db.find_partial_name("Alpha", limit=10)
        assert [row.corp_code for row in partial] == [
            "00100001",
            "00100002",
            "00100003",
            "00100004",
        ]

        stock = db.find_by_stock_code("654321")
        assert stock is not None
        assert stock.corp_code == "00100002"
        assert stock.is_listed is True
    finally:
        db.close()


def test_read_paths_do_not_implicitly_refresh(tmp_path) -> None:
    db_path = tmp_path / "corp.sqlite3"
    db = CorpCodeDB(db_path)
    try:
        assert db.count() == 0
        assert db.find_exact_name("Alpha") == []
        assert db.find_partial_name("Alpha") == []
        assert db.find_by_stock_code("123456") is None
        assert db.find_best_name_match("Alpha") is None
        assert db.count() == 0
    finally:
        db.close()


def test_refresh_is_atomic_on_write_failure(tmp_path) -> None:
    db_path = tmp_path / "corp.sqlite3"
    db = CorpCodeDB(db_path)
    try:
        initial_xml = _corp_xml_bytes(
            [("00100001", "Stable Corp", "111111", "20240201")]
        )
        db.refresh_from_zip_bytes(_zip_bytes({"CORPCODE.xml": initial_xml}))
        assert db.count() == 1

        duplicate_xml = _corp_xml_bytes(
            [
                ("00100001", "Dup Corp A", "111111", "20240201"),
                ("00100001", "Dup Corp B", "222222", "20240202"),
            ]
        )

        with pytest.raises(CorpCodeDbError) as exc_info:
            db.refresh_from_zip_bytes(_zip_bytes({"CORPCODE.xml": duplicate_xml}))

        assert exc_info.value.code == CorpCodeDbErrorCode.DB_WRITE_FAILED
        # Prior state remains intact after rollback.
        assert db.count() == 1
        best = db.find_best_name_match("Stable Corp")
        assert best is not None
        assert best.corp_code == "00100001"
    finally:
        db.close()


def test_refresh_rejects_entity_expansion_xml(tmp_path) -> None:
    db_path = tmp_path / "corp.sqlite3"
    db = CorpCodeDB(db_path)
    try:
        malicious_xml = b"""
        <!DOCTYPE result [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
        <result>
          <list>
            <corp_code>00100001</corp_code>
            <corp_name>&xxe;</corp_name>
            <stock_code>123456</stock_code>
            <modify_date>20240201</modify_date>
          </list>
        </result>
        """

        with pytest.raises(CorpCodeDbError) as exc_info:
            db.refresh_from_zip_bytes(_zip_bytes({"CORPCODE.xml": malicious_xml}))

        assert exc_info.value.code == CorpCodeDbErrorCode.MALFORMED_XML
    finally:
        db.close()


def test_refresh_fails_closed_without_defusedxml(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "corp.sqlite3"
    db = CorpCodeDB(db_path)
    try:
        monkeypatch.setattr("dart_pipeline.corp_code_db.DefusedElementTree", None)
        monkeypatch.setattr(
            "dart_pipeline.corp_code_db._DEFUSEDXML_IMPORT_ERROR",
            ModuleNotFoundError("defusedxml"),
        )

        xml_bytes = _corp_xml_bytes(
            [("00100001", "Secure Corp", "123456", "20240201")]
        )
        with pytest.raises(CorpCodeDbError) as exc_info:
            db.refresh_from_zip_bytes(_zip_bytes({"CORPCODE.xml": xml_bytes}))

        assert exc_info.value.code == CorpCodeDbErrorCode.MALFORMED_XML
        assert "defusedxml is required" in str(exc_info.value)
    finally:
        db.close()


def test_refresh_from_api_is_explicit(tmp_path) -> None:
    db_path = tmp_path / "corp.sqlite3"

    class StubApiClient:
        def __init__(self, payload: bytes) -> None:
            self.payload = payload
            self.calls = 0

        def download_corp_code_zip(self) -> bytes:
            self.calls += 1
            return self.payload

    xml_bytes = _corp_xml_bytes(
        [("00100001", "Api Corp", "123456", "20240201")]
    )
    client = StubApiClient(_zip_bytes({"CORPCODE.xml": xml_bytes}))

    db = CorpCodeDB(db_path)
    try:
        inserted = db.refresh_from_api(client)
        assert inserted == 1
        assert client.calls == 1
        assert db.find_best_name_match("Api Corp") is not None
    finally:
        db.close()
