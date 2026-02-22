from __future__ import annotations

import io
import sqlite3
import zipfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Iterable

try:
    from defusedxml import ElementTree as DefusedElementTree
    from defusedxml.common import DefusedXmlException
except ModuleNotFoundError as exc:  # pragma: no cover - exercised via monkeypatch test
    DefusedElementTree = None

    class DefusedXmlException(Exception):
        """Placeholder exception type when defusedxml is unavailable."""

    _DEFUSEDXML_IMPORT_ERROR = exc
else:
    _DEFUSEDXML_IMPORT_ERROR = None

from dart_pipeline.dart_api import DartApiClient


class CorpCodeDbErrorCode(str, Enum):
    MALFORMED_ZIP = "MALFORMED_ZIP"
    MALFORMED_XML = "MALFORMED_XML"
    INVALID_RESPONSE_SCHEMA = "INVALID_RESPONSE_SCHEMA"
    DB_WRITE_FAILED = "DB_WRITE_FAILED"


class CorpCodeDbError(Exception):
    def __init__(self, code: CorpCodeDbErrorCode, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(self.__str__())

    def __str__(self) -> str:
        return f"{self.code.value}: {self.message}"


@dataclass(frozen=True)
class CorpCodeRecord:
    corp_code: str
    corp_name: str
    stock_code: str
    modify_date: str
    is_listed: bool


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


def _escape_like_term(term: str) -> str:
    return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class CorpCodeDB:
    def __init__(
        self,
        db_path: str | Path,
        *,
        max_corp_xml_bytes: int = 50_000_000,
    ) -> None:
        if max_corp_xml_bytes <= 0:
            raise ValueError("max_corp_xml_bytes must be > 0")

        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        self._max_corp_xml_bytes = int(max_corp_xml_bytes)
        self._ensure_schema()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "CorpCodeDB":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _ensure_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS corp (
                corp_code TEXT PRIMARY KEY,
                corp_name TEXT NOT NULL,
                stock_code TEXT NOT NULL DEFAULT '',
                modify_date TEXT NOT NULL DEFAULT '',
                is_listed INTEGER NOT NULL CHECK (is_listed IN (0, 1))
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_corp_name ON corp(corp_name)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_corp_stock_code ON corp(stock_code)"
        )
        self._conn.commit()

    def count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) AS cnt FROM corp").fetchone()
        if row is None:
            return 0
        return int(row["cnt"])

    def refresh_from_api(self, api_client: DartApiClient) -> int:
        zip_bytes = api_client.download_corp_code_zip()
        return self.refresh_from_zip_bytes(zip_bytes)

    def refresh_from_zip_bytes(self, zip_bytes: bytes) -> int:
        xml_bytes = self._extract_corp_xml_bytes(zip_bytes)
        records = self._parse_corp_xml_bytes(xml_bytes)
        self._replace_all_records(records)
        return len(records)

    def _extract_corp_xml_bytes(self, zip_bytes: bytes) -> bytes:
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
                infos = archive.infolist()
                if not infos:
                    raise CorpCodeDbError(
                        CorpCodeDbErrorCode.MALFORMED_ZIP,
                        "corp code zip is empty",
                    )

                xml_info: zipfile.ZipInfo | None = None
                for info in infos:
                    if not _is_safe_zip_member_name(info.filename):
                        raise CorpCodeDbError(
                            CorpCodeDbErrorCode.MALFORMED_ZIP,
                            f"unsafe zip member path detected: {info.filename}",
                        )
                    member_name = PurePosixPath(info.filename.replace("\\", "/")).name
                    if member_name.upper() == "CORPCODE.XML":
                        xml_info = info

                if xml_info is None:
                    raise CorpCodeDbError(
                        CorpCodeDbErrorCode.MALFORMED_ZIP,
                        "corp code zip missing CORPCODE.xml",
                    )

                if xml_info.file_size > self._max_corp_xml_bytes:
                    raise CorpCodeDbError(
                        CorpCodeDbErrorCode.MALFORMED_ZIP,
                        (
                            "CORPCODE.xml exceeds max_corp_xml_bytes "
                            f"({xml_info.file_size} > {self._max_corp_xml_bytes})"
                        ),
                    )

                xml_bytes = archive.read(xml_info)
                if len(xml_bytes) > self._max_corp_xml_bytes:
                    raise CorpCodeDbError(
                        CorpCodeDbErrorCode.MALFORMED_ZIP,
                        (
                            "CORPCODE.xml extracted bytes exceed max_corp_xml_bytes "
                            f"({len(xml_bytes)} > {self._max_corp_xml_bytes})"
                        ),
                    )
                return xml_bytes
        except zipfile.BadZipFile as exc:
            raise CorpCodeDbError(
                CorpCodeDbErrorCode.MALFORMED_ZIP,
                f"invalid zip bytes: {exc}",
            ) from exc
        except KeyError as exc:
            raise CorpCodeDbError(
                CorpCodeDbErrorCode.MALFORMED_ZIP,
                f"invalid zip entry: {exc}",
            ) from exc

    def _parse_corp_xml_bytes(self, xml_bytes: bytes) -> list[CorpCodeRecord]:
        if DefusedElementTree is None:
            raise CorpCodeDbError(
                CorpCodeDbErrorCode.MALFORMED_XML,
                f"defusedxml is required for secure XML parsing: {_DEFUSEDXML_IMPORT_ERROR}",
            )

        try:
            root = DefusedElementTree.fromstring(xml_bytes)
        except (DefusedXmlException, DefusedElementTree.ParseError, ValueError) as exc:
            raise CorpCodeDbError(
                CorpCodeDbErrorCode.MALFORMED_XML,
                f"invalid corp code xml: {exc}",
            ) from exc

        records: list[CorpCodeRecord] = []
        for item in root.findall(".//list"):
            corp_code = (item.findtext("corp_code") or "").strip()
            corp_name = (item.findtext("corp_name") or "").strip()
            stock_code = (item.findtext("stock_code") or "").strip()
            modify_date = (item.findtext("modify_date") or "").strip()

            if not corp_code or not corp_name:
                continue

            is_listed = stock_code.isdigit() and len(stock_code) == 6
            records.append(
                CorpCodeRecord(
                    corp_code=corp_code,
                    corp_name=corp_name,
                    stock_code=stock_code,
                    modify_date=modify_date,
                    is_listed=is_listed,
                )
            )

        records.sort(key=lambda row: (row.corp_code, row.corp_name, row.stock_code))
        return records

    def _replace_all_records(self, records: Iterable[CorpCodeRecord]) -> None:
        payload = [
            (
                row.corp_code,
                row.corp_name,
                row.stock_code,
                row.modify_date,
                1 if row.is_listed else 0,
            )
            for row in records
        ]

        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute("DROP TABLE IF EXISTS temp.corp_refresh")
            self._conn.execute(
                """
                CREATE TEMP TABLE corp_refresh (
                    corp_code TEXT PRIMARY KEY,
                    corp_name TEXT NOT NULL,
                    stock_code TEXT NOT NULL,
                    modify_date TEXT NOT NULL,
                    is_listed INTEGER NOT NULL CHECK (is_listed IN (0, 1))
                )
                """
            )
            self._conn.executemany(
                """
                INSERT INTO corp_refresh (corp_code, corp_name, stock_code, modify_date, is_listed)
                VALUES (?, ?, ?, ?, ?)
                """,
                payload,
            )
            self._conn.execute("DELETE FROM corp")
            self._conn.execute(
                """
                INSERT INTO corp (corp_code, corp_name, stock_code, modify_date, is_listed)
                SELECT corp_code, corp_name, stock_code, modify_date, is_listed
                FROM corp_refresh
                """
            )
            self._conn.execute("DROP TABLE corp_refresh")
            self._conn.commit()
        except sqlite3.Error as exc:
            self._conn.rollback()
            try:
                self._conn.execute("DROP TABLE IF EXISTS temp.corp_refresh")
            except sqlite3.Error:
                pass
            raise CorpCodeDbError(
                CorpCodeDbErrorCode.DB_WRITE_FAILED,
                f"refresh failed and transaction rolled back: {exc}",
            ) from exc

    def find_exact_name(self, corp_name: str) -> list[CorpCodeRecord]:
        if not isinstance(corp_name, str):
            raise ValueError("corp_name must be a string")
        normalized = corp_name.strip()
        if not normalized:
            return []

        rows = self._conn.execute(
            """
            SELECT corp_code, corp_name, stock_code, modify_date, is_listed
            FROM corp
            WHERE corp_name = ?
            ORDER BY corp_code ASC
            """,
            (normalized,),
        ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def find_partial_name(
        self,
        query: str,
        *,
        limit: int = 10,
    ) -> list[CorpCodeRecord]:
        if not isinstance(query, str):
            raise ValueError("query must be a string")
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")

        normalized = query.strip()
        if not normalized:
            return []

        escaped = _escape_like_term(normalized)
        rows = self._conn.execute(
            """
            SELECT corp_code, corp_name, stock_code, modify_date, is_listed
            FROM corp
            WHERE corp_name LIKE ? ESCAPE '\\'
            ORDER BY
                is_listed DESC,
                LENGTH(corp_name) ASC,
                corp_name COLLATE BINARY ASC,
                corp_code ASC
            LIMIT ?
            """,
            (f"%{escaped}%", limit),
        ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def find_by_stock_code(self, stock_code: str) -> CorpCodeRecord | None:
        if not isinstance(stock_code, str):
            raise ValueError("stock_code must be a string")

        normalized = stock_code.strip()
        if not normalized:
            return None

        row = self._conn.execute(
            """
            SELECT corp_code, corp_name, stock_code, modify_date, is_listed
            FROM corp
            WHERE stock_code = ?
            ORDER BY corp_code ASC
            LIMIT 1
            """,
            (normalized,),
        ).fetchone()
        return self._row_to_record(row) if row is not None else None

    def find_best_name_match(self, corp_name: str) -> CorpCodeRecord | None:
        exact = self.find_exact_name(corp_name)
        if exact:
            return exact[0]

        partial = self.find_partial_name(corp_name, limit=1)
        if partial:
            return partial[0]
        return None

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> CorpCodeRecord:
        return CorpCodeRecord(
            corp_code=str(row["corp_code"]),
            corp_name=str(row["corp_name"]),
            stock_code=str(row["stock_code"]),
            modify_date=str(row["modify_date"]),
            is_listed=bool(int(row["is_listed"])),
        )


__all__ = [
    "CorpCodeDB",
    "CorpCodeDbError",
    "CorpCodeDbErrorCode",
    "CorpCodeRecord",
]
