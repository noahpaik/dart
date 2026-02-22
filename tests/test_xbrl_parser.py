from __future__ import annotations

from pathlib import Path

import pytest

from dart_pipeline.contracts import XbrlSource
from dart_pipeline.track_c.xbrl_parser import (
    NOTE_ROLES,
    XbrlParser,
    classify_source,
    discover_xbrl_linkbase_files,
    extract_role_code,
    extract_segment_members,
    extract_sga_accounts,
    parse_xbrl_notes,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "track_c" / "basic_bundle"


def _note_by_role(parsed_notes, role_code: str):
    return next(note for note in parsed_notes if note.role_code == role_code)


def test_discover_xbrl_linkbase_files_loads_expected_patterns() -> None:
    paths = discover_xbrl_linkbase_files(FIXTURE_DIR)

    assert paths["pre"] is not None
    assert paths["lab_ko"] is not None
    assert paths["lab_en"] is not None
    assert paths["pre"].name.endswith("_pre.xml")
    assert paths["lab_ko"].name.endswith("_lab-ko.xml")
    assert paths["lab_en"].name.endswith("_lab-en.xml")


def test_extract_role_code_supports_role_dash_pattern() -> None:
    role_uri = "http://dart.fss.or.kr/role/ifrs_15_role-D831150"
    assert extract_role_code(role_uri) == "D831150"


def test_classify_source_by_account_prefix() -> None:
    assert classify_source("dart_SalariesWages") == XbrlSource.DART
    assert classify_source("entity00134477_CustomMember") == XbrlSource.COMPANY
    assert classify_source("ifrs-full_Revenue") == XbrlSource.IFRS


def test_parse_xbrl_notes_loads_labels_and_applies_filters() -> None:
    notes = parse_xbrl_notes(FIXTURE_DIR)

    assert [note.role_code for note in notes] == ["D822105", "D831150", "D838000"]

    note_d831150 = _note_by_role(notes, "D831150")
    assert note_d831150.role_name == NOTE_ROLES["D831150"]
    assert [ref.account_id for ref in note_d831150.accounts] == [
        "dart_SalariesWagesSellingGeneralAdministrativeExpenses",
        "ifrs-full_RevenueFromContractsWithCustomers",
    ]
    assert [ref.account_id for ref in note_d831150.members] == [
        "entity00134477_SalesDomesticMember",
        "ifrs-full_OperatingSegmentsMember",
    ]

    dart_account = note_d831150.accounts[0]
    ifrs_account = note_d831150.accounts[1]
    assert dart_account.label_ko == "급여"
    assert dart_account.label_en == "Salaries and wages"
    assert dart_account.source == XbrlSource.DART
    assert ifrs_account.label_ko == "고객과의 계약에서 생기는 수익"
    assert ifrs_account.label_en == "Revenue from contracts with customers"
    assert ifrs_account.source == XbrlSource.IFRS

    company_member = note_d831150.members[0]
    ifrs_member = note_d831150.members[1]
    assert company_member.label_ko == "국내"
    assert company_member.source == XbrlSource.COMPANY
    assert ifrs_member.label_ko == "영업부문"
    assert ifrs_member.source == XbrlSource.IFRS

    note_d822105 = _note_by_role(notes, "D822105")
    assert note_d822105.role_name == NOTE_ROLES["D822100"]
    assert [ref.account_id for ref in note_d822105.accounts] == [
        "dart_DepreciationExpenseSellingGeneralAdministrativeExpenses",
        "ifrs-full_PropertyPlantAndEquipment",
    ]
    assert [ref.account_id for ref in note_d822105.members] == [
        "entity00134477_HeadquartersMember",
    ]

    assert "D999999" not in {note.role_code for note in notes}


def test_parse_xbrl_notes_is_deterministic() -> None:
    parsed_first = parse_xbrl_notes(FIXTURE_DIR)
    parsed_second = parse_xbrl_notes(FIXTURE_DIR)
    assert parsed_first == parsed_second


def test_extract_sga_accounts_and_parser_method() -> None:
    notes = parse_xbrl_notes(FIXTURE_DIR)

    expected = {
        "dart_DepreciationExpenseSellingGeneralAdministrativeExpenses": "감가상각비",
        "dart_SalariesWagesSellingGeneralAdministrativeExpenses": "급여",
    }

    assert extract_sga_accounts(notes) == expected

    parser = XbrlParser(FIXTURE_DIR)
    assert parser.get_sga_accounts() == expected


def test_extract_segment_members_and_parser_method() -> None:
    notes = parse_xbrl_notes(FIXTURE_DIR)

    extracted = extract_segment_members(notes)
    assert [member.account_id for member in extracted] == [
        "entity00134477_HeadquartersMember",
        "entity00134477_SalesDomesticMember",
    ]
    assert [member.label_ko for member in extracted] == ["본사", "국내"]
    assert all(member.source == XbrlSource.COMPANY for member in extracted)

    parser = XbrlParser(FIXTURE_DIR)
    assert parser.get_segment_members() == extracted


def test_parse_xbrl_notes_empty_dir_returns_empty_list(tmp_path: Path) -> None:
    assert parse_xbrl_notes(tmp_path) == []


def test_parse_xbrl_notes_invalid_input_errors() -> None:
    with pytest.raises(ValueError):
        parse_xbrl_notes(FIXTURE_DIR / "not_exists")

    with pytest.raises(ValueError):
        XbrlParser(FIXTURE_DIR, note_roles={" ": "role"})  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        extract_sga_accounts([object()])  # type: ignore[list-item]

    with pytest.raises(ValueError):
        extract_segment_members([object()])  # type: ignore[list-item]
