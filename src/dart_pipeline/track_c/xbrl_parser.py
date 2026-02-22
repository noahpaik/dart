from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from defusedxml import ElementTree as DefusedElementTree

from dart_pipeline.contracts import XbrlAccountRef, XbrlMemberRef, XbrlNote, XbrlSource

NOTE_ROLES: dict[str, str] = {
    "D818000": "특수관계자",
    "D822100": "유형자산",
    "D822380": "금융상품위험",
    "D823180": "무형자산",
    "D826380": "재고자산",
    "D827570": "충당부채",
    "D831150": "수익분해",
    "D832610": "리스",
    "D834480": "종업원급여",
    "D835110": "법인세",
    "D838000": "주당이익",
    "D851100": "현금흐름조정",
    "D871100": "영업부문",
}

_LABEL_ROLE_URI = "http://www.xbrl.org/2003/role/label"
_LINK_NS = "http://www.xbrl.org/2003/linkbase"
_XLINK_NS = "http://www.w3.org/1999/xlink"
_NS = {"link": _LINK_NS, "xlink": _XLINK_NS}
_SKIP_TOKENS = ("Abstract", "Table", "LineItems", "Axis")
_ROLE_PATTERN = re.compile(r"role-([A-Za-z0-9]+)")
_ROLE_FALLBACK_PATTERN = re.compile(r"([DU]\d{6})", re.IGNORECASE)
_SGA_ACCOUNT_ID_KEYWORDS = (
    "SellingGeneralAdministrativeExpenses",
    "SalariesWages",
    "ProvisionForSeveranceIndemnities",
    "EmployeeBenefits",
    "MiscellaneousExpenses",
    "TotalSellingGeneralAdministrativeExpenses",
)
_SEGMENT_ROLE_NAMES = frozenset({"영업부문"})


@dataclass
class _NoteBucket:
    role_name: str
    accounts: dict[str, XbrlAccountRef] = field(default_factory=dict)
    members: dict[str, XbrlMemberRef] = field(default_factory=dict)


def _ensure_directory(xbrl_dir: str | Path) -> Path:
    if not isinstance(xbrl_dir, (str, Path)):
        raise ValueError("xbrl_dir must be str or pathlib.Path")
    path = Path(xbrl_dir)
    if not path.exists() or not path.is_dir():
        raise ValueError("xbrl_dir must point to an existing directory")
    return path


def _first_match(directory: Path, pattern: str) -> Path | None:
    matches = sorted(directory.glob(pattern))
    if not matches:
        return None
    return matches[0]


def discover_xbrl_linkbase_files(xbrl_dir: str | Path) -> dict[str, Path | None]:
    directory = _ensure_directory(xbrl_dir)
    return {
        "pre": _first_match(directory, "*_pre.xml"),
        "lab_ko": _first_match(directory, "*_lab-ko.xml"),
        "lab_en": _first_match(directory, "*_lab-en.xml"),
    }


def _extract_account_id(href: str) -> str:
    if not isinstance(href, str) or "#" not in href:
        return ""
    return href.rsplit("#", 1)[-1].strip()


def extract_role_code(role_uri: str) -> str:
    if not isinstance(role_uri, str):
        return ""

    matched = _ROLE_PATTERN.search(role_uri)
    if matched:
        return matched.group(1).upper()

    fallback_matched = _ROLE_FALLBACK_PATTERN.search(role_uri)
    if fallback_matched:
        return fallback_matched.group(1).upper()

    return ""


def _resolve_role_name(role_code: str, note_roles: Mapping[str, str]) -> str | None:
    if role_code in note_roles:
        return note_roles[role_code]

    if role_code.endswith("5"):
        adjusted = f"{role_code[:-1]}0"
        if adjusted in note_roles:
            return note_roles[adjusted]
    return None


def classify_source(account_id: str) -> XbrlSource:
    normalized = account_id.lower()
    if normalized.startswith("dart_"):
        return XbrlSource.DART
    if normalized.startswith("entity"):
        return XbrlSource.COMPANY
    return XbrlSource.IFRS


def _normalize_note_roles(note_roles: Mapping[str, str] | None) -> dict[str, str]:
    normalized = dict(NOTE_ROLES)
    if note_roles is None:
        return normalized
    if not isinstance(note_roles, Mapping):
        raise ValueError("note_roles must be a mapping of str to str")

    for raw_role_code, raw_role_name in note_roles.items():
        if not isinstance(raw_role_code, str) or not isinstance(raw_role_name, str):
            raise ValueError("note_roles must be a mapping of str to str")

        role_code = raw_role_code.strip().upper()
        role_name = raw_role_name.strip()
        if not role_code:
            raise ValueError("note_roles key must be non-empty")
        if not role_name:
            raise ValueError("note_roles value must be non-empty")
        normalized[role_code] = role_name
    return normalized


def _load_labels(label_file: Path | None) -> dict[str, str]:
    if label_file is None:
        return {}

    labels: dict[str, str] = {}
    root = DefusedElementTree.parse(label_file).getroot()

    for label_link in root.findall(".//link:labelLink", _NS):
        account_by_loc_label: dict[str, str] = {}
        text_by_label_resource: dict[str, str] = {}

        for loc in label_link.findall("link:loc", _NS):
            loc_label = loc.get(f"{{{_XLINK_NS}}}label", "")
            account_id = _extract_account_id(loc.get(f"{{{_XLINK_NS}}}href", ""))
            if loc_label and account_id:
                account_by_loc_label[loc_label] = account_id

        for label in label_link.findall("link:label", _NS):
            label_id = label.get(f"{{{_XLINK_NS}}}label", "")
            role = label.get(f"{{{_XLINK_NS}}}role", "")
            if not label_id or role != _LABEL_ROLE_URI:
                continue
            text = (label.text or "").strip()
            if text:
                text_by_label_resource[label_id] = text

        for label_arc in label_link.findall("link:labelArc", _NS):
            source = label_arc.get(f"{{{_XLINK_NS}}}from", "")
            target = label_arc.get(f"{{{_XLINK_NS}}}to", "")
            account_id = account_by_loc_label.get(source)
            label_text = text_by_label_resource.get(target)
            if account_id and label_text and account_id not in labels:
                labels[account_id] = label_text

    return labels


class XbrlParser:
    def __init__(
        self,
        xbrl_dir: str | Path,
        note_roles: Mapping[str, str] | None = None,
    ) -> None:
        self.xbrl_dir = _ensure_directory(xbrl_dir)
        self.note_roles = _normalize_note_roles(note_roles)
        self.paths = discover_xbrl_linkbase_files(self.xbrl_dir)
        self.labels_ko: dict[str, str] = {}
        self.labels_en: dict[str, str] = {}

    def parse(self) -> list[XbrlNote]:
        self.labels_ko = _load_labels(self.paths["lab_ko"])
        self.labels_en = _load_labels(self.paths["lab_en"])

        pre_file = self.paths["pre"]
        if pre_file is None:
            return []

        root = DefusedElementTree.parse(pre_file).getroot()
        notes_by_role: dict[str, _NoteBucket] = {}

        for presentation_link in root.findall(".//link:presentationLink", _NS):
            role_uri = presentation_link.get(f"{{{_XLINK_NS}}}role", "")
            role_code = extract_role_code(role_uri)
            if not role_code:
                continue

            role_name = _resolve_role_name(role_code, self.note_roles)
            if role_name is None:
                continue

            note_bucket = notes_by_role.setdefault(
                role_code,
                _NoteBucket(role_name=role_name),
            )

            for loc in presentation_link.findall("link:loc", _NS):
                account_id = _extract_account_id(loc.get(f"{{{_XLINK_NS}}}href", ""))
                if not account_id:
                    continue
                if any(token in account_id for token in _SKIP_TOKENS):
                    continue

                source = classify_source(account_id)

                if "Member" in account_id:
                    if account_id not in note_bucket.members:
                        note_bucket.members[account_id] = XbrlMemberRef(
                            account_id=account_id,
                            label_ko=self.labels_ko.get(account_id, ""),
                            source=source,
                        )
                    continue

                if account_id not in note_bucket.accounts:
                    note_bucket.accounts[account_id] = XbrlAccountRef(
                        account_id=account_id,
                        label_ko=self.labels_ko.get(account_id, ""),
                        label_en=self.labels_en.get(account_id, ""),
                        source=source,
                    )

        notes = [
            XbrlNote(
                role_code=role_code,
                role_name=bucket.role_name,
                accounts=list(bucket.accounts.values()),
                members=list(bucket.members.values()),
            )
            for role_code, bucket in notes_by_role.items()
        ]
        notes.sort(key=lambda note: (note.role_code, note.role_name))
        return notes

    def get_sga_accounts(self) -> dict[str, str]:
        return extract_sga_accounts(self.parse())

    def get_segment_members(self) -> list[XbrlMemberRef]:
        return extract_segment_members(self.parse())


def _ensure_notes_sequence(notes: Sequence[XbrlNote]) -> None:
    for index, note in enumerate(notes):
        if not isinstance(note, XbrlNote):
            raise ValueError(f"notes[{index}] must be XbrlNote")


def extract_sga_accounts(notes: Sequence[XbrlNote]) -> dict[str, str]:
    if not isinstance(notes, Sequence):
        raise ValueError("notes must be a sequence of XbrlNote")
    _ensure_notes_sequence(notes)

    sga_accounts: dict[str, str] = {}
    for note in notes:
        for account in note.accounts:
            if account.source != XbrlSource.DART:
                continue
            if not (
                any(keyword in account.account_id for keyword in _SGA_ACCOUNT_ID_KEYWORDS)
                or "판관비" in account.label_ko
            ):
                continue
            sga_accounts.setdefault(account.account_id, account.label_ko)

    return {account_id: sga_accounts[account_id] for account_id in sorted(sga_accounts)}


def extract_segment_members(notes: Sequence[XbrlNote]) -> list[XbrlMemberRef]:
    if not isinstance(notes, Sequence):
        raise ValueError("notes must be a sequence of XbrlNote")
    _ensure_notes_sequence(notes)

    segment_notes = [note for note in notes if note.role_name in _SEGMENT_ROLE_NAMES]
    target_notes = segment_notes if segment_notes else list(notes)

    members_by_id: dict[str, XbrlMemberRef] = {}
    for note in target_notes:
        for member in note.members:
            if member.source != XbrlSource.COMPANY:
                continue
            members_by_id.setdefault(member.account_id, member)

    return sorted(
        members_by_id.values(),
        key=lambda member: (member.account_id, member.source.value, member.label_ko),
    )


def parse_xbrl_notes(
    xbrl_dir: str | Path,
    note_roles: Mapping[str, str] | None = None,
) -> list[XbrlNote]:
    return XbrlParser(xbrl_dir=xbrl_dir, note_roles=note_roles).parse()


__all__ = [
    "NOTE_ROLES",
    "XbrlParser",
    "classify_source",
    "discover_xbrl_linkbase_files",
    "extract_role_code",
    "extract_segment_members",
    "extract_sga_accounts",
    "parse_xbrl_notes",
]
