# Track C Phase-C1

## Scope
- Add strict Track C contracts:
  - `XbrlAccountRef(account_id, label_ko, label_en, source)`
  - `XbrlMemberRef(account_id, label_ko, source)`
  - `XbrlNote(role_code, role_name, accounts, members)`
- Add local XBRL parser (`src/dart_pipeline/track_c/xbrl_parser.py`) for:
  - `*_pre.xml`, `*_lab-ko.xml`, `*_lab-en.xml` discovery
  - safe parsing via `defusedxml.ElementTree`
  - role-code extraction from `xlink:role` URIs
  - role-name mapping from default `NOTE_ROLES`
  - account/member split and structural-node skip
- Add Track C routing helper:
  - `route_from_track_c_roles(...)`
  - derives `found_roles` from parsed note role codes
  - delegates routing decision to existing `route_by_coverage`

## Rules
- Source classification:
  - `dart_*` -> `dart`
  - `entity*` -> `company`
  - otherwise -> `ifrs`
- Structural nodes are skipped when collecting references:
  - `Abstract`, `Table`, `LineItems`, `Axis`
- Deterministic ordering:
  - notes sorted by `(role_code, role_name)`
  - accounts sorted by `(account_id, source, label_ko, label_en)`
  - members sorted by `(account_id, source, label_ko)`
- No network calls in parser/routing/tests.

## Example JSON

```json
{
  "role_code": "D831150",
  "role_name": "수익분해",
  "accounts": [
    {
      "account_id": "dart_SalariesWagesSellingGeneralAdministrativeExpenses",
      "label_ko": "급여",
      "label_en": "Salaries and wages",
      "source": "dart"
    },
    {
      "account_id": "ifrs-full_RevenueFromContractsWithCustomers",
      "label_ko": "고객과의 계약에서 생기는 수익",
      "label_en": "Revenue from contracts with customers",
      "source": "ifrs"
    }
  ],
  "members": [
    {
      "account_id": "entity00134477_SalesDomesticMember",
      "label_ko": "국내",
      "source": "company"
    }
  ]
}
```
