from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
EVENT_ATOMS = ROOT / "data" / "nft_workload" / "derived" / "event_atoms_nft.csv"
REPORT_OUT = ROOT / "data" / "nft_workload" / "reports" / "nft_event_atoms_check.json"

EXPECTED_TYPES = {"NFTTrade", "NFTTransfer"}
NULL_STRINGS = {"", "nan", "none", "null", "<nil>", "nat"}


def clean_value(value: object) -> str:
    if pd.isna(value):
        return ""
    s = str(value).strip()
    if s.lower() in NULL_STRINGS:
        return ""
    return s


def present(series: pd.Series) -> pd.Series:
    return series.map(lambda x: clean_value(x) != "")


def count_missing(df: pd.DataFrame, cols: list[str]) -> dict[str, int]:
    return {c: int((~present(df[c])).sum()) for c in cols if c in df.columns}


def main() -> int:
    REPORT_OUT.parent.mkdir(parents=True, exist_ok=True)
    atoms = pd.read_csv(EVENT_ATOMS, low_memory=False, dtype=str)
    event_types = set(atoms["event_type"].dropna().astype(str).unique())
    parsed_time = pd.to_datetime(atoms["block_time"], errors="coerce", utc=True)

    trade = atoms[atoms["event_type"] == "NFTTrade"]
    transfer = atoms[atoms["event_type"] == "NFTTransfer"]
    from_to_equal = (
        present(transfer["from_address"])
        & present(transfer["to_address"])
        & (transfer["from_address"].astype(str).str.lower() == transfer["to_address"].astype(str).str.lower())
    )

    min_ts = parsed_time.min() if parsed_time.notna().any() else None
    max_ts = parsed_time.max() if parsed_time.notna().any() else None
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    end_exclusive = pd.Timestamp("2026-01-15T00:00:00Z")
    last_day = pd.Timestamp("2026-01-14T00:00:00Z")

    min_date = None if min_ts is None or pd.isna(min_ts) else min_ts.date().isoformat()
    max_date = None if max_ts is None or pd.isna(max_ts) else max_ts.date().isoformat()
    coverage = {
        "expected_start_inclusive": start.isoformat(),
        "expected_end_exclusive": end_exclusive.isoformat(),
        "min_time": None if min_ts is None or pd.isna(min_ts) else min_ts.isoformat(),
        "max_time": None if max_ts is None or pd.isna(max_ts) else max_ts.isoformat(),
        "min_date": min_date,
        "max_date": max_date,
        "starts_on_expected_start_date": bool(min_date == "2026-01-01"),
        "ends_before_expected_exclusive_end": bool(max_ts is not None and not pd.isna(max_ts) and max_ts < end_exclusive),
        "has_events_on_last_expected_day": bool(max_ts is not None and not pd.isna(max_ts) and max_ts >= last_day),
    }
    coverage["covers_2026_01_01_to_2026_01_15_exclusive"] = bool(
        coverage["starts_on_expected_start_date"]
        and coverage["ends_before_expected_exclusive_end"]
        and coverage["has_events_on_last_expected_day"]
    )

    report = {
        "input": str(EVENT_ATOMS.relative_to(ROOT)),
        "row_count": int(len(atoms)),
        "event_id_unique": bool(atoms["event_id"].is_unique),
        "duplicate_event_id_rows": int(atoms["event_id"].duplicated().sum()),
        "event_types": sorted(event_types),
        "event_types_valid": bool(event_types <= EXPECTED_TYPES),
        "nft_empty_rows": int((~present(atoms["nft"])).sum()),
        "block_time_parse_failed_rows": int(parsed_time.isna().sum()),
        "time_coverage": coverage,
        "trade_required_null_counts": count_missing(trade, ["buyer", "seller", "nft", "block_time"]),
        "transfer_required_null_counts": count_missing(transfer, ["from_address", "to_address", "nft", "block_time"]),
        "from_address_equals_to_address_rows": int(from_to_equal.sum()),
        "passed": False,
    }
    report["passed"] = bool(
        report["event_id_unique"]
        and report["event_types_valid"]
        and report["nft_empty_rows"] == 0
        and report["block_time_parse_failed_rows"] == 0
        and all(v == 0 for v in report["trade_required_null_counts"].values())
        and all(v == 0 for v in report["transfer_required_null_counts"].values())
        and report["time_coverage"]["covers_2026_01_01_to_2026_01_15_exclusive"]
    )
    REPORT_OUT.write_text(json.dumps(report, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=True))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
