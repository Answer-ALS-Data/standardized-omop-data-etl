"""Map causes independently; preserve legacy dates and one output per source row."""
import csv
import json
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path


def read(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def key(row):
    return (" ".join(row["diedcaus"].lower().split()),
            "".join(row["icd10cm"].upper().split()))


def offset(value):
    try:
        d = Decimal(value)
        return int(d) if d.is_finite() and d == d.to_integral_value() else None
    except (InvalidOperation, ValueError):
        return None


def load_lookup(path):
    lookup = {}
    for row in read(path):
        k = (row["diedcaus_key"], row["icd10cm_key"])
        if k != key(dict(diedcaus=k[0], icd10cm=k[1])):
            raise ValueError("Noncanonical mortality lookup key")
        if k in lookup:
            raise ValueError("Duplicate mortality lookup key")
        if row["mapping_status"] not in {"RETAINED", "APPROVED"}:
            raise ValueError("Unresolved mortality mapping")
        cid = row["cause_concept_id"]
        if not cid.isdigit():
            raise ValueError("Invalid mortality target")
        lookup[k] = int(cid)
    return lookup


def main():
    source = Path("source_tables")
    lookup = load_lookup(source / "usagi/mortality-cause-mapping.csv")
    source_rows = read(source / "mortality.csv")
    dates = pd.read_csv(source / "mortality.csv")["dieddt"]
    groups = defaultdict(list)
    for i, row in enumerate(source_rows, 1):
        groups[row["Participant_ID"]].append((i, row))
    completed = defaultdict(list)
    for name, date, flag in [("alsfrs_r.csv", "alsfrsdt", "alsfdn"),
                             ("vital_signs.csv", "vsdt", "vsdn")]:
        if not (source / name).exists():
            continue
        for row in read(source / name):
            dt = offset(row[date])
            if offset(row[flag]) == 1 and dt is not None:
                completed[row["Participant_ID"]].append(dt)
    output, audit, errors = [], [], []
    for i, row in enumerate(source_rows, 1):
        pid = row["Participant_ID"]
        entries = groups[pid]
        record = dict(Participant_ID=pid, source_rows=[i], source_records=[row])
        warnings = []
        if len(entries) > 1:
            warnings.append("Repeated participant mortality rows preserved without deduplication")
        k = key(row)
        if k not in lookup:
            errors.append(dict(record, reason="Unmapped cause/code pair"))
            continue
        # Match the original pandas NA recognition, conversion and sentinel policy.
        value = dates.iloc[i - 1]
        if pd.isna(value):
            date, dt = "1900-01-01", None
            warnings.append("Missing death date represented by legacy 1900-01-01 sentinel")
        else:
            date = (datetime(2016, 1, 1) + timedelta(days=float(value))).strftime("%Y-%m-%d")
            dt = int(value)
            if float(value) < 0:
                warnings.append("Death precedes screening")
            if any(v > float(value) for v in completed[pid]):
                warnings.append("Collected assessment after death")
        parts = [f"mortality+dieddt (days since intake): {dt}"] if dt is not None else []
        for field, label in [("diedcaus", "death cause"), ("icd10cm", "ICD-10-CM code")]:
            if row[field].strip():
                parts.append(f"mortality+{field} ({label}): {row[field].strip()}")
        if not parts:
            parts.append("mortality+dieddt (days since intake)")
        output.append(dict(person_id=pid, death_date=date, death_type_concept_id=32851,
                           cause_concept_id=lookup[k], cause_source_value=" | ".join(parts)))
        audit.append(dict(record, disposition="EMIT", cause_key=k, cause_concept_id=lookup[k], death_date=date, warnings=warnings))
    Path("logs").mkdir(exist_ok=True)
    Path("logs/mortality-reconciliation.json").write_text(json.dumps(dict(records=audit, errors=errors), indent=2)+"\n")
    if errors:
        raise ValueError("Mortality reconciliation failed; see logs/mortality-reconciliation.json")
    with Path("processed_source/mortality--death.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["person_id", "death_date", "death_type_concept_id", "cause_concept_id", "cause_source_value"])
        w.writeheader()
        w.writerows(output)


if __name__ == "__main__":
    main()
