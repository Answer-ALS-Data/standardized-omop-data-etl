"""Map causes independently of people; retain explicit date exceptions and audit."""
import csv
import json
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
    groups = defaultdict(list)
    for i, row in enumerate(read(source / "mortality.csv"), 1):
        groups[row["Participant_ID"]].append((i, row))
    completed = defaultdict(list)
    for name, date, flag in [("alsfrs_r.csv", "alsfrsdt", "alsfdn"),
                             ("vital_signs.csv", "vsdt", "vsdn")]:
        for row in read(source / name):
            dt = offset(row[date])
            if offset(row[flag]) == 1 and dt is not None:
                completed[row["Participant_ID"]].append(dt)
    output, audit, errors = [], [], []
    for pid, entries in groups.items():
        row = entries[0][1]
        record = dict(Participant_ID=pid, source_rows=[i for i, _ in entries],
                      source_records=[r for _, r in entries])
        signatures = {(offset(r["dieddt"]), key(r)) for _, r in entries}
        if len(signatures) != 1 or not pid.strip():
            errors.append(dict(record, reason="Conflicting repeated death or missing identity"))
            continue
        k = key(row)
        if k not in lookup:
            errors.append(dict(record, reason="Unmapped cause/code pair"))
            continue
        dt = offset(row["dieddt"])
        reason = None
        if dt is None:
            reason = "Missing or nonintegral death date"
        elif dt < 0:
            reason = "Death precedes screening"
        elif any(v > dt for v in completed[pid]):
            reason = "Collected assessment after death"
        try:
            date = (datetime(2016, 1, 1) + timedelta(days=dt)).strftime("%Y-%m-%d") if reason is None else None
        except (OverflowError, ValueError):
            reason = "Unrepresentable death date"
        if reason:
            audit.append(dict(record, disposition="HOLD", reason=reason))
            continue
        parts = [f"mortality+dieddt (days since intake): {dt}"]
        for field, label in [("diedcaus", "death cause"), ("icd10cm", "ICD-10-CM code")]:
            if row[field].strip():
                parts.append(f"mortality+{field} ({label}): {row[field].strip()}")
        output.append(dict(person_id=pid, death_date=date, death_type_concept_id=32851,
                           cause_concept_id=lookup[k], cause_source_value=" | ".join(parts)))
        audit.append(dict(record, disposition="EMIT", cause_key=k, cause_concept_id=lookup[k], death_date=date))
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
