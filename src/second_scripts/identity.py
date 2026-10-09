"""Separate opaque source identity from strict generated numeric keys.

SubjectUID and Participant_ID are opaque keys; never pass them to integer_id.
"""
import csv
import re
from decimal import Decimal
from pathlib import Path


class IdentityError(ValueError):
    pass


def source_id(value):
    if not isinstance(value, str):
        raise IdentityError('Source identity must be text')
    text = value.strip()
    if not text or text.lower() in ('na', 'nan', 'none', 'null'):
        raise IdentityError('Missing source identity')
    return text


def integer_id(value):
    text = str(value).strip()
    if not re.fullmatch(r'[0-9]+(?:\.0+)?', text):
        raise IdentityError('Expected a positive integer ID or integral decimal representation')
    number = int(Decimal(text))
    if number <= 0:
        raise IdentityError('ID must be positive')
    return str(number)


def final_id(value):
    number = integer_id(value)
    if len(number) > 7:
        raise IdentityError('ID exceeds the seven-digit namespace; refusing truncation')
    return '11' + number.zfill(7)


def read_table(path):
    with Path(path).open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        rows = list(reader)
        if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames):
            raise IdentityError('Missing or duplicate CSV headers')
        if any(None in row or any(v is None for v in row.values()) for row in rows):
            raise IdentityError('Malformed CSV row')
        return reader.fieldnames, rows


def write_table(path, columns, rows):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)
