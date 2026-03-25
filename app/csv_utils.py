from __future__ import annotations

import csv
import io

_DANGEROUS_SPREADSHEET_PREFIXES = ("=", "+", "-", "@")


def decode_csv_bytes(data: bytes) -> str | None:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return None


def parse_csv_cells(data: bytes) -> list[str]:
    decoded = decode_csv_bytes(data)
    if decoded is None:
        return []

    values: list[str] = []
    reader = csv.reader(io.StringIO(decoded))
    for row in reader:
        for cell in row:
            candidate = cell.strip()
            if candidate:
                values.append(candidate)
    return values


def sanitize_csv_cell(value: object) -> str:
    text = str(value or "")
    if text.startswith(_DANGEROUS_SPREADSHEET_PREFIXES):
        return f"'{text}"
    return text


def build_csv_bytes(headers: list[str], rows: list[list[object]]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([sanitize_csv_cell(header) for header in headers])
    for row in rows:
        writer.writerow([sanitize_csv_cell(cell) for cell in row])
    return buffer.getvalue().encode("utf-8")
