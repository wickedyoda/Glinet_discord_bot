import csv
import io

from app.csv_utils import build_csv_bytes, parse_csv_cells, parse_xlsx_cells, parse_spreadsheet_cells
from openpyxl import Workbook


def test_parse_csv_cells_supports_utf8_bom_and_multiple_cells():
    payload = "\ufeffAlpha, Beta\nGamma\n".encode()

    assert parse_csv_cells(payload) == ["Alpha", "Beta", "Gamma"]


def test_build_csv_bytes_escapes_formula_like_cells():
    payload = build_csv_bytes(
        ["display_name", "username"],
        [["=cmd", "+sum"], ["-value", "@mention"]],
    )

    reader = csv.reader(io.StringIO(payload.decode("utf-8")))
    rows = list(reader)

    assert rows == [
        ["display_name", "username"],
        ["'=cmd", "'+sum"],
        ["'-value", "'@mention"],
    ]


def test_parse_xlsx_cells_extracts_all_values():
    workbook = Workbook()
    sheet = workbook.active
    sheet["A1"] = "Alice"
    sheet["B1"] = "Bob"
    sheet["A2"] = "Charlie"
    sheet["B2"] = "Diana"
    
    excel_bytes = io.BytesIO()
    workbook.save(excel_bytes)
    excel_bytes.seek(0)
    
    result = parse_xlsx_cells(excel_bytes.getvalue())
    assert set(result) == {"Alice", "Bob", "Charlie", "Diana"}


def test_parse_spreadsheet_cells_handles_csv():
    payload = "Alpha, Beta\nGamma\n".encode()
    result = parse_spreadsheet_cells(payload, "names.csv")
    assert result == ["Alpha", "Beta", "Gamma"]


def test_parse_spreadsheet_cells_handles_xlsx():
    workbook = Workbook()
    sheet = workbook.active
    sheet["A1"] = "Alice"
    sheet["B1"] = "Bob"
    
    excel_bytes = io.BytesIO()
    workbook.save(excel_bytes)
    excel_bytes.seek(0)
    
    result = parse_spreadsheet_cells(excel_bytes.getvalue(), "names.xlsx")
    assert set(result) == {"Alice", "Bob"}
