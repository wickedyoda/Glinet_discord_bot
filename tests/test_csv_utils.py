import csv
import io

from app.csv_utils import build_csv_bytes, parse_csv_cells


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
