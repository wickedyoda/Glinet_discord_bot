from app.image_metadata import detect_image_metadata

PNG_1X1 = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00"
    b"\x1f\x15\xc4\x89"
    b"\x00\x00\x00\x0bIDATx\x9cc`\x00\x02\x00\x00\x05\x00\x01"
    b"\x0d\n-\xb4"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


def test_detect_image_metadata_for_png():
    metadata = detect_image_metadata(PNG_1X1)

    assert metadata is not None
    assert metadata["format"] == "PNG"
    assert metadata["media_type"] == "image/png"
    assert metadata["width"] == 1
    assert metadata["height"] == 1
    assert metadata["size_bytes"] == len(PNG_1X1)


def test_detect_image_metadata_rejects_invalid_payload():
    assert detect_image_metadata(b"not an image") is None
