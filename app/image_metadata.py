from __future__ import annotations

import struct

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
GIF87A_SIGNATURE = b"GIF87a"
GIF89A_SIGNATURE = b"GIF89a"
WEBP_RIFF_SIGNATURE = b"RIFF"
WEBP_WEBP_SIGNATURE = b"WEBP"
JPEG_SOI = b"\xff\xd8"
JPEG_SOF_MARKERS = {
    0xC0,
    0xC1,
    0xC2,
    0xC3,
    0xC5,
    0xC6,
    0xC7,
    0xC9,
    0xCA,
    0xCB,
    0xCD,
    0xCE,
    0xCF,
}
WELCOME_IMAGE_ALLOWED_EXTENSIONS = (".png", ".jpg", ".jpeg", ".webp", ".gif")
WELCOME_IMAGE_MIN_WIDTH = 64
WELCOME_IMAGE_MIN_HEIGHT = 64
WELCOME_IMAGE_MAX_WIDTH = 4096
WELCOME_IMAGE_MAX_HEIGHT = 4096


def detect_image_metadata(payload: bytes) -> dict | None:
    data = bytes(payload or b"")
    if not data:
        return None
    if data.startswith(PNG_SIGNATURE):
        return _detect_png_metadata(data)
    if data.startswith((GIF87A_SIGNATURE, GIF89A_SIGNATURE)):
        return _detect_gif_metadata(data)
    if data.startswith(JPEG_SOI):
        return _detect_jpeg_metadata(data)
    if data.startswith(WEBP_RIFF_SIGNATURE) and data[8:12] == WEBP_WEBP_SIGNATURE:
        return _detect_webp_metadata(data)
    return None


def _detect_png_metadata(data: bytes) -> dict | None:
    if len(data) < 24:
        return None
    width, height = struct.unpack(">II", data[16:24])
    if width <= 0 or height <= 0:
        return None
    return {
        "format": "PNG",
        "media_type": "image/png",
        "width": width,
        "height": height,
        "size_bytes": len(data),
    }


def _detect_gif_metadata(data: bytes) -> dict | None:
    if len(data) < 10:
        return None
    width, height = struct.unpack("<HH", data[6:10])
    if width <= 0 or height <= 0:
        return None
    return {
        "format": "GIF",
        "media_type": "image/gif",
        "width": width,
        "height": height,
        "size_bytes": len(data),
    }


def _detect_jpeg_metadata(data: bytes) -> dict | None:
    offset = 2
    data_length = len(data)
    while offset + 1 < data_length:
        if data[offset] != 0xFF:
            offset += 1
            continue
        marker = data[offset + 1]
        offset += 2
        while marker == 0xFF and offset < data_length:
            marker = data[offset]
            offset += 1
        if marker in {0xD8, 0xD9, 0x01} or 0xD0 <= marker <= 0xD7:
            continue
        if offset + 2 > data_length:
            return None
        segment_length = struct.unpack(">H", data[offset : offset + 2])[0]
        if segment_length < 2 or offset + segment_length > data_length:
            return None
        if marker in JPEG_SOF_MARKERS:
            if offset + 7 > data_length:
                return None
            height, width = struct.unpack(">HH", data[offset + 3 : offset + 7])
            if width <= 0 or height <= 0:
                return None
            return {
                "format": "JPEG",
                "media_type": "image/jpeg",
                "width": width,
                "height": height,
                "size_bytes": len(data),
            }
        offset += segment_length
    return None


def _detect_webp_metadata(data: bytes) -> dict | None:
    if len(data) < 30:
        return None
    chunk = data[12:16]
    if chunk == b"VP8 ":
        return _detect_webp_vp8_metadata(data)
    if chunk == b"VP8L":
        return _detect_webp_vp8l_metadata(data)
    if chunk == b"VP8X":
        return _detect_webp_vp8x_metadata(data)
    return None


def _detect_webp_vp8_metadata(data: bytes) -> dict | None:
    if len(data) < 30 or data[23:26] != b"\x9d\x01\x2a":
        return None
    width = struct.unpack("<H", data[26:28])[0] & 0x3FFF
    height = struct.unpack("<H", data[28:30])[0] & 0x3FFF
    if width <= 0 or height <= 0:
        return None
    return {
        "format": "WEBP",
        "media_type": "image/webp",
        "width": width,
        "height": height,
        "size_bytes": len(data),
    }


def _detect_webp_vp8l_metadata(data: bytes) -> dict | None:
    if len(data) < 25:
        return None
    if data[20] != 0x2F:
        return None
    bits = data[21] | (data[22] << 8) | (data[23] << 16) | (data[24] << 24)
    width = (bits & 0x3FFF) + 1
    height = ((bits >> 14) & 0x3FFF) + 1
    if width <= 0 or height <= 0:
        return None
    return {
        "format": "WEBP",
        "media_type": "image/webp",
        "width": width,
        "height": height,
        "size_bytes": len(data),
    }


def _detect_webp_vp8x_metadata(data: bytes) -> dict | None:
    if len(data) < 30:
        return None
    width_minus_one = data[24] | (data[25] << 8) | (data[26] << 16)
    height_minus_one = data[27] | (data[28] << 8) | (data[29] << 16)
    width = width_minus_one + 1
    height = height_minus_one + 1
    if width <= 0 or height <= 0:
        return None
    return {
        "format": "WEBP",
        "media_type": "image/webp",
        "width": width,
        "height": height,
        "size_bytes": len(data),
    }
