from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import NamedTuple

logger = logging.getLogger("invite_bot.translate")

LANGUAGE_NAMES: dict[str, str] = {
    "af": "Afrikaans",
    "sq": "Albanian",
    "am": "Amharic",
    "ar": "Arabic",
    "hy": "Armenian",
    "az": "Azerbaijani",
    "eu": "Basque",
    "be": "Belarusian",
    "bn": "Bengali",
    "bs": "Bosnian",
    "bg": "Bulgarian",
    "ca": "Catalan",
    "ceb": "Cebuano",
    "ny": "Chichewa",
    "zh": "Chinese (Simplified)",
    "zh-cn": "Chinese (Simplified)",
    "zh-tw": "Chinese (Traditional)",
    "co": "Corsican",
    "hr": "Croatian",
    "cs": "Czech",
    "da": "Danish",
    "nl": "Dutch",
    "en": "English",
    "eo": "Esperanto",
    "et": "Estonian",
    "tl": "Filipino",
    "fi": "Finnish",
    "fr": "French",
    "fy": "Frisian",
    "gl": "Galician",
    "ka": "Georgian",
    "de": "German",
    "el": "Greek",
    "gu": "Gujarati",
    "ht": "Haitian Creole",
    "ha": "Hausa",
    "haw": "Hawaiian",
    "iw": "Hebrew",
    "he": "Hebrew",
    "hi": "Hindi",
    "hmn": "Hmong",
    "hu": "Hungarian",
    "is": "Icelandic",
    "ig": "Igbo",
    "id": "Indonesian",
    "ga": "Irish",
    "it": "Italian",
    "ja": "Japanese",
    "jw": "Javanese",
    "kn": "Kannada",
    "kk": "Kazakh",
    "km": "Khmer",
    "rw": "Kinyarwanda",
    "ko": "Korean",
    "ku": "Kurdish",
    "ky": "Kyrgyz",
    "lo": "Lao",
    "la": "Latin",
    "lv": "Latvian",
    "lt": "Lithuanian",
    "lb": "Luxembourgish",
    "mk": "Macedonian",
    "mg": "Malagasy",
    "ms": "Malay",
    "ml": "Malayalam",
    "mt": "Maltese",
    "mi": "Maori",
    "mr": "Marathi",
    "mn": "Mongolian",
    "my": "Myanmar (Burmese)",
    "ne": "Nepali",
    "no": "Norwegian",
    "or": "Odia",
    "ps": "Pashto",
    "fa": "Persian",
    "pl": "Polish",
    "pt": "Portuguese",
    "pa": "Punjabi",
    "ro": "Romanian",
    "ru": "Russian",
    "sm": "Samoan",
    "gd": "Scots Gaelic",
    "sr": "Serbian",
    "st": "Sesotho",
    "sn": "Shona",
    "sd": "Sindhi",
    "si": "Sinhala",
    "sk": "Slovak",
    "sl": "Slovenian",
    "so": "Somali",
    "es": "Spanish",
    "su": "Sundanese",
    "sw": "Swahili",
    "sv": "Swedish",
    "tg": "Tajik",
    "ta": "Tamil",
    "tt": "Tatar",
    "te": "Telugu",
    "th": "Thai",
    "tr": "Turkish",
    "tk": "Turkmen",
    "uk": "Ukrainian",
    "ur": "Urdu",
    "ug": "Uyghur",
    "uz": "Uzbek",
    "vi": "Vietnamese",
    "cy": "Welsh",
    "xh": "Xhosa",
    "yi": "Yiddish",
    "yo": "Yoruba",
    "zu": "Zulu",
}


class TranslationResult(NamedTuple):
    text: str
    source_language: str
    source_language_name: str
    target_language: str
    target_language_name: str


FLAG_EMOJI_TO_LANG: dict[str, str] = {
    "🇺🇸": "en",  # United States -> English
    "🇬🇧": "en",  # United Kingdom -> English
    "🇨🇦": "en",  # Canada (default) -> English
    "🇦🇺": "en",  # Australia -> English
    "🇮🇪": "en",  # Ireland -> English
    "🇪🇸": "es",  # Spain -> Spanish
    "🇲🇽": "es",  # Mexico -> Spanish
    "🇦🇷": "es",  # Argentina -> Spanish
    "🇫🇷": "fr",  # France -> French
    "🇨🇦_qc": "fr",  # Quebec (not standard but tolerated)
    "🇧🇪": "fr",  # Belgium -> French
    "🇨🇭": "fr",  # Switzerland (one of) -> French
    "🇩🇪": "de",  # Germany -> German
    "🇦🇹": "de",  # Austria -> German
    "🇮🇹": "it",  # Italy -> Italian
    "🇵🇹": "pt",  # Portugal -> Portuguese
    "🇧🇷": "pt",  # Brazil -> Portuguese
    "🇷🇺": "ru",  # Russia -> Russian
    "🇨🇳": "zh-cn",  # China -> Simplified Chinese
    "🇭🇰": "zh-tw",  # Hong Kong -> Traditional Chinese
    "🇹🇼": "zh-tw",  # Taiwan -> Traditional Chinese
    "🇯🇵": "ja",  # Japan -> Japanese
    "🇰🇷": "ko",  # South Korea -> Korean
    "🇸🇦": "ar",  # Saudi Arabia -> Arabic
    "🇪🇬": "ar",  # Egypt -> Arabic
    "🇦🇪": "ar",  # UAE -> Arabic
    "🇮🇳": "hi",  # India -> Hindi
    "🇹🇷": "tr",  # Turkey -> Turkish
    "🇳🇱": "nl",  # Netherlands -> Dutch
    "🇵🇱": "pl",  # Poland -> Polish
    "🇸🇪": "sv",  # Sweden -> Swedish
    "🇳🇴": "no",  # Norway -> Norwegian
    "🇩🇰": "da",  # Denmark -> Danish
    "🇫🇮": "fi",  # Finland -> Finnish
    "🇬🇷": "el",  # Greece -> Greek
    "🇮🇱": "he",  # Israel -> Hebrew
    "🇹🇭": "th",  # Thailand -> Thai
    "🇻🇳": "vi",  # Vietnam -> Vietnamese
    "🇮🇩": "id",  # Indonesia -> Indonesian
    "🇲🇾": "ms",  # Malaysia -> Malay
    "🇵🇭": "tl",  # Philippines -> Filipino/Tagalog
    "🇺🇦": "uk",  # Ukraine -> Ukrainian
    "🇨🇿": "cs",  # Czech Republic -> Czech
    "🇷🇴": "ro",  # Romania -> Romanian
    "🇭🇺": "hu",  # Hungary -> Hungarian
    "🇮🇸": "is",  # Iceland -> Icelandic
    "🇪🇪": "et",  # Estonia -> Estonian
    "🇱🇻": "lv",  # Latvia -> Latvian
    "🇱🇹": "lt",  # Lithuania -> Lithuanian
}


def get_language_name(code: str) -> str:
    normalized = str(code or "").strip().lower()
    return LANGUAGE_NAMES.get(normalized, normalized.upper() if normalized else "Unknown")


def get_lang_for_flag(emoji: str) -> str | None:
    """Resolve a country flag emoji to its ISO 639-1 language code, or None if not a known flag."""
    if not emoji:
        return None
    raw = str(emoji).strip()
    return FLAG_EMOJI_TO_LANG.get(raw)


def translate_text(
    text: str,
    target_lang: str = "en",
    source_lang: str = "auto",
    timeout: float = 8.0,
) -> TranslationResult:
    clean_text = str(text or "").strip()
    if not clean_text:
        raise ValueError("Cannot translate empty text.")

    safe_target = str(target_lang or "en").strip().lower() or "en"
    safe_source = str(source_lang or "auto").strip().lower() or "auto"

    encoded_query = urllib.parse.quote(clean_text)
    url = (
        f"https://translate.googleapis.com/translate_a/single"
        f"?client=gtx&sl={safe_source}&tl={safe_target}&dt=t&q={encoded_query}"
    )

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:  # nosec B310
            raw_data = response.read().decode("utf-8")
            data = json.loads(raw_data)
    except urllib.error.HTTPError as exc:
        logger.error("HTTP error translating text (%s): %s", exc.code, exc)
        raise RuntimeError(f"Translation service returned error code {exc.code}.") from exc
    except urllib.error.URLError as exc:
        logger.error("Connection error translating text: %s", exc)
        raise RuntimeError("Unable to reach translation service.") from exc
    except Exception as exc:
        logger.exception("Unexpected error during translation")
        raise RuntimeError(f"Translation failed: {exc}") from exc

    if not isinstance(data, list) or not data:
        raise RuntimeError("Unexpected response structure from translation service.")

    sentences = data[0] if isinstance(data[0], list) else []
    translated_parts = [
        str(segment[0])
        for segment in sentences
        if isinstance(segment, list) and len(segment) > 0 and segment[0]
    ]
    translated_text = "".join(translated_parts).strip()

    detected_code = str(data[2]).strip() if len(data) > 2 and data[2] else safe_source

    return TranslationResult(
        text=translated_text,
        source_language=detected_code,
        source_language_name=get_language_name(detected_code),
        target_language=safe_target,
        target_language_name=get_language_name(safe_target),
    )
