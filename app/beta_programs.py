from __future__ import annotations

import hashlib
import json
import re
import urllib.parse
from html import unescape

import requests
from bs4 import BeautifulSoup

BETA_PROGRAM_HEADING_PATTERN = re.compile(r"\bbeta testing products\b", re.IGNORECASE)
BETA_PROGRAM_STOP_HEADING_PATTERN = re.compile(r"\bregister to join\b", re.IGNORECASE)
BETA_PROGRAM_DEADLINE_PATTERN = re.compile(r"^Deadline:\s*(.+)$", re.IGNORECASE)


def _clean_text(value: str):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _clip_text(value: str, max_chars: int):
    text = str(value or "")
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3]}..."


def _normalize_http_url_setting(raw_value: str, fallback_value: str):
    value = str(raw_value or "").strip()
    candidate = value or str(fallback_value or "").strip()
    if not candidate:
        raise ValueError("URL is required.")
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    parsed = urllib.parse.urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("URL must use http:// or https:// and include a host.")
    normalized_path = parsed.path or "/"
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, normalized_path, "", parsed.query, ""))


def parse_beta_program_snapshot_json(raw_value) -> list[dict]:
    try:
        parsed = json.loads(str(raw_value or "[]"))
    except (TypeError, ValueError):
        return []
    if not isinstance(parsed, list):
        return []
    programs = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        program_id = str(item.get("program_id") or "").strip()
        title = _clip_text(str(item.get("title") or "").strip(), max_chars=200)
        if not program_id or not title:
            continue
        programs.append(
            {
                "program_id": program_id,
                "title": title,
                "summary": _clip_text(str(item.get("summary") or "").strip(), max_chars=400),
                "deadline": _clip_text(str(item.get("deadline") or "").strip(), max_chars=120),
                "apply_url": str(item.get("apply_url") or "").strip(),
            }
        )
    programs.sort(key=lambda item: (item["title"].casefold(), item["program_id"]))
    return programs


def serialize_beta_program_snapshot(programs: list[dict]) -> str:
    normalized = parse_beta_program_snapshot_json(json.dumps(programs))
    return json.dumps(normalized, separators=(",", ":"), sort_keys=True)


def _find_beta_program_section_heading(soup: BeautifulSoup):
    for tag in soup.find_all(re.compile(r"^h[1-6]$")):
        heading_text = _clean_text(tag.get_text(" ", strip=True))
        if BETA_PROGRAM_HEADING_PATTERN.search(heading_text):
            return tag
    return None


def _iter_beta_program_section_nodes(heading_tag):
    for node in heading_tag.next_elements:
        if node is heading_tag:
            continue
        if getattr(node, "name", None) and re.fullmatch(r"h[1-6]", str(node.name), flags=re.IGNORECASE):
            heading_text = _clean_text(node.get_text(" ", strip=True))
            if BETA_PROGRAM_STOP_HEADING_PATTERN.search(heading_text):
                break
        yield node


def _find_beta_program_card_container(link_tag):
    container_names = {"article", "section", "div", "li"}
    for candidate in (link_tag, *link_tag.parents):
        name = getattr(candidate, "name", None)
        if name not in container_names:
            continue
        apply_links = [
            link
            for link in candidate.find_all("a", href=True)
            if "apply" in _clean_text(link.get_text(" ", strip=True)).casefold()
        ]
        if len(apply_links) != 1:
            continue
        texts = [_clean_text(text) for text in candidate.stripped_strings]
        texts = [text for text in texts if text]
        if len(texts) >= 2:
            return candidate
    return link_tag.parent or link_tag


def _extract_beta_program_card_texts(container) -> list[str]:
    texts = []
    previous = None
    for raw_text in container.stripped_strings:
        cleaned = _clean_text(raw_text)
        if not cleaned or cleaned == previous:
            continue
        previous = cleaned
        texts.append(cleaned)
    return texts


def _extract_beta_programs_from_select_inputs(soup: BeautifulSoup, source_url: str):
    programs = []
    seen_program_ids = set()
    for select_tag in soup.find_all("select"):
        select_name = str(select_tag.get("name") or "")
        select_id = str(select_tag.get("id") or "")
        identity = f"{select_name} {select_id}".strip().casefold()
        if not identity:
            continue
        if "dropdown1" not in identity and "product" not in identity:
            continue
        for option_tag in select_tag.find_all("option"):
            raw_value = str(option_tag.get("value") or "").strip()
            title = _clean_text(option_tag.get_text(" ", strip=True))
            if not title or title.casefold() in {"select product", "product", "choose product"}:
                continue
            if not raw_value and not title:
                continue
            apply_url = urllib.parse.urljoin(source_url, raw_value) if raw_value else source_url
            program_id = hashlib.sha256(f"{title}|{apply_url}".encode()).hexdigest()[:24]
            if program_id in seen_program_ids:
                continue
            seen_program_ids.add(program_id)
            programs.append(
                {
                    "program_id": program_id,
                    "title": _clip_text(title, max_chars=200),
                    "summary": "",
                    "deadline": "",
                    "apply_url": apply_url,
                }
            )
    programs.sort(key=lambda item: (item["title"].casefold(), item["apply_url"]))
    return programs


def parse_beta_testing_programs(page_html: str, source_url: str):
    soup = BeautifulSoup(page_html, "html.parser")
    select_programs = _extract_beta_programs_from_select_inputs(soup, source_url)
    heading_tag = _find_beta_program_section_heading(soup)
    if heading_tag is None and select_programs:
        return select_programs
    if heading_tag is None:
        raise RuntimeError("Could not find the Beta Testing Products section on the GL.iNet beta page.")

    seen_program_ids = set()
    programs = []
    for node in _iter_beta_program_section_nodes(heading_tag):
        if getattr(node, "name", None) != "a":
            continue
        if not node.has_attr("href"):
            continue
        link_text = _clean_text(node.get_text(" ", strip=True))
        if "apply" not in link_text.casefold():
            continue
        apply_url = urllib.parse.urljoin(source_url, str(node.get("href") or "").strip())
        if not apply_url:
            continue
        container = _find_beta_program_card_container(node)
        texts = _extract_beta_program_card_texts(container)
        deadline = ""
        content_lines = []
        for text in texts:
            deadline_match = BETA_PROGRAM_DEADLINE_PATTERN.match(text)
            if deadline_match:
                deadline = _clip_text(deadline_match.group(1).strip(), max_chars=120)
                continue
            if text.casefold() == "apply here":
                continue
            content_lines.append(text)
        title = _clip_text(content_lines[0], max_chars=200) if content_lines else ""
        if not title or title.casefold() == _clean_text(heading_tag.get_text(" ", strip=True)).casefold():
            continue
        summary = _clip_text(content_lines[1], max_chars=400) if len(content_lines) > 1 else ""
        program_id = hashlib.sha256(f"{title}|{apply_url}".encode()).hexdigest()[:24]
        if program_id in seen_program_ids:
            continue
        seen_program_ids.add(program_id)
        programs.append(
            {
                "program_id": program_id,
                "title": title,
                "summary": summary,
                "deadline": deadline,
                "apply_url": apply_url,
            }
        )

    programs.sort(key=lambda item: (item["title"].casefold(), item["apply_url"]))
    if select_programs:
        existing_by_title = {item["title"].casefold(): item for item in programs}
        for select_program in select_programs:
            existing = existing_by_title.get(select_program["title"].casefold())
            if existing is None:
                programs.append(select_program)
                continue
            if not existing.get("apply_url"):
                existing["apply_url"] = select_program["apply_url"]
    return programs


def fetch_beta_testing_programs(
    source_url: str = "",
    *,
    fallback_url: str,
    request_timeout_seconds: int,
    request_user_agent: str,
):
    normalized_url = _normalize_http_url_setting(source_url, fallback_url)
    response = requests.get(
        normalized_url,
        timeout=request_timeout_seconds,
        headers={
            "User-Agent": request_user_agent,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
        },
    )
    if response.status_code >= 400:
        raise RuntimeError(f"GL.iNet beta page returned HTTP {response.status_code}.")
    final_url = str(response.url or "")
    final_host = (urllib.parse.urlparse(final_url).netloc or "").lower()
    if final_host.startswith("www."):
        final_host = final_host[4:]
    if final_host and final_host != "gl-inet.com":
        raise RuntimeError("GL.iNet beta page redirected to an unexpected host.")
    programs = parse_beta_testing_programs(response.text, final_url or normalized_url)
    return {
        "source_url": final_url or normalized_url,
        "source_name": "GL.iNet Beta Programs",
        "programs": programs,
    }
