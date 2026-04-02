#!/usr/bin/env python3
"""Refresh the Wenjie sentiment dataset from real source pages.

The input file is treated as a curated seed registry: editors keep the event
classification and dashboard-specific fields, while this script refreshes live
metadata from every direct source URL before writing the output dataset.

Current live refresh scope:
1. request every seeded source page through curl
2. resolve the effective final URL
3. extract page title and published time from source-specific metadata
4. keep manual sentiment / risk / action fields unchanged
5. validate the final output schema for the dashboard frontend

This keeps the deployed static dashboard stable today and leaves room for
future source discovery without changing the output schema again.
"""

from __future__ import annotations

import argparse
import copy
import html
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
FETCH_SENTINEL = "__CODEX_FETCH_META__"
FETCH_TIMEOUT_SECONDS = 25
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
)

REQUIRED_FIELDS = [
    "id",
    "publishedAt",
    "capturedAt",
    "entity",
    "model",
    "title",
    "summary",
    "category",
    "sentiment",
    "riskLevel",
    "riskReason",
    "sourceType",
    "sourceTier",
    "sourceName",
    "publisher",
    "url",
    "keywords",
    "impactScope",
    "status",
    "traceability",
    "suggestedAction",
]
ALLOWED_ENTITIES = {"brand", "M5", "M6", "M7", "M8", "M9"}
ALLOWED_SENTIMENTS = {"positive", "negative", "mixed", "neutral"}
ALLOWED_RISK_LEVELS = {"high", "medium", "low"}
ALLOWED_SOURCE_TYPES = {"official", "media"}

FETCH_CACHE: dict[str, "FetchedPage"] = {}


@dataclass
class ValidationIssue:
    level: str
    message: str
    event_id: str | None = None


@dataclass
class FetchedPage:
    requested_url: str
    final_url: str
    http_code: int
    content_type: str
    body: str


@dataclass(frozen=True)
class DateCandidate:
    iso: str
    precision: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input seed json path")
    parser.add_argument("--output", required=True, help="Output events json path")
    parser.add_argument(
        "--touch-updated-at",
        action="store_true",
        help="Refresh meta.updatedAt to current Shanghai time",
    )
    parser.add_argument(
        "--check-urls",
        action="store_true",
        help="Fail validation on broken URLs when --strict is enabled",
    )
    parser.add_argument(
        "--prefer-live-title",
        action="store_true",
        help="Replace manual event.title with the fetched page title when available",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with status 1 when warnings or errors are found",
    )
    return parser.parse_args()


def now_shanghai_iso() -> str:
    return datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds")


def load_dataset(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def collect_events(dataset: dict[str, Any], prefer_live_title: bool) -> list[dict[str, Any]]:
    events = []
    changed_count = 0

    for seed_event in dataset.get("events", []):
        refreshed, changes = refresh_event(seed_event, prefer_live_title=prefer_live_title)
        if changes:
            changed_count += 1
            change_list = ", ".join(sorted(changes))
            print(f"REFRESH [{seed_event.get('id', 'unknown')}]: {change_list}")
        events.append(refreshed)

    print(f"Refreshed {len(events)} events from live source pages; {changed_count} changed.")
    return events


def refresh_event(event: dict[str, Any], prefer_live_title: bool) -> tuple[dict[str, Any], set[str]]:
    refreshed = copy.deepcopy(event)
    changes: set[str] = set()
    url = refreshed.get("url", "")

    if not url:
        return refreshed, changes

    try:
        page = fetch_page(url)
    except RuntimeError as error:
        print(f"WARN [{event.get('id', 'unknown')}]: {error}")
        return refreshed, changes

    if page.final_url and not urls_equivalent(page.final_url, url):
        refreshed["url"] = page.final_url
        changes.add("url")

    live_title = extract_live_title(page)
    if prefer_live_title and live_title and live_title != refreshed.get("title"):
        refreshed["title"] = live_title
        changes.add("title")

    live_published = extract_published_at(page)
    chosen_published = choose_published_at(refreshed.get("publishedAt"), live_published)
    if chosen_published and chosen_published != refreshed.get("publishedAt"):
        refreshed["publishedAt"] = chosen_published
        changes.add("publishedAt")

    if changes or not refreshed.get("capturedAt"):
        refreshed["capturedAt"] = now_shanghai_iso()
        changes.add("capturedAt")

    return refreshed, changes


def normalize_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []

    for event in events:
        normalized.append({key: event.get(key) for key in REQUIRED_FIELDS})

    normalized.sort(key=sort_key, reverse=True)
    return normalized


def sort_key(event: dict[str, Any]) -> datetime:
    effective = event.get("publishedAt") or event.get("capturedAt")
    return parse_datetime(effective) or datetime.min.replace(tzinfo=SHANGHAI_TZ)


def parse_datetime(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def fetch_page(url: str) -> FetchedPage:
    cached = FETCH_CACHE.get(url)
    if cached:
        return cached

    command = [
        "curl",
        "-L",
        "--silent",
        "--show-error",
        "--compressed",
        "--retry",
        "2",
        "--retry-delay",
        "1",
        "--max-time",
        str(FETCH_TIMEOUT_SECONDS),
        "--user-agent",
        USER_AGENT,
        "--output",
        "-",
        "--write-out",
        f"\n{FETCH_SENTINEL}%{{http_code}}\t%{{url_effective}}\t%{{content_type}}",
        url,
    ]
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )

    if result.returncode != 0:
        raise RuntimeError(f"url 请求失败：{result.stderr.strip() or 'curl exited with non-zero status'}")

    if FETCH_SENTINEL not in result.stdout:
        raise RuntimeError("url 请求失败：无法解析 curl 返回元数据")

    body, metadata = result.stdout.rsplit(f"\n{FETCH_SENTINEL}", 1)
    parts = metadata.strip().split("\t")
    if len(parts) != 3:
        raise RuntimeError("url 请求失败：curl 元数据格式异常")

    http_code_str, final_url, content_type = parts

    try:
        http_code = int(http_code_str)
    except ValueError as error:
        raise RuntimeError("url 请求失败：无法解析 HTTP 状态码") from error

    page = FetchedPage(
        requested_url=url,
        final_url=final_url or url,
        http_code=http_code,
        content_type=content_type or "",
        body=body,
    )
    FETCH_CACHE[url] = page
    return page


def extract_live_title(page: FetchedPage) -> str | None:
    host = hostname_for(page.final_url)
    html_text = page.body

    if host == "digitalpower.huawei.com":
        title = first_clean_match(
            html_text,
            [r'"headline"\s*:\s*"([^"]+)"', r"<title[^>]*>(.*?)</title>"],
        )
        if title:
            return clean_title(title)

    title = first_clean_match(
        html_text,
        [
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+name=["\']title["\'][^>]+content=["\'](.*?)["\']',
            r"<title[^>]*>(.*?)</title>",
            r"<h1[^>]*>(.*?)</h1>",
        ],
    )
    if not title:
        return None
    return clean_title(title)


def extract_published_at(page: FetchedPage) -> DateCandidate | None:
    host = hostname_for(page.final_url)
    html_text = page.body

    domain_specific = {
        "www.huawei.com": extract_huawei_date,
        "digitalpower.huawei.com": extract_digitalpower_date,
        "aito.auto": extract_aito_date,
        "www.news.cn": extract_news_cn_date,
        "www.bbtnews.com.cn": extract_bbtnews_date,
        "www.pcauto.com.cn": extract_pcauto_date,
    }

    if "ithome.com" in host:
        candidate = extract_ithome_date(html_text)
        if candidate:
            return candidate

    if "sina.com.cn" in host:
        candidate = extract_sina_date(html_text)
        if candidate:
            return candidate

    extractor = domain_specific.get(host)
    if extractor:
        candidate = extractor(html_text)
        if candidate:
            return candidate

    return extract_generic_date(html_text)


def extract_huawei_date(html_text: str) -> DateCandidate | None:
    raw = first_clean_match(
        html_text,
        [r'<input[^>]+id=["\']releaseFormatTime["\'][^>]+value=["\'](.*?)["\']'],
    )
    return parse_date_candidate(raw)


def extract_digitalpower_date(html_text: str) -> DateCandidate | None:
    raw = first_clean_match(
        html_text,
        [r'"datePublished"\s*:\s*"([^"]+)"', r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\'](.*?)["\']'],
    )
    return parse_date_candidate(raw)


def extract_aito_date(html_text: str) -> DateCandidate | None:
    raw = first_clean_match(
        html_text,
        [r"时间[:：]\s*([0-9/\-:\s]+)", r"发布时间[:：]\s*([0-9/\-:\s]+)"],
    )
    return parse_date_candidate(raw)


def extract_news_cn_date(html_text: str) -> DateCandidate | None:
    special = re.search(r"(\d{4})\s+(\d{2})/(\d{2})\s+(\d{2}:\d{2}:\d{2})", html_text)
    if special:
        raw = f"{special.group(1)}-{special.group(2)}-{special.group(3)} {special.group(4)}"
        candidate = parse_date_candidate(raw)
        if candidate:
            return candidate

    visible = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", html_text)
    if visible:
        candidate = parse_date_candidate(visible.group(1))
        if candidate:
            return candidate

    raw = first_clean_match(
        html_text,
        [r'<meta[^>]+name=["\']publishdate["\'][^>]+content=["\'](.*?)["\']'],
    )
    return parse_date_candidate(raw)


def extract_bbtnews_date(html_text: str) -> DateCandidate | None:
    snippet = html_text[:4000]
    raw = first_clean_match(snippet, [r"<span>\s*(\d{4}-\d{2}-\d{2})\s*</span>"])
    return parse_date_candidate(raw)


def extract_ithome_date(html_text: str) -> DateCandidate | None:
    raw = first_clean_match(
        html_text,
        [r'<span[^>]+id=["\']pubtime_baidu["\'][^>]*>(.*?)</span>'],
    )
    return parse_date_candidate(raw)


def extract_sina_date(html_text: str) -> DateCandidate | None:
    raw = first_clean_match(
        html_text,
        [
            r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+name=["\']weibo:\s*article:create_at["\'][^>]+content=["\'](.*?)["\']',
            r'<span[^>]+class=["\']date["\'][^>]*>(.*?)</span>',
        ],
    )
    return parse_date_candidate(raw)


def extract_pcauto_date(html_text: str) -> DateCandidate | None:
    raw = first_clean_match(
        html_text,
        [
            r'"pubDate"\s*:\s*"([^"]+)"',
            r'<span[^>]+id=["\']pubtime_baidu["\'][^>]*>(.*?)</span>',
        ],
    )
    return parse_date_candidate(raw)


def extract_generic_date(html_text: str) -> DateCandidate | None:
    raw = first_clean_match(
        html_text,
        [
            r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+name=["\']publishdate["\'][^>]+content=["\'](.*?)["\']',
            r'"datePublished"\s*:\s*"([^"]+)"',
            r'<span[^>]+id=["\']pubtime_baidu["\'][^>]*>(.*?)</span>',
            r"时间[:：]\s*([0-9/\-:\s]+)",
            r"发布时间[:：]\s*([0-9/\-:\s]+)",
        ],
    )
    return parse_date_candidate(raw)


def first_clean_match(text: str, patterns: list[str]) -> str | None:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return clean_html_text(match.group(1))
    return None


def clean_html_text(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned = re.sub(r"<[^>]+>", " ", value)
    cleaned = html.unescape(cleaned)
    cleaned = cleaned.replace("\u3000", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def clean_title(title: str) -> str:
    cleaned = clean_html_text(title) or title
    suffix_patterns = [
        r"\s*-\s*IT之家\s*$",
        r"\s*_\s*北京商报\s*$",
        r"\s*_\s*太平洋汽车\s*$",
        r"\s*-\s*华为数字能源\s*$",
        r"\s*-\s*华为\s*$",
        r"\s*_\s*新浪.*$",
    ]
    for pattern in suffix_patterns:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip()
    return re.sub(r"\s+", " ", cleaned).strip()


def parse_date_candidate(raw: str | None) -> DateCandidate | None:
    if not raw:
        return None

    cleaned = clean_html_text(raw)
    if not cleaned:
        return None

    cleaned = cleaned.replace("上午", " ")
    cleaned = cleaned.replace("下午", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    special = re.match(r"^(\d{4})\s+(\d{2})/(\d{2})\s+(\d{2}:\d{2}:\d{2})$", cleaned)
    if special:
        cleaned = f"{special.group(1)}-{special.group(2)}-{special.group(3)} {special.group(4)}"

    normalized = (
        cleaned.replace("年", "-")
        .replace("月", "-")
        .replace("日", "")
        .replace("/", "-")
        .replace("T", "T")
    )
    normalized = re.sub(r"\s+", " ", normalized).strip()

    candidates = [cleaned, normalized]
    formats = [
        ("%Y-%m-%dT%H:%M:%S%z", 3),
        ("%Y-%m-%d %H:%M:%S%z", 3),
        ("%Y-%m-%dT%H:%M:%S", 3),
        ("%Y-%m-%d %H:%M:%S", 3),
        ("%Y-%m-%d %H:%M", 2),
        ("%Y-%m-%d", 1),
    ]

    for value in candidates:
        for fmt, precision in formats:
            try:
                parsed = datetime.strptime(value, fmt)
            except ValueError:
                continue

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
            else:
                parsed = parsed.astimezone(SHANGHAI_TZ)
            return DateCandidate(parsed.isoformat(timespec="seconds"), precision)

    return None


def choose_published_at(existing: str | None, live: DateCandidate | None) -> str | None:
    if not live:
        return existing
    if not existing:
        return live.iso

    existing_dt = parse_datetime(existing)
    if existing_dt is None:
        return live.iso

    live_dt = parse_datetime(live.iso)
    if (
        live_dt is not None
        and live_dt.hour == 0
        and live_dt.minute == 0
        and live_dt.second == 0
        and existing_dt.date() == live_dt.date()
        and (existing_dt.hour, existing_dt.minute, existing_dt.second) != (0, 0, 0)
    ):
        return existing

    existing_precision = precision_from_existing(existing)
    if live.iso == existing:
        return existing

    if live.precision > existing_precision:
        return live.iso

    if live.precision == existing_precision:
        return live.iso

    return existing


def precision_from_existing(value: str) -> int:
    if re.search(r"T\d{2}:\d{2}:\d{2}", value):
        return 3
    if re.search(r"T\d{2}:\d{2}", value):
        return 2
    return 1


def hostname_for(url: str) -> str:
    return urlparse(url).netloc.lower()


def urls_equivalent(left: str, right: str) -> bool:
    return normalize_url_for_compare(left) == normalize_url_for_compare(right)


def normalize_url_for_compare(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    return parsed._replace(netloc=parsed.netloc.lower(), path=path, fragment="").geturl()


def validate_dataset(dataset: dict[str, Any], check_urls: bool) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    events = dataset.get("events", [])

    meta = dataset.get("meta", {})
    for key in ("title", "updatedAt", "updateFrequency", "sourcePolicy", "scope", "version"):
        if not meta.get(key):
            issues.append(ValidationIssue("error", f"meta.{key} 缺失"))

    seen_ids: set[str] = set()
    for event in events:
        event_id = event.get("id")

        if event_id in seen_ids:
            issues.append(ValidationIssue("error", "事件 id 重复", event_id))
        else:
            seen_ids.add(event_id)

        for field in REQUIRED_FIELDS:
            value = event.get(field)
            if value in (None, "") and field not in {"publishedAt", "capturedAt"}:
                issues.append(ValidationIssue("error", f"{field} 缺失", event_id))

        if not event.get("publishedAt") and not event.get("capturedAt"):
            issues.append(ValidationIssue("error", "publishedAt / capturedAt 至少要保留一个", event_id))

        if event.get("entity") not in ALLOWED_ENTITIES:
            issues.append(ValidationIssue("error", f"entity 不在允许范围：{event.get('entity')}", event_id))

        if event.get("sentiment") not in ALLOWED_SENTIMENTS:
            issues.append(ValidationIssue("error", f"sentiment 不在允许范围：{event.get('sentiment')}", event_id))

        if event.get("riskLevel") not in ALLOWED_RISK_LEVELS:
            issues.append(ValidationIssue("error", f"riskLevel 不在允许范围：{event.get('riskLevel')}", event_id))

        if event.get("sourceType") not in ALLOWED_SOURCE_TYPES:
            issues.append(ValidationIssue("error", f"sourceType 不在允许范围：{event.get('sourceType')}", event_id))

        if not isinstance(event.get("keywords"), list):
            issues.append(ValidationIssue("error", "keywords 必须是数组", event_id))

        weak_url_reason = detect_weak_url(event.get("url", ""))
        if weak_url_reason:
            issues.append(ValidationIssue("warning", weak_url_reason, event_id))

        if parse_datetime(event.get("publishedAt")) is None and event.get("publishedAt"):
            issues.append(ValidationIssue("warning", "publishedAt 不是 ISO 时间", event_id))

        if parse_datetime(event.get("capturedAt")) is None and event.get("capturedAt"):
            issues.append(ValidationIssue("warning", "capturedAt 不是 ISO 时间", event_id))

        if check_urls:
            status_issue = check_url_status(event.get("url", ""))
            if status_issue:
                issues.append(ValidationIssue("warning", status_issue, event_id))

    return issues


def detect_weak_url(url: str) -> str | None:
    if not url:
        return "url 缺失"

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "url 不是有效的 http/https 地址"

    if parsed.path in {"", "/"}:
        return "url 指向首页或根路径，疑似弱链接"

    lowered_path = parsed.path.lower()
    lowered_query = parsed.query.lower()
    blocked_path_tokens = ["/search", "/s/", "/channel/", "/channels/", "/list/", "/tag/", "/tags/"]
    blocked_query_keys = {"q", "query", "keyword", "keywords", "wd", "search"}

    if any(token in lowered_path for token in blocked_path_tokens):
        return "url 疑似搜索页、栏目页或标签页"

    query_keys = {key.lower() for key in parse_qs(lowered_query).keys()}
    if blocked_query_keys.intersection(query_keys):
        return "url 疑似搜索结果页"

    return None


def check_url_status(url: str) -> str | None:
    try:
        page = fetch_page(url)
    except RuntimeError as error:
        return str(error)

    if page.http_code != 200:
        return f"url 请求返回 {page.http_code}"

    if "html" not in page.content_type and not page.body.strip():
        return f"url 返回内容异常：content-type={page.content_type or 'unknown'}"

    return None


def write_dataset(path: Path, dataset: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dataset, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def print_issues(issues: list[ValidationIssue]) -> None:
    if not issues:
        print("No validation issues found.")
        return

    for issue in issues:
        prefix = issue.level.upper()
        suffix = f" [{issue.event_id}]" if issue.event_id else ""
        print(f"{prefix}{suffix}: {issue.message}")


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    dataset = load_dataset(input_path)
    dataset["events"] = normalize_events(collect_events(dataset, prefer_live_title=args.prefer_live_title))

    if args.touch_updated_at:
        dataset.setdefault("meta", {})["updatedAt"] = now_shanghai_iso()

    issues = validate_dataset(dataset, check_urls=args.check_urls)
    print_issues(issues)
    write_dataset(output_path, dataset)

    if args.strict and issues:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
