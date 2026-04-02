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
import hashlib
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


@dataclass(frozen=True)
class DiscoveryCandidate:
    url: str
    title: str
    summary: str
    published_at: str | None
    source_type: str
    source_tier: str
    source_name: str
    publisher: str
    discovery_source: str


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
        "--discover",
        action="store_true",
        help="Discover new candidate events from configured official/media sources before refresh",
    )
    parser.add_argument(
        "--sync-input",
        action="store_true",
        help="Write refreshed seed events back to the input file after discovery",
    )
    parser.add_argument(
        "--discover-days",
        type=int,
        default=45,
        help="Only keep newly discovered events whose publishedAt is within this many days when available",
    )
    parser.add_argument(
        "--discover-max-new",
        type=int,
        default=10,
        help="Maximum number of newly discovered events to append in one run",
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


def discover_events(
    dataset: dict[str, Any],
    discover_days: int,
    discover_max_new: int,
) -> tuple[dict[str, Any], int]:
    existing_urls = {
        normalize_url_for_compare(event.get("url", ""))
        for event in dataset.get("events", [])
        if event.get("url")
    }
    discovered_candidates = discover_candidates()
    scored_candidates: list[tuple[int, datetime, DiscoveryCandidate]] = []

    for candidate in discovered_candidates:
        normalized_url = normalize_url_for_compare(candidate.url)
        if normalized_url in existing_urls:
            continue

        candidate_dt = parse_datetime(candidate.published_at) if candidate.published_at else None
        if candidate_dt and age_in_days(candidate_dt) > discover_days:
            continue

        scored_candidates.append((source_priority(candidate), candidate_dt or datetime.min.replace(tzinfo=SHANGHAI_TZ), candidate))

    scored_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)

    new_events = []
    existing_ids = {event.get("id", "") for event in dataset.get("events", [])}
    for _, _, candidate in scored_candidates:
        if len(new_events) >= discover_max_new:
            break

        seed_event = build_seed_event_from_candidate(candidate, existing_ids)
        refreshed_event, _ = refresh_event(seed_event, prefer_live_title=True)
        published_dt = parse_datetime(refreshed_event.get("publishedAt"))
        if published_dt is None:
            continue
        if published_dt and age_in_days(published_dt) > discover_days:
            continue

        normalized_url = normalize_url_for_compare(refreshed_event["url"])
        if normalized_url in existing_urls:
            continue

        existing_ids.add(refreshed_event["id"])
        existing_urls.add(normalized_url)
        new_events.append(refreshed_event)
        print(f"DISCOVER [{refreshed_event['id']}]: {refreshed_event['title']}")

    if not new_events:
        return dataset, 0

    next_dataset = copy.deepcopy(dataset)
    next_dataset["events"] = list(dataset.get("events", [])) + new_events
    return next_dataset, len(new_events)


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


def discover_candidates() -> list[DiscoveryCandidate]:
    candidates: list[DiscoveryCandidate] = []
    candidates.extend(discover_from_huawei_search())
    candidates.extend(discover_from_xinhua_auto())
    candidates.extend(discover_from_ithome_tags())
    candidates.extend(discover_from_aito_sitemap())
    return dedupe_candidates(candidates)


def discover_from_huawei_search() -> list[DiscoveryCandidate]:
    website_id = "36eacc0c64c54804827c3f4922e87328"
    keywords = ["问界", "赛力斯", "AITO", "鸿蒙智行", "引望"]
    candidates: list[DiscoveryCandidate] = []

    for keyword in keywords:
        payload = {
            "websiteId": website_id,
            "searchTxt": keyword,
            "pageNum": 1,
            "pageSize": 20,
            "customParam": {"site": "cn", "type": "news"},
        }
        try:
            response = post_json_via_curl(
                "https://www.huawei.com/hwp_ai_isearch_service/msc/hwp_ai_isearch_search_public?searchService=www-search",
                payload,
            )
        except RuntimeError as error:
            print(f"WARN [discover-huawei-search:{keyword}]: {error}")
            continue

        for item in response.get("data", {}).get("data", []):
            title = clean_title(item.get("title", ""))
            summary = clean_html_text(item.get("description")) or ""
            if not is_relevant_text(f"{title} {summary}"):
                continue

            host_name = item.get("hostName", "")
            source_meta = map_huawei_host(host_name)
            if not source_meta:
                continue

            raw_url = item.get("url", "")
            if not raw_url:
                continue

            url = raw_url if raw_url.startswith("http") else f"https://{raw_url}"
            candidates.append(
                DiscoveryCandidate(
                    url=url,
                    title=title,
                    summary=summary,
                    published_at=parse_date_candidate(item.get("releaseFormatTime")).iso if item.get("releaseFormatTime") else None,
                    source_type="official",
                    source_tier=source_meta["source_tier"],
                    source_name=source_meta["source_name"],
                    publisher=source_meta["publisher"],
                    discovery_source=f"华为官网搜索接口 / 关键词 {keyword}",
                )
            )

    return candidates


def discover_from_xinhua_auto() -> list[DiscoveryCandidate]:
    try:
        page = fetch_page("https://www.news.cn/auto/")
    except RuntimeError as error:
        print(f"WARN [discover-xinhua]: {error}")
        return []

    candidates: list[DiscoveryCandidate] = []
    pattern = re.compile(
        r'<div class="item item-style1">.*?<a href="(?P<href>[^"]+/c\.html)".*?<div class="tit"><a [^>]*>(?P<title>.*?)</a></div>.*?<div class="time">(?P<date>\d{4}-\d{2}-\d{2})</div>',
        flags=re.DOTALL,
    )
    for match in pattern.finditer(page.body):
        title = clean_html_text(match.group("title")) or ""
        if not is_relevant_text(title):
            continue

        href = match.group("href")
        url = href if href.startswith("http") else f"https://www.news.cn{href}"
        candidates.append(
            DiscoveryCandidate(
                url=url,
                title=title,
                summary="",
                published_at=parse_date_candidate(match.group("date")).iso if match.group("date") else None,
                source_type="media",
                source_tier="权威媒体",
                source_name="新华网",
                publisher="新华网",
                discovery_source="新华汽车列表页",
            )
        )

    return candidates


def discover_from_ithome_tags() -> list[DiscoveryCandidate]:
    sources = [
        ("https://www.ithome.com/tags/AITO/", "IT之家 AITO 标签页"),
        ("https://www.ithome.com/tags/%E9%97%AE%E7%95%8C/", "IT之家 问界 标签页"),
    ]
    candidates: list[DiscoveryCandidate] = []
    pattern = re.compile(
        r'<div class="c" data-ot="(?P<date>[^"]+)">.*?<a title="(?P<title>[^"]+)" target="_blank" href="(?P<href>https://www\.ithome\.com/0/\d+/\d+\.htm)" class="title">.*?</a>.*?<div class="m">(?P<summary>.*?)</div>',
        flags=re.DOTALL,
    )

    for url, label in sources:
        try:
            page = fetch_page(url)
        except RuntimeError as error:
            print(f"WARN [discover-ithome:{label}]: {error}")
            continue

        for match in pattern.finditer(page.body):
            title = clean_html_text(match.group("title")) or ""
            summary = clean_html_text(match.group("summary")) or ""
            if not is_relevant_text(f"{title} {summary}"):
                continue
            candidates.append(
                DiscoveryCandidate(
                    url=match.group("href"),
                    title=title,
                    summary=summary,
                    published_at=parse_date_candidate(match.group("date")).iso if match.group("date") else None,
                    source_type="media",
                    source_tier="行业媒体",
                    source_name="IT之家",
                    publisher="IT之家",
                    discovery_source=label,
                )
            )

    return candidates


def discover_from_aito_sitemap() -> list[DiscoveryCandidate]:
    try:
        xml_text = fetch_text("https://aito.auto/sitemap.xml")
    except RuntimeError as error:
        print(f"WARN [discover-aito-sitemap]: {error}")
        return []

    candidates: list[DiscoveryCandidate] = []
    pattern = re.compile(
        r"<url>\s*<loc>(?P<loc>https://aito\.auto/news/[^<]+)</loc>\s*<lastmod>(?P<lastmod>\d{4}-\d{2}-\d{2})</lastmod>",
        flags=re.DOTALL,
    )
    for match in pattern.finditer(xml_text):
        url = match.group("loc")
        if detect_weak_url(url):
            continue
        if not re.search(r"/news/.*(m5|m6|m7|m8|m9|aito|harmonyos|cockpit)", url, flags=re.IGNORECASE):
            continue
        title = title_from_url_slug(url)
        candidates.append(
            DiscoveryCandidate(
                url=url,
                title=title,
                summary="",
                published_at=parse_date_candidate(match.group("lastmod")).iso if match.group("lastmod") else None,
                source_type="official",
                source_tier="品牌官方",
                source_name="AITO 官网",
                publisher="AITO",
                discovery_source="AITO sitemap",
            )
        )
    return candidates


def dedupe_candidates(candidates: list[DiscoveryCandidate]) -> list[DiscoveryCandidate]:
    by_url: dict[str, DiscoveryCandidate] = {}
    for candidate in candidates:
        key = normalize_url_for_compare(candidate.url)
        current = by_url.get(key)
        if current is None or source_priority(candidate) > source_priority(current):
            by_url[key] = candidate
    return list(by_url.values())


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

    live_summary = extract_live_summary(page)
    if (
        refreshed.get("status") == "pending-review"
        and live_summary
        and live_summary != refreshed.get("summary")
    ):
        refreshed["summary"] = live_summary
        changes.add("summary")

    live_published = extract_published_at(page)
    chosen_published = choose_published_at(refreshed.get("publishedAt"), live_published)
    if chosen_published and chosen_published != refreshed.get("publishedAt"):
        refreshed["publishedAt"] = chosen_published
        changes.add("publishedAt")

    if changes or not refreshed.get("capturedAt"):
        refreshed["capturedAt"] = now_shanghai_iso()
        changes.add("capturedAt")

    return refreshed, changes


def build_seed_event_from_candidate(candidate: DiscoveryCandidate, existing_ids: set[str]) -> dict[str, Any]:
    entity, model = infer_entity_and_model(candidate.title, candidate.summary)
    category = infer_category(candidate.title, candidate.summary)
    sentiment = infer_sentiment(candidate.title, candidate.summary)
    risk_level = infer_risk_level(candidate.title, candidate.summary)
    published_at = candidate.published_at
    event_id = build_event_id(entity, published_at, candidate.url, existing_ids)

    return {
        "id": event_id,
        "publishedAt": published_at,
        "capturedAt": now_shanghai_iso(),
        "entity": entity,
        "model": model,
        "title": candidate.title,
        "summary": candidate.summary or f"{candidate.source_name}出现与{model}相关的新增事件，建议人工补充摘要。",
        "category": category,
        "sentiment": sentiment,
        "riskLevel": risk_level,
        "riskReason": infer_risk_reason(risk_level, candidate),
        "sourceType": candidate.source_type,
        "sourceTier": candidate.source_tier,
        "sourceName": candidate.source_name,
        "publisher": candidate.publisher,
        "url": candidate.url,
        "keywords": infer_keywords(candidate.title, candidate.summary, entity),
        "impactScope": infer_impact_scope(category, sentiment),
        "status": "pending-review",
        "traceability": f"自动发现：{candidate.discovery_source} -> 直达原始正文页；建议人工复核标签、摘要与风险等级。",
        "suggestedAction": "建议人工复核事件分类、情绪方向与风险等级，如为持续议题可补充后续跟进稿或官方回应。",
    }


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


def fetch_text(url: str) -> str:
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
        raise RuntimeError(result.stderr.strip() or "curl exited with non-zero status")
    return result.stdout


def post_json_via_curl(url: str, payload: dict[str, Any]) -> dict[str, Any]:
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
        "-H",
        "Content-Type: application/json",
        "-X",
        "POST",
        "--data",
        json.dumps(payload, ensure_ascii=False),
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
        raise RuntimeError(result.stderr.strip() or "curl exited with non-zero status")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("JSON 响应解析失败") from error


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


def extract_live_summary(page: FetchedPage) -> str | None:
    summary = first_clean_match(
        page.body,
        [
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
        ],
    )
    return clean_html_text(summary)


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


def infer_entity_and_model(title: str, summary: str) -> tuple[str, str]:
    text = normalize_relevance_text(f"{title} {summary}")
    rules = [
        (r"问界\s*新?\s*M5|M5 Ultra|问界M5|问界 M5", ("M5", "问界 M5")),
        (r"问界\s*M6|问界M6", ("M6", "问界 M6")),
        (r"问界\s*新?\s*M7|问界M7|问界 M7", ("M7", "问界 M7")),
        (r"问界\s*M8|问界M8", ("M8", "问界 M8")),
        (r"问界\s*M9|问界M9", ("M9", "问界 M9")),
    ]
    for pattern, value in rules:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return value
    return "brand", "问界品牌"


def infer_category(title: str, summary: str) -> str:
    text = normalize_relevance_text(f"{title} {summary}")
    mapping = [
        (r"争议|维权|投诉|事故|召回|老款|问题", "争议事件"),
        (r"官图|内饰|配色|续航|曝光", "新品动态"),
        (r"交付|下线|到店", "交付跟踪"),
        (r"订单|大定|预订|预售", "订单跟踪"),
        (r"上市|发布|官图|亮相", "新品动态"),
        (r"OTA|升级|雷达|鸿蒙座舱|智驾", "功能升级"),
        (r"保值率|测评|得分|榜首|冠军", "口碑背书"),
        (r"合作|签署|战略|联盟|生态", "合作动态"),
        (r"服务|用户中心|救援|权益", "服务权益"),
        (r"海外|阿联酋|出海", "海外市场"),
    ]
    for pattern, category in mapping:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return category
    return "动态跟踪"


def infer_sentiment(title: str, summary: str) -> str:
    text = normalize_relevance_text(f"{title} {summary}")
    if re.search(r"争议|维权|投诉|事故|召回|老款|问题", text, flags=re.IGNORECASE):
        return "negative"
    if re.search(r"升级|收费|观望|传闻|待确认", text, flags=re.IGNORECASE):
        return "mixed"
    if re.search(r"交付|订单|发布|上市|官图|联盟|冠军|榜首|得分|到店|出海|OTA", text, flags=re.IGNORECASE):
        return "positive"
    return "neutral"


def infer_risk_level(title: str, summary: str) -> str:
    text = normalize_relevance_text(f"{title} {summary}")
    if re.search(r"争议|维权|投诉|事故|召回", text, flags=re.IGNORECASE):
        return "high"
    if re.search(r"升级|收费|传闻|观望|预订|预售|订单|老款|问题", text, flags=re.IGNORECASE):
        return "medium"
    return "low"


def infer_risk_reason(risk_level: str, candidate: DiscoveryCandidate) -> str:
    base = {
        "high": "标题或摘要命中争议 / 事故 / 投诉类关键词，自动归类为高风险待复核事件。",
        "medium": "标题或摘要命中升级、预售、订单、收费或传闻类关键词，自动归类为中风险待复核事件。",
        "low": "当前内容更偏新品、交付、合作或口碑背书，自动归类为低风险待复核事件。",
    }[risk_level]
    return f"{base} 来源主体为{candidate.publisher}，建议人工确认最终定性。"


def infer_keywords(title: str, summary: str, entity: str) -> list[str]:
    text = normalize_relevance_text(f"{title} {summary}")
    candidates = [
        "问界", "AITO", "鸿蒙智行", "赛力斯", "华为", "引望",
        "M5", "M6", "M7", "M8", "M9",
        "订单", "交付", "上市", "预订", "OTA", "升级", "保值率", "测评", "服务", "权益", "出海",
    ]
    found = [item for item in candidates if re.search(re.escape(item), text, flags=re.IGNORECASE)]
    if entity != "brand" and entity not in found:
        found.insert(0, entity)
    return found[:8] if found else ["问界", "新增事件"]


def infer_impact_scope(category: str, sentiment: str) -> str:
    mapping = {
        "争议事件": "车主情绪、社媒讨论、订单稳定性",
        "交付跟踪": "用户信心、交付预期、转化效率",
        "订单跟踪": "传播热度、潜客线索、销量预期",
        "新品动态": "新品关注度、舆论热度、潜客决策",
        "功能升级": "老车主预期、体验口碑、技术讨论",
        "口碑背书": "高端心智、用户信任、媒体引用",
        "合作动态": "品牌背书、生态协同、资本认知",
        "服务权益": "用户满意度、服务口碑、留存复购",
        "海外市场": "品牌国际化认知、渠道信心、市场想象",
        "动态跟踪": "品牌热度、用户关注、后续跟踪",
    }
    if sentiment == "negative" and category != "争议事件":
        return "社媒情绪、品牌口碑、业务应对"
    return mapping.get(category, "品牌热度、用户关注、后续跟踪")


def build_event_id(entity: str, published_at: str, url: str, existing_ids: set[str]) -> str:
    date_value = parse_datetime(published_at)
    date_part = date_value.strftime("%Y%m%d") if date_value else "undated"
    host = hostname_for(url).replace("www.", "").split(".")[0]
    match = re.search(r"/(\d{3,})\.htm?$", url) or re.search(r"/([^/]+?)(?:\.html?|/)?$", url)
    tail = re.sub(r"[^a-z0-9]+", "-", (match.group(1) or "").lower()).strip("-")[:24] or hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
    base = f"auto-{entity.lower()}-{date_part}-{host}-{tail}"
    candidate = base
    counter = 2
    while candidate in existing_ids:
        candidate = f"{base}-{counter}"
        counter += 1
    return candidate


def source_priority(candidate: DiscoveryCandidate) -> int:
    tier_order = {
        "品牌官方": 4,
        "合作方官方": 3,
        "权威媒体": 2,
        "行业媒体": 1,
    }
    return tier_order.get(candidate.source_tier, 0)


def map_huawei_host(host_name: str) -> dict[str, str] | None:
    mapping = {
        "集团官网": {
            "source_tier": "合作方官方",
            "source_name": "华为官网",
            "publisher": "华为",
        },
        "数字能源官网": {
            "source_tier": "合作方官方",
            "source_name": "华为数字能源",
            "publisher": "华为数字能源",
        },
        "终端官网": {
            "source_tier": "合作方官方",
            "source_name": "华为终端官网",
            "publisher": "华为终端",
        },
    }
    return mapping.get(host_name)


def is_relevant_text(text: str) -> bool:
    return bool(re.search(r"问界|AITO|鸿蒙智行|赛力斯|引望", normalize_relevance_text(text), flags=re.IGNORECASE))


def age_in_days(value: datetime) -> int:
    return int((now_shanghai_dt() - value).total_seconds() // 86400)


def now_shanghai_dt() -> datetime:
    return datetime.now(SHANGHAI_TZ)


def title_from_url_slug(url: str) -> str:
    slug = url.rstrip("/").split("/")[-1]
    slug = re.sub(r"-\d{8}$", "", slug)
    return clean_title(slug.replace("-", " "))


def normalize_relevance_text(text: str) -> str:
    return re.sub(r"\s+", "", text or "")


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
    normalized = re.sub(r"(\d{2}:\d{2}:\d{2})\.\d+(?=[+-])", r"\1", normalized)
    normalized = re.sub(r"(\d{2}:\d{2}:\d{2})\.\d+$", r"\1", normalized)
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
    discovered_count = 0
    if args.discover:
        dataset, discovered_count = discover_events(
            dataset,
            discover_days=args.discover_days,
            discover_max_new=args.discover_max_new,
        )
        print(f"Discovered {discovered_count} new events.")

    refreshed_events = normalize_events(collect_events(dataset, prefer_live_title=args.prefer_live_title))
    dataset["events"] = refreshed_events

    if args.sync_input:
        input_snapshot = copy.deepcopy(dataset)
        write_dataset(input_path, input_snapshot)

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
