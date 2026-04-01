#!/usr/bin/env python3
"""Validate and normalize the Wenjie sentiment dashboard dataset.

This script is intentionally conservative: it does not try to invent or scrape
new events yet. Instead it prepares the structure needed for automation by:

1. validating required fields and enums
2. rejecting obviously weak URLs such as homepages or search pages
3. sorting events by publishedAt / capturedAt
4. optionally checking that each URL still returns HTTP 200
5. optionally refreshing meta.updatedAt to the current Shanghai time

When real collectors are ready, add them in `collect_events()` and keep the
final output schema unchanged.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

SHANGHAI_OFFSET = "+08:00"
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


@dataclass
class ValidationIssue:
    level: str
    message: str
    event_id: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input events.json path")
    parser.add_argument("--output", required=True, help="Output events.json path")
    parser.add_argument(
        "--touch-updated-at",
        action="store_true",
        help="Refresh meta.updatedAt to current Shanghai time",
    )
    parser.add_argument(
        "--check-urls",
        action="store_true",
        help="Request every URL and fail on non-200 responses when --strict is enabled",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with status 1 when warnings or errors are found",
    )
    return parser.parse_args()


def now_shanghai_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def load_dataset(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def collect_events(dataset: dict[str, Any]) -> list[dict[str, Any]]:
    # TODO: Replace this passthrough with real collector logic. Keep the output
    # schema compatible with the dashboard frontend and the validation below.
    return list(dataset.get("events", []))


def normalize_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []

    for event in events:
      # Keep only a stable subset of fields on write so automation does not
      # accumulate ad-hoc keys over time.
        normalized.append(
            {
                key: event.get(key)
                for key in REQUIRED_FIELDS
            }
        )

    normalized.sort(key=sort_key, reverse=True)
    return normalized


def sort_key(event: dict[str, Any]) -> datetime:
    effective = event.get("publishedAt") or event.get("capturedAt")
    return parse_datetime(effective) or datetime.min.astimezone()


def parse_datetime(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


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
        request = Request(url, headers={"User-Agent": "Mozilla/5.0 Codex URL validator"})
        with urlopen(request, timeout=20) as response:
            status = getattr(response, "status", 200)
            if status != 200:
                return f"url 请求返回 {status}"
    except HTTPError as error:
        return f"url 请求返回 {error.code}"
    except URLError as error:
        return f"url 请求失败：{error.reason}"
    except Exception as error:  # noqa: BLE001
        return f"url 请求失败：{error}"

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
    dataset["events"] = normalize_events(collect_events(dataset))

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
