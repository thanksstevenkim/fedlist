#!/usr/bin/env python3
"""Fetch Fediverse instance statistics into data/stats.json."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

TIMEOUT = 5
BASE_DIR = Path(__file__).resolve().parent.parent
INSTANCES_PATH = BASE_DIR / "data" / "instances.json"
STATS_PATH = BASE_DIR / "data" / "stats.json"

try:  # Optional dependency, falls back to urllib if unavailable
    import requests  # type: ignore
except Exception:  # pragma: no cover - optional import guard
    requests = None


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    instances = load_instances(INSTANCES_PATH)
    if not instances:
        logging.error("No instances to process. Did you populate data/instances.json?")
        return

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    results = []

    for instance in instances:
        url = str(instance.get("url", "")).strip()
        platform = str(instance.get("platform", "")).strip().lower()

        if not url:
            logging.warning("Skipping instance without URL: %s", instance.get("name", "(unknown)"))
            continue

        try:
            if platform == "mastodon":
                stats = fetch_mastodon_stats(url)
            elif platform == "misskey":
                stats = fetch_misskey_stats(url)
            else:
                logging.warning("Skipping %s (%s): unsupported platform", url, platform or "unknown")
                continue
        except Exception as exc:
            logging.warning("Skipping %s: %s", url, exc)
            continue

        if not stats:
            logging.warning("Skipping %s: no statistics returned", url)
            continue

        stats_record = {
            "url": url.rstrip("/"),
            "users_total": stats.get("users_total"),
            "users_active_month": stats.get("users_active_month"),
            "statuses": stats.get("statuses"),
            "fetched_at": now,
        }
        results.append(stats_record)
        logging.info("Fetched stats for %s", url)

    STATS_PATH.write_text(json.dumps(results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    logging.info("Wrote %s", STATS_PATH.relative_to(BASE_DIR))


def load_instances(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        logging.error("Instances file not found: %s", path)
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logging.error("Invalid JSON in %s: %s", path, exc)
        return []

    if not isinstance(data, list):
        logging.error("Expected a list in %s", path)
        return []

    return data


def fetch_mastodon_stats(base_url: str) -> Dict[str, Optional[int]]:
    endpoint = normalize_base(base_url) + "/api/v1/instance"
    payload = request_json(endpoint)

    usage = payload.get("usage", {}) if isinstance(payload, dict) else {}
    users = usage.get("users", {}) if isinstance(usage, dict) else {}
    stats = payload.get("stats", {}) if isinstance(payload, dict) else {}

    users_total = coalesce_numeric(users.get("total"), stats.get("user_count"))
    users_active = coalesce_numeric(users.get("active_month"), stats.get("active_month"))
    statuses = coalesce_numeric(
        usage.get("statuses"),
        usage.get("local_posts"),
        stats.get("status_count"),
        stats.get("posts")
    )

    return {
        "users_total": users_total,
        "users_active_month": users_active,
        "statuses": statuses,
    }


def fetch_misskey_stats(base_url: str) -> Dict[str, Optional[int]]:
    endpoint = normalize_base(base_url) + "/api/meta"
    payload = request_json(endpoint, method="POST", json_body={"detail": True})

    stats = payload.get("stats", {}) if isinstance(payload, dict) else {}

    users_total = coalesce_numeric(
        stats.get("originalUsersCount"),
        stats.get("usersCount"),
    )
    users_active = coalesce_numeric(
        stats.get("monthlyActiveUsers"),
        stats.get("weeklyActiveUsers"),
        stats.get("dailyActiveUsers"),
        stats.get("activeUsers"),
    )
    statuses = coalesce_numeric(
        stats.get("originalNotesCount"),
        stats.get("notesCount"),
    )

    return {
        "users_total": users_total,
        "users_active_month": users_active,
        "statuses": statuses,
    }


def request_json(url: str, method: str = "GET", json_body: Optional[Dict[str, Any]] = None) -> Any:
    headers = {"Accept": "application/json"}

    if requests is not None:
        try:
            response = requests.request(method, url, json=json_body, timeout=TIMEOUT, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            raise RuntimeError(str(exc))

    import urllib.error
    import urllib.request

    data_bytes: Optional[bytes] = None
    if json_body is not None:
        headers["Content-Type"] = "application/json"
        data_bytes = json.dumps(json_body).encode("utf-8")

    request = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            text = response.read().decode(charset)
    except urllib.error.URLError as exc:  # pragma: no cover - network failure
        raise RuntimeError(str(exc))

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:  # pragma: no cover - invalid response
        raise RuntimeError(f"Invalid JSON response from {url}: {exc}")


def coalesce_numeric(*values: Any) -> Optional[int]:
    for value in values:
        number = to_int(value)
        if number is not None:
            return number
    return None


def to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number


def normalize_base(url: str) -> str:
    return url.rstrip("/")


if __name__ == "__main__":
    main()
