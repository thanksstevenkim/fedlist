#!/usr/bin/env python3
"""Fetch Fediverse instance statistics into data/stats.json with ActivityPub verification."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse

TIMEOUT = 5
USER_AGENT = "fedlist-stats-fetcher/1.0"
BASE_DIR = Path(__file__).resolve().parent.parent
INSTANCES_PATH = BASE_DIR / "data" / "instances.json"
STATS_PATH = BASE_DIR / "data" / "stats.json"

try:  # Optional dependency, falls back to urllib if unavailable
    import requests  # type: ignore
except Exception:  # pragma: no cover - optional import guard
    requests = None


@dataclass
class Instance:
    name: str
    host: str
    url: str
    platform: str


class FetchError(RuntimeError):
    """Raised when a remote fetch fails."""


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    instances = list(load_instances(INSTANCES_PATH))
    if not instances:
        logging.error("No instances to process. Did you populate data/instances.json?")
        return

    now = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    results: List[Dict[str, Any]] = []
    discovered_hosts: Set[str] = set()
    existing_hosts = {instance.host for instance in instances}

    for instance in instances:
        record, errors, peers = process_instance(instance, now)
        results.append(record)

        if record["verified_activitypub"]:
            logging.info("Fetched stats for %s", instance.host)
        else:
            reason = "; ".join(errors) if errors else "no successful responses"
            logging.warning(
                "Verification failed for %s (%s): %s",
                instance.host,
                instance.url,
                reason,
            )

        if args.discover_peers and peers:
            discovered_hosts.update(peers)

    STATS_PATH.write_text(
        json.dumps(results, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logging.info("Wrote %s", STATS_PATH.relative_to(BASE_DIR))

    if args.discover_peers:
        suggestions = sorted(host for host in discovered_hosts if host not in existing_hosts)
        emit_peer_suggestions(suggestions, args.peer_output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch ActivityPub stats for configured instances.")
    parser.add_argument(
        "--discover-peers",
        action="store_true",
        help="Attempt to gather federation peers for later curation.",
    )
    parser.add_argument(
        "--peer-output",
        default=str(BASE_DIR / "data" / "peer_suggestions.json"),
        help="File path for discovered peers (use '-' for stdout).",
    )
    return parser.parse_args()


def emit_peer_suggestions(hosts: Sequence[str], target: str) -> None:
    if not hosts:
        logging.info("No federation peers discovered.")
        return

    if target == "-":
        json.dump(hosts, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
        logging.info("Emitted %d peer suggestions to stdout", len(hosts))
        return

    path = Path(target)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(hosts, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    logging.info("Wrote %s", format_relative(path))


def format_relative(path: Path) -> str:
    try:
        return str(path.relative_to(BASE_DIR))
    except ValueError:
        return str(path)


def load_instances(path: Path) -> Iterable[Instance]:
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

    instances: List[Instance] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url", "")).strip()
        if not url:
            logging.warning("Skipping entry without URL: %s", entry)
            continue
        host = extract_host(entry)
        if not host:
            logging.warning("Skipping %s: could not determine host", url)
            continue
        instances.append(
            Instance(
                name=str(entry.get("name", "")).strip() or host,
                host=host,
                url=normalize_base_url(url, host),
                platform=str(entry.get("platform", "")).strip().lower(),
            )
        )
    return instances


def extract_host(entry: Dict[str, Any]) -> str:
    host = str(entry.get("host", "")).strip().lower()
    if host:
        return host

    url = entry.get("url")
    if isinstance(url, str) and url:
        parsed = urlparse(url)
        if parsed.hostname:
            return parsed.hostname.lower()
        return url.strip().lower().rstrip("/")
    return ""


def normalize_base_url(url: str, host: str) -> str:
    if not url:
        return f"https://{host}"
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or host
    path = parsed.path.rstrip("/")
    if not path:
        path = ""
    rebuilt = f"{scheme}://{netloc}{path}"
    return rebuilt.rstrip("/")


def process_instance(instance: Instance, timestamp: str) -> Tuple[Dict[str, Any], List[str], Set[str]]:
    record: Dict[str, Any] = {
        "host": instance.host,
        "verified_activitypub": False,
        "software": {},
        "open_registrations": None,
        "users_total": None,
        "users_active_month": None,
        "statuses": None,
        "languages_detected": [],
        "fetched_at": timestamp,
    }
    errors: List[str] = []
    languages: List[str] = []
    languages_seen = set()
    peers: Set[str] = set()

    try:
        nodeinfo = fetch_nodeinfo(instance.host)
    except FetchError as exc:
        errors.append(f"nodeinfo: {exc}")
        nodeinfo = None

    if nodeinfo:
        record["verified_activitypub"] = True
        update_software(record, nodeinfo.get("software", {}))
        update_open_registrations(record, nodeinfo.get("openRegistrations"))

        usage = nodeinfo.get("usage") if isinstance(nodeinfo, dict) else None
        users = usage.get("users") if isinstance(usage, dict) else None
        update_numeric(record, "users_total", coerce_int(users, "total"))
        update_numeric(record, "users_active_month", coerce_int(users, "activeMonth"))
        update_numeric(record, "statuses", coerce_int(usage, "localPosts"))

        append_languages(languages, languages_seen, usage.get("languages") if isinstance(usage, dict) else None)
        peers.update(extract_peer_hosts_from_nodeinfo(nodeinfo))

    platform_data: Optional[Dict[str, Any]] = None
    if instance.platform == "mastodon":
        try:
            platform_data = fetch_mastodon(instance.url)
        except FetchError as exc:
            errors.append(f"mastodon: {exc}")
        else:
            record["verified_activitypub"] = True
    elif instance.platform == "misskey":
        try:
            platform_data = fetch_misskey(instance.url)
        except FetchError as exc:
            errors.append(f"misskey: {exc}")
        else:
            record["verified_activitypub"] = True
    else:
        errors.append(f"unsupported platform: {instance.platform or 'unknown'}")

    if platform_data:
        update_software(record, platform_data.get("software", {}))
        update_open_registrations(record, platform_data.get("open_registrations"))
        update_numeric(record, "users_total", platform_data.get("users_total"))
        update_numeric(record, "users_active_month", platform_data.get("users_active_month"))
        update_numeric(record, "statuses", platform_data.get("statuses"))
        append_languages(languages, languages_seen, platform_data.get("languages"))
        peers.update(normalize_peer_list(platform_data.get("peers")))

    record["languages_detected"] = languages
    return record, errors, peers


def extract_peer_hosts_from_nodeinfo(document: Any) -> Set[str]:
    hosts: Set[str] = set()
    if not isinstance(document, dict):
        return hosts
    metadata = document.get("metadata")
    if isinstance(metadata, dict):
        if "peers" in metadata:
            hosts.update(normalize_peer_list(metadata.get("peers")))
        federation = metadata.get("federation")
        if isinstance(federation, dict):
            if "peers" in federation:
                hosts.update(normalize_peer_list(federation.get("peers")))
            if "domains" in federation:
                hosts.update(normalize_peer_list(federation.get("domains")))
    return hosts


def fetch_nodeinfo(host: str) -> Optional[Dict[str, Any]]:
    last_error: Optional[FetchError] = None
    for scheme in ("https", "http"):
        index_url = f"{scheme}://{host}/.well-known/nodeinfo"
        try:
            index_payload = request_json(index_url)
            if not isinstance(index_payload, dict):
                raise FetchError("unexpected nodeinfo index payload")
            links = index_payload.get("links")
            if not isinstance(links, Sequence):
                raise FetchError("nodeinfo index missing links")
            best_link = select_latest_nodeinfo_link(links)
            if not best_link:
                raise FetchError("no valid nodeinfo links")
            href = best_link.get("href")
            if not isinstance(href, str) or not href:
                raise FetchError("nodeinfo link missing href")
            payload = request_json(href)
            if not isinstance(payload, dict):
                raise FetchError("unexpected nodeinfo document")
            return payload
        except FetchError as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise FetchError(str(last_error))
    raise FetchError("nodeinfo endpoint unreachable")


def select_latest_nodeinfo_link(links: Sequence[Any]) -> Optional[Dict[str, Any]]:
    def version_key(link: Dict[str, Any]) -> Tuple[int, int]:
        version = ""
        if isinstance(link, dict):
            rel = link.get("rel")
            href = link.get("href")
            if isinstance(rel, str):
                version = rel.rsplit("/", 1)[-1]
            elif isinstance(href, str):
                version = href.rstrip("/").rsplit("/", 1)[-1]
        major, minor = 0, 0
        if version:
            parts = version.replace("nodeinfo", "").strip("/ ")
            try:
                major_minor = parts.split(".")
                if len(major_minor) >= 2:
                    major = int(major_minor[0])
                    minor = int(major_minor[1])
            except (TypeError, ValueError):
                major, minor = 0, 0
        return major, minor

    candidates = [link for link in links if isinstance(link, dict)]
    if not candidates:
        return None
    return max(candidates, key=version_key)


def fetch_mastodon(base_url: str) -> Dict[str, Any]:
    errors: List[str] = []
    for path in ("/api/v2/instance", "/api/v1/instance"):
        try:
            payload = request_json(f"{base_url}{path}")
        except FetchError as exc:
            errors.append(str(exc))
            continue
        if not isinstance(payload, dict):
            continue
        result = parse_mastodon_payload(payload, path.endswith("v2/instance"))
        result["peers"] = sorted(fetch_mastodon_peers(base_url))
        return result
    raise FetchError("; ".join(errors) if errors else "instance API unavailable")


def fetch_mastodon_peers(base_url: str) -> Set[str]:
    try:
        payload = request_json(f"{base_url}/api/v1/instance/peers")
    except FetchError:
        return set()
    return normalize_peer_list(payload)


def parse_mastodon_payload(payload: Dict[str, Any], is_v2: bool) -> Dict[str, Any]:
    usage = payload.get("usage") if isinstance(payload, dict) else None
    users = usage.get("users") if isinstance(usage, dict) else None
    stats = payload.get("stats") if isinstance(payload, dict) else None
    configuration = payload.get("configuration") if isinstance(payload, dict) else None

    result: Dict[str, Any] = {
        "software": {
            "name": payload.get("software", {}).get("name")
            if isinstance(payload.get("software"), dict)
            else payload.get("version") and "mastodon",
            "version": payload.get("version"),
        },
        "open_registrations": payload.get("registrations", {}).get("enabled")
        if isinstance(payload.get("registrations"), dict)
        else payload.get("registrations"),
        "users_total": first_int(
            coerce_int(users, "total"),
            coerce_int(stats, "user_count"),
        ),
        "users_active_month": first_int(
            coerce_int(users, "activeMonth"),
            coerce_int(stats, "active_month"),
        ),
        "statuses": first_int(
            coerce_int(usage, "localPosts"),
            coerce_int(stats, "status_count"),
        ),
        "languages": [],
    }

    lang_seen: set = set()
    if configuration and isinstance(configuration, dict):
        append_languages(result["languages"], lang_seen, configuration.get("languages"))
    elif is_v2:
        append_languages(result["languages"], lang_seen, payload.get("languages"))

    software = payload.get("software")
    if isinstance(software, dict):
        result["software"] = {
            "name": software.get("name"),
            "version": software.get("version"),
        }

    return result


def fetch_misskey(base_url: str) -> Dict[str, Any]:
    payload = request_json(
        f"{base_url}/api/meta",
        method="POST",
        json_body={"detail": True},
    )
    if not isinstance(payload, dict):
        raise FetchError("unexpected meta payload")

    stats = payload.get("stats") if isinstance(payload, dict) else None

    result: Dict[str, Any] = {
        "software": {
            "name": payload.get("softwareName") or "misskey",
            "version": payload.get("version"),
        },
        "open_registrations": payload.get("disableRegistration") is False,
        "users_total": first_int(
            coerce_int(stats, "originalUsersCount"),
            coerce_int(stats, "usersCount"),
        ),
        "users_active_month": first_int(
            coerce_int(stats, "monthlyActiveUsers"),
            coerce_int(stats, "activeUsers"),
        ),
        "statuses": first_int(
            coerce_int(stats, "originalNotesCount"),
            coerce_int(stats, "notesCount"),
        ),
        "languages": [],
    }

    federation = payload.get("federation") if isinstance(payload, dict) else None
    if isinstance(federation, dict):
        result["peers"] = sorted(normalize_peer_list(federation.get("peers")))
    return result


def update_software(record: Dict[str, Any], software: Any) -> None:
    if not isinstance(software, dict):
        return
    target = record.get("software")
    if not isinstance(target, dict):
        target = {}
        record["software"] = target
    name = software.get("name")
    version = software.get("version")
    if name and not target.get("name"):
        target["name"] = str(name)
    if version and not target.get("version"):
        target["version"] = str(version)


def update_open_registrations(record: Dict[str, Any], value: Any) -> None:
    boolean = coerce_bool(value)
    if boolean is None:
        return
    if record.get("open_registrations") is None:
        record["open_registrations"] = boolean


def update_numeric(record: Dict[str, Any], key: str, value: Any) -> None:
    number = coerce_int_value(value)
    if number is None:
        return
    if record.get(key) is None:
        record[key] = number


def append_languages(target: List[str], seen: set, values: Any) -> None:
    if isinstance(values, dict):
        values = values.values()
    if isinstance(values, (str, bytes)):
        values = [values]
    if not isinstance(values, Sequence) and not isinstance(values, set):
        return
    for value in values:
        code = normalize_language_code(value)
        if not code:
            continue
        if code in seen:
            continue
        seen.add(code)
        target.append(code)


def normalize_peer_list(values: Any) -> Set[str]:
    hosts: Set[str] = set()
    if values is None:
        return hosts
    if isinstance(values, dict):
        for item in values.values():
            hosts.update(normalize_peer_list(item))
        return hosts
    if isinstance(values, (list, tuple, set)):
        for item in values:
            hosts.update(normalize_peer_list(item))
        return hosts
    host = normalize_peer_host(values)
    if host:
        hosts.add(host)
    return hosts


def normalize_peer_host(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.rstrip("/")
    if text.startswith("http://") or text.startswith("https://"):
        parsed = urlparse(text)
        if parsed.hostname:
            host = parsed.hostname.lower()
            if parsed.port:
                return f"{host}:{parsed.port}"
            return host
        text = text.split("://", 1)[-1]
    return text.lower()


def normalize_language_code(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.lower()


def first_int(*values: Any) -> Optional[int]:
    for value in values:
        number = coerce_int_value(value)
        if number is not None:
            return number
    return None


def coerce_int(mapping: Any, key: str) -> Optional[int]:
    if isinstance(mapping, dict):
        return coerce_int_value(mapping.get(key))
    return None


def coerce_int_value(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    if number < 0:
        return None
    return number


def coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value in {"true", "True", "1", 1}:
        return True
    if value in {"false", "False", "0", 0}:
        return False
    return None


def request_json(
    url: str,
    method: str = "GET",
    json_body: Optional[Dict[str, Any]] = None,
) -> Any:
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }

    if requests is not None:
        try:
            response = requests.request(
                method,
                url,
                json=json_body,
                timeout=TIMEOUT,
                headers=headers,
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # pragma: no cover - network failure
            raise FetchError(str(exc))

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
        raise FetchError(str(exc))

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:  # pragma: no cover - invalid response
        raise FetchError(f"Invalid JSON response from {url}: {exc}")


if __name__ == "__main__":
    main()
