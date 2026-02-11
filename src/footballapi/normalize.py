from __future__ import annotations

from datetime import datetime, timezone
from difflib import SequenceMatcher
import re
import unicodedata

TEAM_STOP_WORDS = {
    "ac",
    "afc",
    "athletic",
    "atletico",
    "cf",
    "club",
    "fc",
    "fk",
    "foot",
    "football",
    "if",
    "nk",
    "rc",
    "sc",
    "sk",
    "sporting",
    "sv",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def epoch_ms_to_iso_utc(value: int | float | None) -> str | None:
    if value is None:
        return None
    try:
        timestamp = float(value) / 1000.0
    except (TypeError, ValueError):
        return None
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def epoch_seconds_to_iso_utc(value: int | float | None) -> str | None:
    if value is None:
        return None
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        return None
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def normalize_team_name(name: str | None) -> str:
    if not name:
        return ""
    text = unicodedata.normalize("NFKD", name)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    parts = [p for p in text.split() if p and p not in TEAM_STOP_WORDS]
    if not parts:
        return ""
    return " ".join(parts)


def similarity(left: str | None, right: str | None) -> float:
    left_norm = normalize_team_name(left)
    right_norm = normalize_team_name(right)
    if not left_norm and not right_norm:
        return 1.0
    if not left_norm or not right_norm:
        return 0.0
    return SequenceMatcher(None, left_norm, right_norm).ratio()


def team_pair_similarity(
    home_a: str,
    away_a: str,
    home_b: str,
    away_b: str,
) -> tuple[float, bool]:
    direct = (similarity(home_a, home_b) + similarity(away_a, away_b)) / 2.0
    swapped = (similarity(home_a, away_b) + similarity(away_a, home_b)) / 2.0
    if direct >= swapped:
        return direct, False
    return swapped, True


def minutes_between(left_iso: str | None, right_iso: str | None) -> float | None:
    left = parse_iso_utc(left_iso)
    right = parse_iso_utc(right_iso)
    if left is None or right is None:
        return None
    return abs((left - right).total_seconds()) / 60.0
