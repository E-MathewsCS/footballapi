from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import os
from typing import Any
import threading
import time

from footballapi.normalize import (
    minutes_between,
    parse_iso_utc,
    team_pair_similarity,
    utc_now_iso,
)
from footballapi.providers import EspnProvider, GoalProvider, SofaScoreProvider, StreamedProvider

STATUS_PRIORITY = {
    "live": 5,
    "finished": 4,
    "postponed": 3,
    "cancelled": 2,
    "scheduled": 1,
    "unknown": 0,
}


@dataclass(frozen=True)
class MatchLink:
    base_index: int
    candidate_index: int
    similarity: float
    swapped: bool


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _preferred_status(first: str, second: str) -> str:
    first_priority = STATUS_PRIORITY.get(first, 0)
    second_priority = STATUS_PRIORITY.get(second, 0)
    if second_priority > first_priority:
        return second
    return first


def _score_pair(row: dict[str, Any]) -> tuple[int | None, int | None]:
    return row.get("home_score"), row.get("away_score")


def _max_timestamp(left: str | None, right: str | None) -> str | None:
    left_dt = parse_iso_utc(left)
    right_dt = parse_iso_utc(right)
    if left_dt is None:
        return right
    if right_dt is None:
        return left
    return left if left_dt >= right_dt else right


def _match_records(
    base_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    max_minutes_diff: float = 180.0,
    min_similarity: float = 0.74,
) -> list[MatchLink]:
    links: list[MatchLink] = []
    used_candidates: set[int] = set()

    for base_index, base in enumerate(base_rows):
        best: MatchLink | None = None
        for candidate_index, candidate in enumerate(candidate_rows):
            if candidate_index in used_candidates:
                continue
            minute_gap = minutes_between(
                base.get("start_time_utc"),
                candidate.get("start_time_utc"),
            )
            if minute_gap is not None and minute_gap > max_minutes_diff:
                continue

            similarity, swapped = team_pair_similarity(
                str(base.get("home_team") or ""),
                str(base.get("away_team") or ""),
                str(candidate.get("home_team") or ""),
                str(candidate.get("away_team") or ""),
            )
            if minute_gap is not None:
                similarity += max(0.0, 1.0 - (minute_gap / max_minutes_diff)) * 0.10
            if best is None or similarity > best.similarity:
                best = MatchLink(
                    base_index=base_index,
                    candidate_index=candidate_index,
                    similarity=similarity,
                    swapped=swapped,
                )

        if best is None or best.similarity < min_similarity:
            continue
        used_candidates.add(best.candidate_index)
        links.append(best)
    return links


def merge_provider_matches(
    goal_rows: list[dict[str, Any]],
    espn_rows: list[dict[str, Any]],
    streamed_rows: list[dict[str, Any]],
    sofa_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    sofa_rows = sofa_rows or []
    merged: list[dict[str, Any]] = []
    for row in goal_rows:
        merged_row = copy.deepcopy(row)
        merged_row["sources"] = ["goal"]
        merged_row["confidence"] = 0.72
        merged_row["verification"] = "single_source"
        merged_row["external_ids"] = {"goal": row.get("provider_match_id")}
        merged.append(merged_row)

    goal_espn_links = _match_records(merged, espn_rows, min_similarity=0.76)
    linked_espn: set[int] = set()
    for link in goal_espn_links:
        linked_espn.add(link.candidate_index)
        goal_row = merged[link.base_index]
        espn_row = espn_rows[link.candidate_index]

        goal_row["sources"].append("espn")
        goal_row["external_ids"]["espn"] = espn_row.get("provider_match_id")
        goal_row["confidence"] = round(min(0.98, 0.80 + (link.similarity - 0.75)), 2)
        goal_row["status"] = _preferred_status(
            str(goal_row.get("status") or "unknown"),
            str(espn_row.get("status") or "unknown"),
        )
        goal_row["last_updated_utc"] = _max_timestamp(
            goal_row.get("last_updated_utc"),
            espn_row.get("last_updated_utc"),
        )

        goal_scores = _score_pair(goal_row)
        espn_scores = _score_pair(espn_row)
        if goal_scores == espn_scores and None not in goal_scores:
            goal_row["verification"] = "confirmed_by_multiple_sources"
            goal_row["confidence"] = max(goal_row["confidence"], 0.95)
        elif None not in goal_scores and None not in espn_scores and goal_scores != espn_scores:
            goal_row["verification"] = "score_conflict"
            goal_row["confidence"] = min(goal_row["confidence"], 0.56)
            goal_row["discrepancies"].append(
                (
                    "Goal score "
                    f"{goal_scores[0]}-{goal_scores[1]} differs from ESPN "
                    f"{espn_scores[0]}-{espn_scores[1]}"
                )
            )
        elif None in goal_scores and None not in espn_scores:
            goal_row["home_score"] = espn_row.get("home_score")
            goal_row["away_score"] = espn_row.get("away_score")
            goal_row["verification"] = "filled_from_espn"
            goal_row["confidence"] = max(goal_row["confidence"], 0.83)

        if not goal_row.get("competition") and espn_row.get("competition"):
            goal_row["competition"] = espn_row.get("competition")
        if not goal_row.get("venue") and espn_row.get("venue"):
            goal_row["venue"] = espn_row.get("venue")

    for index, espn_row in enumerate(espn_rows):
        if index in linked_espn:
            continue
        row = copy.deepcopy(espn_row)
        row["sources"] = ["espn"]
        row["confidence"] = 0.68
        row["verification"] = "single_source"
        row["external_ids"] = {"espn": row.get("provider_match_id")}
        merged.append(row)

    merged_sofa_links = _match_records(merged, sofa_rows, min_similarity=0.78)
    linked_sofa: set[int] = set()
    for link in merged_sofa_links:
        linked_sofa.add(link.candidate_index)
        merged_row = merged[link.base_index]
        sofa_row = sofa_rows[link.candidate_index]

        if "sofascore" not in merged_row["sources"]:
            merged_row["sources"].append("sofascore")
        merged_row["external_ids"]["sofascore"] = sofa_row.get("provider_match_id")
        merged_row["status"] = _preferred_status(
            str(merged_row.get("status") or "unknown"),
            str(sofa_row.get("status") or "unknown"),
        )
        merged_row["last_updated_utc"] = _max_timestamp(
            merged_row.get("last_updated_utc"),
            sofa_row.get("last_updated_utc"),
        )

        merged_scores = _score_pair(merged_row)
        sofa_scores = _score_pair(sofa_row)
        if merged_scores == sofa_scores and None not in merged_scores:
            merged_row["verification"] = "confirmed_by_multiple_sources"
            merged_row["confidence"] = max(merged_row["confidence"], 0.96)
        elif None in merged_scores and None not in sofa_scores:
            merged_row["home_score"] = sofa_row.get("home_score")
            merged_row["away_score"] = sofa_row.get("away_score")
            merged_row["verification"] = "filled_from_sofascore"
            merged_row["confidence"] = max(merged_row["confidence"], 0.86)
        elif None not in merged_scores and None not in sofa_scores and merged_scores != sofa_scores:
            merged_row["verification"] = "score_conflict"
            merged_row["confidence"] = min(merged_row["confidence"], 0.50)
            merged_row["discrepancies"].append(
                (
                    "Merged score "
                    f"{merged_scores[0]}-{merged_scores[1]} differs from SofaScore "
                    f"{sofa_scores[0]}-{sofa_scores[1]}"
                )
            )

        if not merged_row.get("competition") and sofa_row.get("competition"):
            merged_row["competition"] = sofa_row.get("competition")

    for index, sofa_row in enumerate(sofa_rows):
        if index in linked_sofa:
            continue
        row = copy.deepcopy(sofa_row)
        row["sources"] = ["sofascore"]
        row["confidence"] = 0.74
        row["verification"] = "single_source"
        row["external_ids"] = {"sofascore": row.get("provider_match_id")}
        merged.append(row)

    stream_links = _match_records(merged, streamed_rows, min_similarity=0.80)
    for link in stream_links:
        merged_row = merged[link.base_index]
        streamed_row = streamed_rows[link.candidate_index]
        watch_url = streamed_row.get("streamed_watch_url")
        if watch_url:
            merged_row["streamed_watch_url"] = watch_url
            if "streamed" not in merged_row["sources"]:
                merged_row["sources"].append("streamed")
            merged_row["external_ids"]["streamed"] = streamed_row.get("provider_match_id")

    def _sort_key(row: dict[str, Any]) -> tuple[int, str, str]:
        status = str(row.get("status") or "unknown")
        status_rank = STATUS_PRIORITY.get(status, 0)
        start = str(row.get("start_time_utc") or "")
        match_name = f"{row.get('home_team') or ''} vs {row.get('away_team') or ''}"
        return (-status_rank, start, match_name.lower())

    merged.sort(key=_sort_key)
    return merged


class LiveScoreService:
    def __init__(
        self,
        cache_seconds: int = 10,
        max_live_stale_seconds: int | None = None,
        providers: list[Any] | None = None,
    ):
        self.cache_seconds = max(0, cache_seconds)
        if max_live_stale_seconds is None:
            max_live_stale_seconds = _env_int("FOOTBALLAPI_MAX_LIVE_STALE_SECONDS", 180)
        self.max_live_stale_seconds = max(0, max_live_stale_seconds)
        self.providers = providers or [
            GoalProvider(),
            EspnProvider(),
            SofaScoreProvider(),
            StreamedProvider(),
        ]
        self._lock = threading.Lock()
        self._cache_payload: dict[str, Any] | None = None
        self._cache_expires_at = 0.0

    def get_scores(
        self,
        status: str = "live",
        source: str = "all",
        league: str | None = None,
        include_stale: bool = False,
        include_conflicts: bool = False,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        payload = self._refresh_if_needed(force_refresh=force_refresh)
        matches = payload.get("matches") or []
        filtered = self._filter_matches(matches, status=status, source=source, league=league)
        filtered, dropped_stale, dropped_conflicts = self._apply_quality_gate(
            filtered,
            generated_at_utc=payload.get("generated_at_utc"),
            include_stale=include_stale,
            include_conflicts=include_conflicts,
        )

        result = copy.deepcopy(payload)
        result["matches"] = filtered
        result["count"] = len(filtered)
        result["quality"] = {
            "max_live_stale_seconds": self.max_live_stale_seconds,
            "include_stale": include_stale,
            "include_conflicts": include_conflicts,
            "dropped_stale_count": dropped_stale,
            "dropped_conflict_count": dropped_conflicts,
        }
        result["filters"] = {
            "status": status,
            "source": source,
            "league": league,
        }
        return result

    def _refresh_if_needed(self, force_refresh: bool) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            if (
                not force_refresh
                and self._cache_payload is not None
                and now < self._cache_expires_at
            ):
                return copy.deepcopy(self._cache_payload)

        provider_rows: dict[str, list[dict[str, Any]]] = {}
        provider_errors: dict[str, str] = {}

        with ThreadPoolExecutor(max_workers=max(1, len(self.providers))) as executor:
            future_map = {executor.submit(p.fetch_matches): p for p in self.providers}
            for future in as_completed(future_map):
                provider = future_map[future]
                provider_name = getattr(provider, "name", provider.__class__.__name__.lower())
                try:
                    data = future.result()
                except Exception as exc:
                    provider_rows[provider_name] = []
                    provider_errors[provider_name] = str(exc)
                    continue
                provider_rows[provider_name] = data if isinstance(data, list) else []

        goal_rows = provider_rows.get("goal", [])
        espn_rows = provider_rows.get("espn", [])
        sofa_rows = provider_rows.get("sofascore", [])
        streamed_rows = provider_rows.get("streamed", [])
        merged_rows = merge_provider_matches(goal_rows, espn_rows, streamed_rows, sofa_rows)

        payload: dict[str, Any] = {
            "generated_at_utc": utc_now_iso(),
            "matches": merged_rows,
            "count": len(merged_rows),
            "providers": {
                provider: {
                    "ok": provider not in provider_errors,
                    "count": len(provider_rows.get(provider, [])),
                    "error": provider_errors.get(provider),
                }
                for provider in sorted(set(provider_rows.keys()) | set(provider_errors.keys()))
            },
        }

        with self._lock:
            self._cache_payload = payload
            self._cache_expires_at = time.time() + self.cache_seconds
            return copy.deepcopy(payload)

    @staticmethod
    def _filter_matches(
        matches: list[dict[str, Any]],
        status: str,
        source: str,
        league: str | None,
    ) -> list[dict[str, Any]]:
        filtered = list(matches)

        normalized_status = (status or "all").strip().lower()
        if normalized_status and normalized_status not in {"all", "*"}:
            wanted_statuses = {
                entry.strip().lower()
                for entry in normalized_status.split(",")
                if entry.strip()
            }
            filtered = [
                row
                for row in filtered
                if str(row.get("status") or "").lower() in wanted_statuses
            ]

        normalized_source = (source or "all").strip().lower()
        if normalized_source and normalized_source not in {"all", "*"}:
            filtered = [
                row
                for row in filtered
                if normalized_source in {src.lower() for src in row.get("sources", [])}
            ]

        if league:
            needle = league.strip().lower()
            if needle:
                filtered = [
                    row
                    for row in filtered
                    if needle in str(row.get("competition") or "").lower()
                ]
        return filtered

    def _apply_quality_gate(
        self,
        matches: list[dict[str, Any]],
        generated_at_utc: str | None,
        include_stale: bool,
        include_conflicts: bool,
    ) -> tuple[list[dict[str, Any]], int, int]:
        reference_time = parse_iso_utc(generated_at_utc) or parse_iso_utc(utc_now_iso())
        if reference_time is None:
            return list(matches), 0, 0

        kept: list[dict[str, Any]] = []
        dropped_stale = 0
        dropped_conflicts = 0
        for row in matches:
            candidate = copy.deepcopy(row)
            last_updated = parse_iso_utc(candidate.get("last_updated_utc"))
            age_seconds: float | None = None
            if last_updated is not None:
                age_seconds = max(0.0, (reference_time - last_updated).total_seconds())
            candidate["last_update_age_seconds"] = age_seconds

            status = str(candidate.get("status") or "unknown").lower()
            is_stale = status == "live" and (
                age_seconds is None or age_seconds > float(self.max_live_stale_seconds)
            )
            candidate["is_stale"] = is_stale
            if is_stale:
                candidate["staleness_reason"] = (
                    "missing_last_update"
                    if age_seconds is None
                    else f"older_than_{self.max_live_stale_seconds}_seconds"
                )

            if candidate.get("verification") == "score_conflict" and not include_conflicts:
                dropped_conflicts += 1
                continue
            if is_stale and not include_stale:
                dropped_stale += 1
                continue
            kept.append(candidate)

        return kept, dropped_stale, dropped_conflicts
