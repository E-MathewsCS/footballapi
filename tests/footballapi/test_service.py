from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone

from footballapi.service import LiveScoreService, merge_provider_matches


class StaticProvider:
    def __init__(self, name: str, rows: list[dict]):
        self.name = name
        self._rows = rows

    def fetch_matches(self) -> list[dict]:
        return copy.deepcopy(self._rows)


def _goal_row(home_score: int, away_score: int) -> dict:
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "provider": "goal",
        "provider_match_id": "goal-1",
        "competition": "Cup",
        "home_team": "Alpha FC",
        "away_team": "Beta FC",
        "home_score": home_score,
        "away_score": away_score,
        "status": "live",
        "raw_status": "LIVE",
        "period": "SECOND_HALF",
        "minute": 77,
        "extra_minute": 0,
        "start_time_utc": now,
        "last_updated_utc": now,
        "venue": "Alpha Stadium",
        "streamed_watch_url": None,
        "discrepancies": [],
    }


def _espn_row(home_score: int, away_score: int) -> dict:
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "provider": "espn",
        "provider_match_id": "espn-1",
        "competition": "Cup",
        "home_team": "Alpha FC",
        "away_team": "Beta FC",
        "home_score": home_score,
        "away_score": away_score,
        "status": "live",
        "raw_status": "STATUS_IN_PROGRESS",
        "period": "Second Half",
        "minute": 77,
        "extra_minute": 0,
        "start_time_utc": now,
        "last_updated_utc": now,
        "venue": "Alpha Stadium",
        "streamed_watch_url": None,
        "discrepancies": [],
    }


def _streamed_row() -> dict:
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "provider": "streamed",
        "provider_match_id": "alpha-vs-beta-123",
        "competition": None,
        "home_team": "Alpha FC",
        "away_team": "Beta FC",
        "home_score": None,
        "away_score": None,
        "status": "unknown",
        "raw_status": None,
        "period": None,
        "minute": None,
        "extra_minute": None,
        "start_time_utc": now,
        "last_updated_utc": now,
        "venue": None,
        "streamed_watch_url": "https://streamed.pk/watch/alpha-vs-beta-123",
        "discrepancies": [],
    }


def test_merge_provider_matches_marks_confirmed_when_scores_match() -> None:
    rows = merge_provider_matches([_goal_row(2, 1)], [_espn_row(2, 1)], [_streamed_row()])
    assert len(rows) == 1
    row = rows[0]
    assert row["verification"] == "confirmed_by_multiple_sources"
    assert row["confidence"] >= 0.95
    assert set(row["sources"]) == {"goal", "espn", "streamed"}
    assert row["streamed_watch_url"] == "https://streamed.pk/watch/alpha-vs-beta-123"


def test_merge_provider_matches_marks_conflict_when_scores_differ() -> None:
    rows = merge_provider_matches([_goal_row(2, 1)], [_espn_row(1, 1)], [])
    assert len(rows) == 1
    row = rows[0]
    assert row["verification"] == "score_conflict"
    assert row["confidence"] <= 0.56
    assert row["discrepancies"]


def test_live_score_service_applies_filters() -> None:
    goal_rows = [
        _goal_row(2, 1),
        {
            **_goal_row(0, 0),
            "provider_match_id": "goal-2",
            "home_team": "Gamma FC",
            "away_team": "Delta FC",
            "status": "finished",
            "raw_status": "RESULT",
        },
    ]
    service = LiveScoreService(
        cache_seconds=0,
        providers=[
            StaticProvider("goal", goal_rows),
            StaticProvider("espn", []),
            StaticProvider("sofascore", []),
            StaticProvider("streamed", []),
        ],
    )

    live_payload = service.get_scores(status="live")
    assert live_payload["count"] == 1
    assert live_payload["matches"][0]["home_team"] == "Alpha FC"

    finished_payload = service.get_scores(status="finished")
    assert finished_payload["count"] == 1
    assert finished_payload["matches"][0]["home_team"] == "Gamma FC"

    goal_only_payload = service.get_scores(status="all", source="goal")
    assert goal_only_payload["count"] == 2


def test_live_score_service_drops_stale_live_rows_by_default() -> None:
    stale_time = (
        datetime.now(timezone.utc) - timedelta(minutes=20)
    ).isoformat().replace("+00:00", "Z")
    stale_goal = _goal_row(1, 0)
    stale_goal["last_updated_utc"] = stale_time

    service = LiveScoreService(
        cache_seconds=0,
        max_live_stale_seconds=60,
        providers=[
            StaticProvider("goal", [stale_goal]),
            StaticProvider("espn", []),
            StaticProvider("sofascore", []),
            StaticProvider("streamed", []),
        ],
    )
    payload = service.get_scores(status="live")
    assert payload["count"] == 0
    assert payload["quality"]["dropped_stale_count"] == 1
