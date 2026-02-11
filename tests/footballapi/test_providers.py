from __future__ import annotations

import json

from footballapi.providers import (
    parse_espn_scoreboard_payload,
    parse_goal_live_scores_html,
    parse_sofascore_live_payload,
    parse_streamed_live_payload,
)


def test_parse_goal_live_scores_html_extracts_live_match() -> None:
    next_data = {
        "props": {
            "pageProps": {
                "content": {
                    "liveScores": [
                        {
                            "competition": {"name": "Cup"},
                            "matches": [
                                {
                                    "id": "goal-1",
                                    "status": "LIVE",
                                    "startDate": "2026-02-11T16:00:00.000Z",
                                    "lastUpdatedAt": "2026-02-11T16:28:24.000Z",
                                    "teamA": {"name": "Alpha FC"},
                                    "teamB": {"name": "Beta FC"},
                                    "score": {"teamA": 2, "teamB": 1},
                                    "period": {"type": "SECOND_HALF", "minute": 77, "extra": 0},
                                    "venue": {"name": "Alpha Stadium"},
                                }
                            ],
                        }
                    ]
                }
            }
        }
    }
    html = (
        '<html><head></head><body><script id="__NEXT_DATA__" '
        f'type="application/json">{json.dumps(next_data)}</script></body></html>'
    )

    rows = parse_goal_live_scores_html(html)
    assert len(rows) == 1
    row = rows[0]
    assert row["provider"] == "goal"
    assert row["competition"] == "Cup"
    assert row["home_team"] == "Alpha FC"
    assert row["away_team"] == "Beta FC"
    assert row["home_score"] == 2
    assert row["away_score"] == 1
    assert row["status"] == "live"
    assert row["minute"] == 77


def test_parse_espn_scoreboard_payload_extracts_status_and_clock() -> None:
    payload = {
        "events": [
            {
                "id": "espn-1",
                "name": "Beta FC at Alpha FC",
                "date": "2026-02-11T16:00Z",
                "competitions": [
                    {
                        "startDate": "2026-02-11T16:00:00Z",
                        "status": {
                            "displayClock": "77'",
                            "type": {
                                "name": "STATUS_IN_PROGRESS",
                                "state": "in",
                                "description": "Second Half",
                                "shortDetail": "77'",
                            },
                        },
                        "competitors": [
                            {
                                "homeAway": "home",
                                "score": "2",
                                "team": {"displayName": "Alpha FC"},
                            },
                            {
                                "homeAway": "away",
                                "score": "1",
                                "team": {"displayName": "Beta FC"},
                            },
                        ],
                        "venue": {"fullName": "Alpha Stadium"},
                    }
                ],
            }
        ]
    }

    rows = parse_espn_scoreboard_payload(payload)
    assert len(rows) == 1
    row = rows[0]
    assert row["provider"] == "espn"
    assert row["status"] == "live"
    assert row["minute"] == 77
    assert row["home_score"] == 2
    assert row["away_score"] == 1
    assert row["home_team"] == "Alpha FC"
    assert row["away_team"] == "Beta FC"


def test_parse_streamed_live_payload_returns_only_football() -> None:
    payload = [
        {
            "id": "alpha-vs-beta-123",
            "category": "football",
            "date": 1770822000000,
            "title": "Alpha FC vs Beta FC",
            "teams": {
                "home": {"name": "Alpha FC"},
                "away": {"name": "Beta FC"},
            },
        },
        {
            "id": "other-1",
            "category": "basketball",
            "date": 1770822000000,
            "title": "Hoops A vs Hoops B",
        },
    ]

    rows = parse_streamed_live_payload(payload)
    assert len(rows) == 1
    row = rows[0]
    assert row["provider"] == "streamed"
    assert row["home_team"] == "Alpha FC"
    assert row["away_team"] == "Beta FC"
    assert row["streamed_watch_url"] == "https://streamed.pk/watch/alpha-vs-beta-123"


def test_parse_sofascore_live_payload_extracts_live_match() -> None:
    payload = {
        "events": [
            {
                "id": 123,
                "startTimestamp": 1770829200,
                "status": {"code": 6, "description": "1st half", "type": "inprogress"},
                "tournament": {"name": "Frauen-Bundesliga"},
                "homeTeam": {"name": "Alpha FC"},
                "awayTeam": {"name": "Beta FC"},
                "homeScore": {"current": 1},
                "awayScore": {"current": 0},
            }
        ]
    }

    rows = parse_sofascore_live_payload(payload, fetched_at_utc="2026-02-11T17:40:00Z")
    assert len(rows) == 1
    row = rows[0]
    assert row["provider"] == "sofascore"
    assert row["status"] == "live"
    assert row["home_team"] == "Alpha FC"
    assert row["away_team"] == "Beta FC"
    assert row["home_score"] == 1
    assert row["away_score"] == 0
    assert row["last_updated_utc"] == "2026-02-11T17:40:00Z"
