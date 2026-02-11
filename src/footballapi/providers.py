from __future__ import annotations

import json
import re
import ssl
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import os

from footballapi.normalize import epoch_ms_to_iso_utc, epoch_seconds_to_iso_utc, utc_now_iso

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
)

GOAL_LIVE_SCORES_URL = "https://www.goal.com/en/live-scores"
ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard"
SOFASCORE_LIVE_URL = "https://www.sofascore.com/api/v1/sport/football/events/live"
STREAMED_LIVE_URL = "https://streamed.pk/api/matches/live"

GOAL_NEXT_DATA_PATTERN = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.DOTALL,
)
ESPN_CLOCK_PATTERN = re.compile(r"(\d+)(?:\+(\d+))?")


class ProviderError(RuntimeError):
    pass


class HttpClient:
    def __init__(self, timeout_seconds: float = 20.0, verify_tls: bool = True):
        self.timeout_seconds = timeout_seconds
        self.verify_tls = verify_tls
        self._ssl_context = None
        if not verify_tls:
            self._ssl_context = ssl._create_unverified_context()

    @classmethod
    def from_env(cls) -> "HttpClient":
        # Allow operators to relax TLS in intercepted corporate networks.
        insecure = os.environ.get("FOOTBALLAPI_INSECURE_TLS", "").strip().lower()
        verify = insecure not in {"1", "true", "yes", "on"}
        timeout = os.environ.get("FOOTBALLAPI_TIMEOUT_SECONDS", "").strip()
        if timeout:
            try:
                return cls(timeout_seconds=float(timeout), verify_tls=verify)
            except ValueError:
                return cls(verify_tls=verify)
        return cls(verify_tls=verify)

    def get_text(self, url: str) -> str:
        request = Request(
            url=url,
            headers={
                "User-Agent": DEFAULT_USER_AGENT,
                "Accept": "*/*",
                "Accept-Encoding": "identity",
            },
        )
        try:
            with urlopen(
                request,
                timeout=self.timeout_seconds,
                context=self._ssl_context,
            ) as response:
                payload = response.read()
        except HTTPError as exc:
            raise ProviderError(f"HTTP {exc.code} while requesting {url}") from exc
        except URLError as exc:
            raise ProviderError(f"Network error while requesting {url}: {exc}") from exc
        except TimeoutError as exc:
            raise ProviderError(f"Timed out while requesting {url}") from exc
        return payload.decode("utf-8", errors="replace")

    def get_json(self, url: str) -> Any:
        text = self.get_text(url)
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise ProviderError(f"Invalid JSON from {url}: {exc}") from exc


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _goal_status(raw: str | None) -> str:
    normalized = (raw or "").strip().upper()
    if normalized == "LIVE":
        return "live"
    if normalized == "RESULT":
        return "finished"
    if normalized == "FIXTURE":
        return "scheduled"
    if normalized == "POSTPONED":
        return "postponed"
    if normalized == "CANCELLED":
        return "cancelled"
    return "unknown"


def _espn_status(status_type: dict[str, Any] | None) -> str:
    status_type = status_type or {}
    name = str(status_type.get("name") or "").upper()
    state = str(status_type.get("state") or "").lower()
    description = str(status_type.get("description") or "").lower()
    short_detail = str(status_type.get("shortDetail") or "").lower()
    haystack = f"{name} {state} {description} {short_detail}"

    if "postponed" in haystack:
        return "postponed"
    if "canceled" in haystack or "cancelled" in haystack:
        return "cancelled"
    if state == "in":
        return "live"
    if state == "post":
        return "finished"
    if state == "pre":
        return "scheduled"
    return "unknown"


def _sofascore_status(status_obj: dict[str, Any] | None) -> str:
    status_obj = status_obj or {}
    code = _to_int(status_obj.get("code"))
    status_type = str(status_obj.get("type") or "").lower()
    description = str(status_obj.get("description") or "").lower()
    haystack = f"{status_type} {description}"

    if "inprogress" in haystack:
        return "live"
    if "finished" in haystack:
        return "finished"
    if "postponed" in haystack:
        return "postponed"
    if "cancel" in haystack:
        return "cancelled"
    if code is not None:
        if code in {1, 2, 3, 4, 5}:
            return "scheduled"
        if code in {6, 7, 8, 9, 10, 31, 32, 33}:
            return "live"
        if code in {100, 120}:
            return "finished"
    return "unknown"


def parse_goal_live_scores_html(html: str) -> list[dict[str, Any]]:
    # Goal embeds structured match data in a Next.js payload script tag.
    match = GOAL_NEXT_DATA_PATTERN.search(html)
    if not match:
        raise ProviderError("Goal payload did not include __NEXT_DATA__")
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise ProviderError(f"Unable to parse Goal __NEXT_DATA__: {exc}") from exc

    page_props = payload.get("props", {}).get("pageProps", {}).get("content", {})
    competitions = page_props.get("liveScores") or []
    rows: list[dict[str, Any]] = []

    for competition_block in competitions:
        competition = (competition_block.get("competition") or {}).get("name")
        for match_row in competition_block.get("matches") or []:
            team_a = match_row.get("teamA") or {}
            team_b = match_row.get("teamB") or {}
            score = match_row.get("score") or {}
            period = match_row.get("period") or {}
            rows.append(
                {
                    "provider": "goal",
                    "provider_match_id": match_row.get("id"),
                    "competition": competition,
                    "home_team": team_a.get("name") or team_a.get("short"),
                    "away_team": team_b.get("name") or team_b.get("short"),
                    "home_score": _to_int(score.get("teamA")),
                    "away_score": _to_int(score.get("teamB")),
                    "status": _goal_status(match_row.get("status")),
                    "raw_status": match_row.get("status"),
                    "period": period.get("type"),
                    "minute": _to_int(period.get("minute")),
                    "extra_minute": _to_int(period.get("extra")),
                    "start_time_utc": match_row.get("startDate"),
                    "last_updated_utc": match_row.get("lastUpdatedAt") or match_row.get("cachedAt"),
                    "venue": (match_row.get("venue") or {}).get("name"),
                    "streamed_watch_url": None,
                    "discrepancies": [],
                }
            )
    return rows


def parse_espn_scoreboard_payload(
    payload: dict[str, Any],
    fetched_at_utc: str | None = None,
) -> list[dict[str, Any]]:
    events = payload.get("events") or []
    rows: list[dict[str, Any]] = []

    for event in events:
        competition = (event.get("competitions") or [{}])[0]
        competitors = competition.get("competitors") or []
        # ESPN usually tags home/away explicitly; fallback to first two rows if missing.
        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if home is None or away is None:
            if len(competitors) >= 2:
                home = competitors[0]
                away = competitors[1]
            else:
                continue

        status_obj = competition.get("status") or {}
        status_type = status_obj.get("type") or {}
        display_clock = str(status_obj.get("displayClock") or "")
        minute_match = ESPN_CLOCK_PATTERN.search(display_clock)
        minute = _to_int(minute_match.group(1)) if minute_match else None
        extra = _to_int(minute_match.group(2)) if minute_match else None
        league = (event.get("league") or {}).get("name")

        rows.append(
            {
                "provider": "espn",
                "provider_match_id": event.get("id"),
                "competition": league or (competition.get("notes") or [{}])[0].get("headline"),
                "home_team": (home.get("team") or {}).get("displayName"),
                "away_team": (away.get("team") or {}).get("displayName"),
                "home_score": _to_int(home.get("score")),
                "away_score": _to_int(away.get("score")),
                "status": _espn_status(status_type),
                "raw_status": status_type.get("name") or status_type.get("description"),
                "period": status_type.get("description"),
                "minute": minute,
                "extra_minute": extra,
                "start_time_utc": competition.get("startDate") or event.get("date"),
                "last_updated_utc": fetched_at_utc,
                "venue": (competition.get("venue") or {}).get("fullName"),
                "streamed_watch_url": None,
                "discrepancies": [],
            }
        )
    return rows


def parse_sofascore_live_payload(
    payload: dict[str, Any],
    fetched_at_utc: str | None = None,
) -> list[dict[str, Any]]:
    events = payload.get("events") or []
    fetch_iso = fetched_at_utc or utc_now_iso()
    rows: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        home_team = (event.get("homeTeam") or {}).get("name")
        away_team = (event.get("awayTeam") or {}).get("name")
        if not home_team or not away_team:
            continue

        competition = (
            (((event.get("tournament") or {}).get("uniqueTournament") or {}).get("name"))
            or ((event.get("tournament") or {}).get("name"))
        )
        home_score = _to_int((event.get("homeScore") or {}).get("current"))
        away_score = _to_int((event.get("awayScore") or {}).get("current"))

        rows.append(
            {
                "provider": "sofascore",
                "provider_match_id": event.get("id"),
                "competition": competition,
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "status": _sofascore_status(event.get("status") or {}),
                "raw_status": (event.get("status") or {}).get("description"),
                "period": (event.get("status") or {}).get("description"),
                # SofaScore minute formats vary across competitions. Keep period text only.
                "minute": None,
                "extra_minute": None,
                "start_time_utc": epoch_seconds_to_iso_utc(_to_int(event.get("startTimestamp"))),
                "last_updated_utc": fetch_iso,
                "venue": None,
                "streamed_watch_url": None,
                "discrepancies": [],
            }
        )
    return rows


def parse_streamed_live_payload(
    payload: list[dict[str, Any]],
    fetched_at_utc: str | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in payload:
        if str(item.get("category") or "").lower() != "football":
            continue
        teams = item.get("teams") or {}
        home_team = (teams.get("home") or {}).get("name")
        away_team = (teams.get("away") or {}).get("name")
        if not home_team or not away_team:
            title = str(item.get("title") or "")
            if " vs " in title:
                left, right = title.split(" vs ", 1)
                home_team = home_team or left.strip()
                away_team = away_team or right.strip()

        match_id = item.get("id")
        rows.append(
            {
                "provider": "streamed",
                "provider_match_id": match_id,
                "competition": None,
                "home_team": home_team,
                "away_team": away_team,
                # Streamed live feed is used for match discovery/watch URLs, not score truth.
                "home_score": None,
                "away_score": None,
                "status": "unknown",
                "raw_status": None,
                "period": None,
                "minute": None,
                "extra_minute": None,
                "start_time_utc": epoch_ms_to_iso_utc(_to_int(item.get("date"))),
                "last_updated_utc": fetched_at_utc or utc_now_iso(),
                "venue": None,
                "streamed_watch_url": (
                    f"https://streamed.pk/watch/{match_id}" if match_id else None
                ),
                "discrepancies": [],
            }
        )
    return rows


class GoalProvider:
    name = "goal"

    def __init__(self, http_client: HttpClient | None = None):
        self.http_client = http_client or HttpClient.from_env()

    def fetch_matches(self) -> list[dict[str, Any]]:
        html = self.http_client.get_text(GOAL_LIVE_SCORES_URL)
        return parse_goal_live_scores_html(html)


class EspnProvider:
    name = "espn"

    def __init__(self, http_client: HttpClient | None = None):
        self.http_client = http_client or HttpClient.from_env()

    def fetch_matches(self) -> list[dict[str, Any]]:
        payload = self.http_client.get_json(ESPN_SCOREBOARD_URL)
        if not isinstance(payload, dict):
            raise ProviderError("ESPN payload is not an object")
        return parse_espn_scoreboard_payload(payload, fetched_at_utc=utc_now_iso())


class SofaScoreProvider:
    name = "sofascore"

    def __init__(self, http_client: HttpClient | None = None):
        self.http_client = http_client or HttpClient.from_env()

    def fetch_matches(self) -> list[dict[str, Any]]:
        payload = self.http_client.get_json(SOFASCORE_LIVE_URL)
        if not isinstance(payload, dict):
            raise ProviderError("SofaScore payload is not an object")
        return parse_sofascore_live_payload(payload, fetched_at_utc=utc_now_iso())


class StreamedProvider:
    name = "streamed"

    def __init__(self, http_client: HttpClient | None = None):
        self.http_client = http_client or HttpClient.from_env()

    def fetch_matches(self) -> list[dict[str, Any]]:
        payload = self.http_client.get_json(STREAMED_LIVE_URL)
        if not isinstance(payload, list):
            raise ProviderError("Streamed payload is not a list")
        return parse_streamed_live_payload(payload, fetched_at_utc=utc_now_iso())
