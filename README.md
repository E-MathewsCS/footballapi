# footballapi

Live football scores API designed for website embedding with strict freshness gates.

## Features

- Multi-source live collection:
  - Goal (`https://www.goal.com/en/live-scores`)
  - ESPN (`https://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard`)
  - SofaScore (`https://www.sofascore.com/api/v1/sport/football/events/live`)
  - Streamed (`https://streamed.pk/api/matches/live`) for watch-link enrichment
- Cross-source match merge and score verification.
- Default stale-row filtering for live output.
- Default score-conflict filtering.
- Simple HTTP API with CORS enabled.

## Install

```powershell
py -m pip install -e .[dev]
```

## Run

```powershell
py -m footballapi --host 0.0.0.0 --port 8080 --cache-seconds 10
```

## Endpoints

- `GET /health`
- `GET /api/live-scores`

Example:

```powershell
curl "http://127.0.0.1:8080/api/live-scores?status=live&refresh=1"
```

## Query Params

- `status`: `live`, `scheduled`, `finished`, `postponed`, `cancelled`, `all` (default `live`)
- `source`: `all`, `goal`, `espn`, `sofascore`, `streamed` (default `all`)
- `league`: case-insensitive competition substring filter
- `refresh`: `1|true` force pull
- `include_stale`: include stale live rows (default `false`)
- `include_conflicts`: include unresolved score conflicts (default `false`)

## Freshness / Quality

- Live rows older than `FOOTBALLAPI_MAX_LIVE_STALE_SECONDS` are dropped by default.
- Default `FOOTBALLAPI_MAX_LIVE_STALE_SECONDS` is `180`.
- Score conflicts are dropped by default.
- API response includes `quality` with drop counters.

## Environment

```powershell
$env:FOOTBALLAPI_MAX_LIVE_STALE_SECONDS='180'
$env:FOOTBALLAPI_INSECURE_TLS='1'
```

Use `FOOTBALLAPI_INSECURE_TLS=1` only if your network TLS interception breaks source fetches.

## Tests

```powershell
py -m pytest -q
```
