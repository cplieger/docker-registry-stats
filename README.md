# docker-registry-stats

![License: GPL-3.0](https://img.shields.io/badge/license-GPL--3.0-blue)
[![GitHub release](https://img.shields.io/github/v/release/cplieger/docker-registry-stats)](https://github.com/cplieger/docker-registry-stats/releases)
[![Image Size](https://ghcr-badge.egpl.dev/cplieger/registry-stats/size)](https://github.com/cplieger/docker-registry-stats/pkgs/container/registry-stats)
![Platforms](https://img.shields.io/badge/platforms-amd64%20%7C%20arm64-blue)
![base: Distroless](https://img.shields.io/badge/base-Distroless_nonroot-4285F4?logo=google)

Docker Hub and GHCR download tracker with Grafana dashboard

## Overview

Collects download statistics from Docker Hub and GHCR for your public
container images and serves the data via a lightweight HTTP API. Designed
for Grafana dashboards but works with any tool that can query JSON APIs.

Supports both explicit repos (`myuser/myapp`) and owner wildcards
(`myuser/*`) to automatically discover and track all public repos
for an owner. Wildcards are resolved on each poll cycle, so newly
published images are picked up automatically.

Data is stored as one JSON file per day, overwritten on each poll cycle.
Old snapshots are automatically pruned based on a configurable retention
period. Historical time-series data builds up locally as snapshots
accumulate — the registries only expose current totals.

This is a distroless, rootless container — it runs as `nonroot` on
`gcr.io/distroless/static` with no shell or package manager. It has
zero external Go dependencies (stdlib-only).

**Example use case:** You publish Docker images to GHCR and Docker Hub
and want to track download trends over time. Configure your repos
(or use `owner/*` wildcards to auto-discover all of them), point
Grafana at the HTTP API, and get a dashboard showing cumulative
downloads, daily deltas, and per-package breakdowns — no external
analytics service required.

### Limitations

- **Public repositories only.** Docker Hub uses the unauthenticated API.
  GHCR download counts are scraped from public package pages. Private
  repositories and packages are not supported.
- **GHCR scraping is fragile.** Download counts and package listings
  are extracted from GitHub's HTML, not an official API. If GitHub
  changes their page structure, scraping will break. The container
  logs a clear error with a link to open an issue when this happens.
- **No historical backfill.** The registries only expose current totals.
  Time-series data is built locally as snapshots accumulate. If you
  start today, you only have data from today forward.


## Container Registries

This image is published to both GHCR and Docker Hub:

| Registry | Image |
|----------|-------|
| GHCR | `ghcr.io/cplieger/registry-stats` |
| Docker Hub | `docker.io/cplieger/registry-stats` |

```bash
# Pull from GHCR
docker pull ghcr.io/cplieger/registry-stats:latest

# Pull from Docker Hub
docker pull cplieger/registry-stats:latest
```

Both registries receive identical images and tags. Use whichever you prefer.

## Quick Start

```yaml
services:
  registry-stats:
    image: ghcr.io/cplieger/registry-stats:latest
    container_name: registry-stats
    restart: unless-stopped
    user: "1000:1000"  # match your host user
    mem_limit: 64m

    environment:
      TZ: "Europe/Paris"
      DOCKERHUB_REPOS: "owner1/*,owner2/app2"  # owner/repo or owner/* format, comma-separated
      GHCR_REPOS: "owner1/*,owner2/app2"  # owner/package or owner/* format, comma-separated
      POLL_INTERVAL_HOURS: "1"  # 0 = collect once then serve
      RETENTION_DAYS: "90"  # 0 = keep forever

    ports:
      - "9100:9100"

    volumes:
      - "/opt/appdata/registry-stats:/data"  # daily JSON snapshots

    healthcheck:
      test:
        - CMD
        - /registry-stats
        - health
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 15s
```

## Deployment

1. Set `DOCKERHUB_REPOS` to a comma-separated list of Docker Hub
   repositories in `owner/repo` format
   (e.g. `myuser/myapp,myuser/otherapp`). Use `owner/*` to
   automatically track all public repos for an owner
   (e.g. `myuser/*`).
2. Set `GHCR_REPOS` to a comma-separated list of public GHCR packages
   in `owner/package` format. Use `owner/*` to automatically track
   all public packages for an owner. Only public packages are
   supported.
3. You can mix wildcards and explicit refs freely
   (e.g. `myuser/*,otheruser/specific-app`). Duplicates are
   automatically deduplicated — if `myuser/*` discovers `myapp` and
   you also list `myuser/myapp`, it's only collected once.
4. Mount a persistent directory to `/data` for snapshot storage.
5. The container starts collecting immediately and serves the HTTP API
   on port 9100. With the default 1-hour poll interval, you'll have
   your first data point within minutes.
6. For Grafana integration, see the
   [Grafana Integration](#grafana-integration) section below. If you
   use a different dashboard tool, see the
   [API Reference](#api-reference) for endpoint documentation and
   examples.


## Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `TZ` | Container timezone | `Europe/Paris` | No |
| `DOCKERHUB_REPOS` | Comma-separated list of Docker Hub repositories to track. Use `owner/repo` for specific repos or `owner/*` to auto-discover all public repos for an owner (e.g. `myuser/*,otheruser/specific-app`) | `owner1/*,owner2/app2` | No |
| `GHCR_REPOS` | Comma-separated list of public GHCR packages to track. Use `owner/package` for specific packages or `owner/*` to auto-discover all public packages for an owner (e.g. `myuser/*,otheruser/specific-app`) | `owner1/*,owner2/app2` | No |
| `POLL_INTERVAL_HOURS` | Hours between collection cycles. Set to 0 to collect once and then only serve the API (no recurring polls). Wildcards are re-expanded on each cycle, picking up newly published images | `1` | No |
| `RETENTION_DAYS` | Number of days to keep snapshot files. Older snapshots are automatically deleted. Set to 0 to keep all snapshots forever | `90` | No |


## Volumes

| Mount | Description |
|-------|-------------|
| `/data` | Snapshot storage directory. Contains one JSON file per day (e.g. `2025-01-15.json`) within the configured retention period. Size is minimal — typically under 2 MB for 90 days of data. |

## Ports

| Port | Description |
|------|-------------|
| `9100` | HTTP API for Grafana and other consumers |

## API Reference

The HTTP API serves JSON on port 9100. All endpoints return `[]`
(not `null`) for empty results and use ISO 8601 timestamps.

### Filtering

All data endpoints support these query parameters:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `registry` | Filter by registry (`dockerhub` or `ghcr`) | `?registry=dockerhub` |
| `repo` | Filter by package name | `?repo=myuser/myapp` |

Omitting a filter returns all data. Multiple repos can be
comma-separated or passed as repeated parameters.

### Endpoints

#### `GET /api/health`

Returns `{"status":"ok"}`. Used as the Docker healthcheck endpoint
and as the Grafana Infinity datasource health check URL.

#### `GET /api/summary`

Current snapshot overview — one row per package per registry.

```json
[
  {"registry":"dockerhub","name":"myuser/myapp","pull_count":1234,"tag_count":5},
  {"registry":"ghcr","name":"myuser/myapp","pull_count":567,"tag_count":0}
]
```

#### `GET /api/pulls`

Cumulative pull counts over time — one row per package per day.
When both registries track the same package, their counts are
merged (summed) per day.

```json
[
  {"timestamp":"2025-01-15T00:00:00Z","repo":"myuser/myapp","pull_count":1801}
]
```

#### `GET /api/pulls/daily`

Daily download deltas — the difference in pull counts between
consecutive days. Counter resets are clamped to zero. The first
day always shows zero (no previous day to compare).

```json
[
  {"timestamp":"2025-01-16T00:00:00Z","repo":"myuser/myapp","daily_pulls":42}
]
```

#### `GET /api/snapshot`

Raw snapshot for debugging. Returns the full snapshot file including
all Docker Hub tag metadata and GHCR download counts. Accepts
`?date=YYYY-MM-DD` to fetch a specific day (defaults to the most
recent snapshot).

### Using Without Grafana

The API returns standard JSON that any HTTP client, dashboard tool,
or script can consume. Examples:

```bash
# Total downloads across all repos
curl -s http://localhost:9100/api/summary | jq '[.[].pull_count] | add'

# Daily deltas for a specific repo
curl -s 'http://localhost:9100/api/pulls/daily?repo=myuser/myapp' | jq .

# Docker Hub repos only
curl -s 'http://localhost:9100/api/summary?registry=dockerhub' | jq .

# Export raw snapshot for backup or external processing
curl -s http://localhost:9100/api/snapshot > backup.json
```

For periodic reporting, point a cron job at `/api/summary` and pipe
the output to your notification system, spreadsheet, or monitoring
tool.

## Grafana Integration

Registry Stats is designed to work with Grafana's
[Infinity datasource](https://grafana.com/grafana/plugins/yesoreyeram-infinity-datasource/)
plugin. A ready-to-import dashboard template is included in the
repository. If you use a different dashboard tool, see the
[API Reference](#api-reference) section for endpoint documentation
and examples.

### 1. Install the Infinity Plugin

Add the plugin to your Grafana instance. In Docker Compose:

```yaml
environment:
  GF_PLUGINS_PREINSTALL: "yesoreyeram-infinity-datasource"
```

Restart Grafana after adding the plugin.

### 2. Configure the Datasource

In Grafana, go to **Connections → Data sources → Add data source**
and select **Infinity**. Configure:

- **URL:** `http://registry-stats:9100` (adjust if your container
  has a different hostname or port mapping)
- **Health check → Custom health check URL:**
  `http://registry-stats:9100/api/health`

Save and test — the health check should return a green checkmark.

### 3. Import the Dashboard

Import `grafana-dashboard.json` from this repository:

1. In Grafana, go to **Dashboards → Import**
2. Upload the JSON file or paste its contents
3. Select your Infinity datasource when prompted

The dashboard includes:
- **Total Downloads** — sum across all packages and registries
- **Tracked Packages** — number of unique packages being monitored
- **Package Overview** — table with per-package download totals,
  merged across registries
- **Cumulative Downloads** — line chart showing download growth
  over time
- **Daily Download Delta** — bar chart showing new downloads per
  day (requires 2+ days of data)

Both the **Repository** and **Registry** dropdowns are dynamic and
populate automatically from your configured packages. When using
wildcards, newly discovered repos appear in the dropdowns on the
next poll cycle without any dashboard changes.

### 4. Customization

The included dashboard is a starting point. Common customizations:
- Adjust the default time range (default: 30 days)
- Add alert rules on download count thresholds
- Create additional panels using the API endpoints above

## Docker Healthcheck

The container includes a built-in Docker healthcheck. After each
collection cycle, the main process creates or removes a marker file
at `/tmp/.healthy`. The `health` subcommand checks for this file.

**When it becomes unhealthy:**
- All configured Docker Hub repos fail to respond (partial failures
  are tolerated — one successful repo keeps the container healthy)
- All configured GHCR packages fail to scrape
- The snapshot file cannot be written to disk
- Wildcard expansion failures alone do not cause unhealthy status
  if explicit repos still succeed

**When it recovers:**
- The next collection cycle where at least one registry responds
  successfully recreates the marker file. No restart required.

**On startup:** The container collects immediately. If both registries
are unreachable on first boot, it starts unhealthy and recovers on
the next successful poll.

To check health manually:
```bash
docker inspect --format='{{json .State.Health.Log}}' registry-stats | python3 -m json.tool
```

| Type | Command | Meaning |
|------|---------|---------|
| Docker | `/registry-stats health` | Exit 0 = last collection succeeded |


## Dependencies

All dependencies are updated automatically via [Renovate](https://github.com/renovatebot/renovate) and pinned by digest or version for reproducibility.

| Dependency | Version | Source |
|------------|---------|--------|
| golang | `1.26-alpine` | [Go](https://hub.docker.com/_/golang) |
| gcr.io/distroless/static-debian13 | `nonroot` | [Distroless](https://github.com/GoogleContainerTools/distroless) |

## Design Principles

- **Always up to date**: Base images, packages, and libraries are updated automatically via Renovate. Unlike many community Docker images that ship outdated or abandoned dependencies, these images receive continuous updates.
- **Minimal attack surface**: When possible, pure Go apps use `gcr.io/distroless/static:nonroot` (no shell, no package manager, runs as non-root). Apps requiring system packages use Alpine with the minimum necessary privileges.
- **Digest-pinned**: Every `FROM` instruction pins a SHA256 digest. All GitHub Actions are digest-pinned.
- **Multi-platform**: Built for `linux/amd64` and `linux/arm64`.
- **Healthchecks**: Every container includes a Docker healthcheck.
- **Provenance**: Build provenance is attested via GitHub Actions, verifiable with `gh attestation verify`.

## Contributing

Issues, suggestions, and pull requests are welcome.

## Credits

This is an original tool that integrates with [Docker Hub API](https://docs.docker.com/docker-hub/api/latest/). Thanks to the Docker Hub API maintainers for building the platform this tool extends.

## Disclaimer

These images are built with care and follow security best practices, but they are intended for **homelab use**. No guarantees of fitness for production environments. Use at your own risk.

This project was built with AI-assisted tooling using [Claude Opus](https://www.anthropic.com/claude) and [Kiro](https://kiro.dev). The human maintainer defines architecture, supervises implementation, and makes all final decisions.

## License

This project is licensed under the [GNU General Public License v3.0](LICENSE).
