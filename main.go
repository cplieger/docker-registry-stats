package main

// registry-stats collects container image statistics from Docker Hub and GHCR,
// stores daily snapshots as flat JSON files, and serves them via a tiny HTTP API
// for Grafana (Infinity datasource) to query directly.
//
// Docker Hub: pull count, tag count, per-tag metadata via unauthenticated API.
// GHCR: download count scraped from public package pages (no API key needed).
//
// Data is stored as /data/YYYY-MM-DD.json (one file per day, overwritten each poll).
// The HTTP API serves current + historical data for Grafana dashboards.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	listenAddr   = ":9100"
	registryHub  = "dockerhub"
	registryGHCR = "ghcr"
)

// healthFile and dataDir are vars (not consts) so tests can override them.
var (
	healthFile = "/tmp/.healthy"
	dataDir    = "/data"
)

// --- Configuration ---

type config struct {
	// Docker Hub repos: "owner/repo" or "owner/*" (wildcard = all public repos)
	DockerHubRepos []repoRef
	// GHCR packages: "owner/repo" or "owner/*" (wildcard = all public packages)
	GHCRRepos []repoRef
	// General
	PollInterval  time.Duration // time between collections (0 = one-shot, collect once then serve)
	RetentionDays int           // auto-delete snapshots older than this (0 = keep forever)
}

// repoRef is an owner/repo pair parsed from env var input.
// Repo is "*" for wildcard refs that expand at collection time.
type repoRef struct {
	Owner string
	Repo  string
}

// --- Data model ---

type snapshot struct {
	Timestamp time.Time   `json:"timestamp"`
	DockerHub []repoStats `json:"docker_hub,omitempty"`
	GHCR      []ghcrStats `json:"ghcr,omitempty"`
}

type repoStats struct {
	Repo        string    `json:"repo"`
	LastUpdated string    `json:"last_updated"`
	Tags        []tagInfo `json:"tags"`
	PullCount   int64     `json:"pull_count"`
}

type tagInfo struct {
	Name        string      `json:"name"`
	LastUpdated string      `json:"last_updated"`
	Digest      string      `json:"digest"`
	Images      []imageInfo `json:"images,omitempty"`
	FullSize    int64       `json:"full_size"`
}

type imageInfo struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	Digest       string `json:"digest"`
	Size         int64  `json:"size"`
}

type ghcrStats struct {
	Package       string `json:"package"`
	DownloadCount int64  `json:"download_count"`
}

func main() {
	// CLI health probe for Docker healthcheck (distroless has no curl/wget).
	// Checks for a marker file instead of making an HTTP request — no port needed.
	if len(os.Args) > 1 && os.Args[1] == "health" {
		if _, err := os.Stat(healthFile); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}

	cfg := loadConfig()
	logConfig(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer setHealthy(false)

	srv := startServer()

	setHealthy(collect(ctx, &cfg))
	pruneSnapshots(&cfg)

	if cfg.PollInterval > 0 {
		slog.Info("scheduled mode", "interval", cfg.PollInterval, "jitter", "±10%")

		for {
			// Add ±10% jitter to avoid predictable access patterns
			jitter := time.Duration(rand.IntN(int(cfg.PollInterval / 5)))
			delay := cfg.PollInterval - cfg.PollInterval/10 + jitter
			timer := time.NewTimer(delay)

			select {
			case <-ctx.Done():
				timer.Stop()
				slog.Info("shutting down", "cause", context.Cause(ctx))
				shutdownServer(srv)
				return
			case <-timer.C:
				setHealthy(collect(ctx, &cfg))
				pruneSnapshots(&cfg)
			}
		}
	}

	<-ctx.Done()
	slog.Info("shutting down", "cause", context.Cause(ctx))
	shutdownServer(srv)
}

// loadConfig reads configuration from environment variables with sensible defaults.
func loadConfig() config {
	retentionDays, err := strconv.Atoi(getEnv("RETENTION_DAYS", "90"))
	if err != nil || retentionDays < 0 {
		retentionDays = 90
	}
	pollIntervalHours, err := strconv.Atoi(getEnv("POLL_INTERVAL_HOURS", "1"))
	if err != nil || pollIntervalHours < 0 {
		pollIntervalHours = 1
	}

	return config{
		DockerHubRepos: parseRepoRefs(getEnv("DOCKERHUB_REPOS", "")),
		GHCRRepos:      parseRepoRefs(getEnv("GHCR_REPOS", "")),
		PollInterval:   time.Duration(pollIntervalHours) * time.Hour,
		RetentionDays:  retentionDays,
	}
}

// logConfig logs the active configuration at startup (no secrets to redact).
func logConfig(cfg *config) {
	for _, r := range cfg.DockerHubRepos {
		slog.Info("docker hub repo", "ref", r.Owner+"/"+r.Repo)
	}
	for _, r := range cfg.GHCRRepos {
		slog.Info("ghcr package", "ref", r.Owner+"/"+r.Repo)
	}
	slog.Info("configuration loaded",
		"docker_hub_refs", len(cfg.DockerHubRepos),
		"ghcr_refs", len(cfg.GHCRRepos),
		"poll_interval", cfg.PollInterval,
		"retention_days", cfg.RetentionDays)
}

// parseRepoRefs parses a comma-separated list of "owner/repo" or "owner/*" pairs.
// Invalid entries (missing slash, unsafe characters) are skipped with a warning.
func parseRepoRefs(s string) []repoRef {
	if s == "" {
		return nil
	}
	var refs []repoRef
	for p := range strings.SplitSeq(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		owner, repo, ok := strings.Cut(p, "/")
		if !ok || owner == "" || repo == "" {
			slog.Warn("skipping invalid repo ref", "input", p, "expected", "owner/repo or owner/*")
			continue
		}
		if !isSafeURLSegment(owner) || (repo != "*" && !isSafeURLSegment(repo)) {
			slog.Warn("skipping repo ref with unsafe characters", "input", p)
			continue
		}
		refs = append(refs, repoRef{Owner: owner, Repo: repo})
	}
	return refs
}

// isSafeURLSegment returns true if s contains no characters that could
// break URL path construction or enable path traversal.
func isSafeURLSegment(s string) bool {
	return !strings.ContainsAny(s, "/%\\?#@:")
}

// --- Collection ---

// httpClient is the shared HTTP client for all outbound requests.
// Created once at startup for connection pooling.
var httpClient = &http.Client{Timeout: 30 * time.Second}

// collect runs a single collection cycle for all configured registries,
// saves the snapshot, and returns true if all collections succeeded.
func collect(ctx context.Context, cfg *config) bool {
	slog.Info("starting collection")
	snap := snapshot{Timestamp: time.Now().UTC()}
	ok := true

	if len(cfg.DockerHubRepos) > 0 {
		dh := collectDockerHub(ctx, httpClient, cfg.DockerHubRepos)
		if len(dh) == 0 {
			ok = false
		}
		snap.DockerHub = dh
	}

	if len(cfg.GHCRRepos) > 0 {
		gh, healthy := collectGHCR(ctx, httpClient, cfg.GHCRRepos)
		if !healthy {
			ok = false
		}
		snap.GHCR = gh
	}

	// Don't save empty snapshots — they corrupt daily delta calculations
	if len(snap.DockerHub) == 0 && len(snap.GHCR) == 0 {
		if len(cfg.DockerHubRepos) == 0 && len(cfg.GHCRRepos) == 0 {
			slog.Warn("no repos configured, skipping snapshot save")
		} else {
			slog.Error("all collections failed, skipping snapshot save")
		}
		return false
	}

	if err := saveSnapshot(snap); err != nil {
		slog.Error("failed to save snapshot", "error", err)
		return false
	}

	slog.Info("collection complete", "docker_hub", len(snap.DockerHub), "ghcr", len(snap.GHCR))
	return ok
}

// collectDockerHub collects stats for all configured Docker Hub repos.
// Wildcard refs (owner/*) are expanded via the owner listing endpoint, which
// already returns pull_count and last_updated — avoiding per-repo API calls.
// Explicit refs use the per-repo endpoint. Results are deduplicated.
func collectDockerHub(ctx context.Context, client *http.Client, refs []repoRef) []repoStats {
	var results []repoStats
	seen := make(map[string]bool)

	// Collect wildcard owners first (bulk endpoint has pull counts built in)
	for _, ref := range refs {
		if ref.Repo != "*" {
			continue
		}
		repos, err := listDockerHubRepos(ctx, client, ref.Owner)
		if err != nil {
			slog.Error("docker hub listing failed", "owner", ref.Owner, "error", err)
			continue
		}
		for i, r := range repos {
			if seen[r.Repo] {
				continue
			}
			seen[r.Repo] = true
			repos[i].Tags = collectDockerHubTags(ctx, client, r.Repo)
			results = append(results, repos[i])
			slog.Info("docker hub repo collected", "repo", r.Repo, "pulls", r.PullCount, "tags", len(repos[i].Tags))
		}
		slog.Info("docker hub wildcard expanded", "owner", ref.Owner, "repos", len(repos))
	}

	// Collect explicit refs (skip if already covered by a wildcard)
	for _, ref := range refs {
		if ref.Repo == "*" {
			continue
		}
		name := ref.Owner + "/" + ref.Repo
		if seen[name] {
			continue
		}
		seen[name] = true

		repoURL := fmt.Sprintf("https://hub.docker.com/v2/repositories/%s/%s/", ref.Owner, ref.Repo)
		repoData, err := doGet(ctx, client, repoURL)
		if err != nil {
			slog.Error("docker hub fetch failed", "repo", name, "error", err)
			continue
		}

		var repoResp struct {
			LastUpdated string `json:"last_updated"`
			PullCount   int64  `json:"pull_count"`
		}
		if err := json.Unmarshal(repoData, &repoResp); err != nil {
			slog.Error("docker hub parse failed", "repo", name, "error", err)
			continue
		}

		tags := collectDockerHubTags(ctx, client, name)
		results = append(results, repoStats{
			Repo:        name,
			PullCount:   repoResp.PullCount,
			LastUpdated: repoResp.LastUpdated,
			Tags:        tags,
		})
		slog.Info("docker hub repo collected", "repo", name, "pulls", repoResp.PullCount, "tags", len(tags))
	}

	return results
}

// listDockerHubRepos paginates the Docker Hub owner listing endpoint.
// Returns repoStats with Repo, PullCount, and LastUpdated populated (Tags left nil).
func listDockerHubRepos(ctx context.Context, client *http.Client, owner string) ([]repoStats, error) {
	var repos []repoStats
	const maxPages = 10

	for page := 1; page <= maxPages; page++ {
		url := fmt.Sprintf("https://hub.docker.com/v2/repositories/%s/?page_size=100&page=%d", owner, page)
		data, err := doGet(ctx, client, url)
		if err != nil {
			return repos, fmt.Errorf("list repos page %d: %w", page, err)
		}

		var resp struct {
			Next    string `json:"next"`
			Results []struct {
				Name        string `json:"name"`
				LastUpdated string `json:"last_updated"`
				PullCount   int64  `json:"pull_count"`
			} `json:"results"`
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			return repos, fmt.Errorf("parse repo list: %w", err)
		}

		for _, r := range resp.Results {
			repos = append(repos, repoStats{
				Repo:        owner + "/" + r.Name,
				PullCount:   r.PullCount,
				LastUpdated: r.LastUpdated,
			})
		}

		if resp.Next == "" {
			break
		}
	}

	return repos, nil
}

// collectDockerHubTags fetches all tags for a Docker Hub repo (owner/name format).
func collectDockerHubTags(ctx context.Context, client *http.Client, repo string) []tagInfo {
	var tags []tagInfo
	const maxPages = 50

	for page := 1; page <= maxPages; page++ {
		tagsURL := fmt.Sprintf(
			"https://hub.docker.com/v2/repositories/%s/tags/?page_size=100&page=%d",
			repo, page)
		data, err := doGet(ctx, client, tagsURL)
		if err != nil {
			slog.Error("docker hub tags fetch failed", "repo", repo, "page", page, "error", err)
			break
		}

		var resp struct {
			Next    string    `json:"next"`
			Results []tagInfo `json:"results"`
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			slog.Error("docker hub tags parse failed", "repo", repo, "error", err)
			break
		}

		tags = append(tags, resp.Results...)

		if resp.Next == "" {
			break
		}
	}

	return tags
}

// collectGHCR collects download counts for all configured GHCR packages.
// Wildcard refs (owner/*) are expanded by scraping the owner's packages page,
// then each package is scraped individually. Results are deduplicated.
func collectGHCR(ctx context.Context, client *http.Client, refs []repoRef) ([]ghcrStats, bool) {
	var results []ghcrStats
	seen := make(map[string]bool)
	failures := 0
	parseFailures := 0
	total := 0

	// Build the full list of packages to scrape (wildcards first, then explicit)
	var packages []repoRef
	for _, ref := range refs {
		if ref.Repo != "*" {
			continue
		}
		names, err := scrapeGHCRPackageList(ctx, client, ref.Owner)
		if err != nil {
			slog.Error("ghcr package listing failed", "owner", ref.Owner, "error", err)
			continue
		}
		for _, name := range names {
			key := ref.Owner + "/" + name
			if !seen[key] {
				seen[key] = true
				packages = append(packages, repoRef{Owner: ref.Owner, Repo: name})
			}
		}
		slog.Info("ghcr wildcard expanded", "owner", ref.Owner, "packages", len(names))
	}
	for _, ref := range refs {
		if ref.Repo == "*" {
			continue
		}
		key := ref.Owner + "/" + ref.Repo
		if !seen[key] {
			seen[key] = true
			packages = append(packages, ref)
		}
	}

	for i, ref := range packages {
		// Space out requests with randomized delay to avoid rate limits
		if i > 0 {
			delay := 2*time.Second + time.Duration(rand.IntN(3000))*time.Millisecond
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return results, true
			case <-timer.C:
			}
		}

		total++
		downloads, err := scrapeGHCRDownloads(ctx, client, ref.Owner, ref.Repo)
		if err != nil {
			slog.Warn("ghcr scrape failed", "package", ref.Owner+"/"+ref.Repo, "error", err)
			failures++
			if errors.Is(err, errHTMLFormatChanged) {
				parseFailures++
			}
		}

		results = append(results, ghcrStats{
			Package:       ref.Owner + "/" + ref.Repo,
			DownloadCount: downloads,
		})
		slog.Info("ghcr package collected", "package", ref.Owner+"/"+ref.Repo, "downloads", downloads)
	}

	if total > 0 && parseFailures == total {
		slog.Error("ghcr HTML format may have changed, all download scrapes failed")
	}

	healthy := total == 0 || failures < total
	return results, healthy
}

// --- GHCR HTML scraping ---

// errHTMLFormatChanged is a sentinel error indicating the GitHub package page
// HTML structure has changed and download counts can no longer be parsed.
var errHTMLFormatChanged = errors.New("GHCR HTML format changed")

// fetchGitHubHTML fetches a GitHub page with browser-like headers.
// Shared by both the package list scraper and the download count scraper.
func fetchGitHubHTML(ctx context.Context, client *http.Client, pageURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
	req.Header.Set("Accept", "text/html")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch page: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		drainBody(resp.Body)
		return "", fmt.Errorf("page returned HTTP %d", resp.StatusCode)
	}

	const maxBody = 2 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}

	return string(body), nil
}

// scrapeGHCRPackageList fetches the owner's packages page and extracts package names.
func scrapeGHCRPackageList(ctx context.Context, client *http.Client, owner string) ([]string, error) {
	pageURL := fmt.Sprintf("https://github.com/users/%s/packages", owner)
	html, err := fetchGitHubHTML(ctx, client, pageURL)
	if err != nil {
		return nil, err
	}
	return parseGHCRPackageList(html, owner)
}

// parseGHCRPackageList extracts package names from the GitHub user packages page HTML.
// Looks for links matching /users/{owner}/packages/container/package/{name}.
func parseGHCRPackageList(html, owner string) ([]string, error) {
	prefix := fmt.Sprintf("/users/%s/packages/container/package/", owner)
	var packages []string
	seen := make(map[string]bool)

	for line := range strings.SplitSeq(html, "\n") {
		for {
			idx := strings.Index(line, prefix)
			if idx == -1 {
				break
			}
			line = line[idx+len(prefix):]
			end := strings.IndexAny(line, `"'<>`)
			if end == -1 {
				break
			}
			name := line[:end]
			if name != "" && !seen[name] && isSafeURLSegment(name) {
				seen[name] = true
				packages = append(packages, name)
			}
		}
	}

	if len(packages) == 0 {
		return nil, fmt.Errorf("no packages found on %s's packages page (HTML format may have changed)", owner)
	}

	return packages, nil
}

// scrapeGHCRDownloads fetches a single package page and extracts the download count.
func scrapeGHCRDownloads(ctx context.Context, client *http.Client, owner, pkg string) (int64, error) {
	pageURL := fmt.Sprintf("https://github.com/users/%s/packages/container/package/%s", owner, pkg)
	html, err := fetchGitHubHTML(ctx, client, pageURL)
	if err != nil {
		return 0, err
	}
	return parseGHCRDownloads(html)
}

// parseGHCRDownloads extracts the download count from GitHub package page HTML.
// Looks for "Total downloads" text, then extracts the number from the title
// attribute of the next line (e.g. <h3 title="12345">12.3K</h3>).
func parseGHCRDownloads(html string) (int64, error) {
	foundMarker := false
	for line := range strings.SplitSeq(html, "\n") {
		if !foundMarker {
			if strings.Contains(line, "Total downloads") {
				foundMarker = true
			}
			continue
		}

		// Parse the title="N" attribute from the line after "Total downloads"
		trimmed := strings.TrimSpace(line)
		titleStart := strings.Index(trimmed, `title="`)
		if titleStart == -1 {
			return 0, errHTMLFormatChanged
		}
		titleStart += len(`title="`)
		titleEnd := strings.Index(trimmed[titleStart:], `"`)
		if titleEnd == -1 {
			return 0, errHTMLFormatChanged
		}
		count, err := strconv.ParseInt(trimmed[titleStart:titleStart+titleEnd], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: parse count: %w", errHTMLFormatChanged, err)
		}
		if count < 0 {
			return 0, fmt.Errorf("%w: negative download count: %d", errHTMLFormatChanged, count)
		}
		return count, nil
	}

	return 0, errHTMLFormatChanged
}

// --- Storage ---

// saveSnapshot atomically writes a snapshot to the data directory as YYYY-MM-DD.json.
func saveSnapshot(snap snapshot) error {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	filename := snap.Timestamp.Format("2006-01-02") + ".json"
	destPath := filepath.Join(dataDir, filename)

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	// Atomic write: temp file + rename prevents corruption on crash.
	tmp, err := os.CreateTemp(dataDir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write temp snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp snapshot: %w", err)
	}
	if err := os.Rename(tmpName, destPath); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename snapshot: %w", err)
	}

	slog.Info("snapshot saved", "file", filename)
	return nil
}

// loadSnapshot reads and parses a snapshot file for the given date (YYYY-MM-DD).
// Validates the date format to prevent path traversal, and caps file size at 50 MB.
func loadSnapshot(date string) (*snapshot, error) {
	if _, err := time.Parse("2006-01-02", date); err != nil {
		return nil, fmt.Errorf("invalid date format %q: %w", date, err)
	}
	path := filepath.Join(dataDir, date+".json")

	// Single file handle avoids TOCTOU between stat and read.
	const maxSnapshotSize = 50 << 20 // 50 MB
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > maxSnapshotSize {
		return nil, fmt.Errorf("snapshot too large: %d bytes", info.Size())
	}

	data, err := io.ReadAll(io.LimitReader(f, maxSnapshotSize))
	if err != nil {
		return nil, err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// listDates returns all snapshot dates in the data directory, sorted chronologically.
// Skips non-date filenames, directories, and temp files from atomic writes.
func listDates() ([]string, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var dates []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".") {
			continue
		}
		date := strings.TrimSuffix(name, ".json")
		if _, err := time.Parse("2006-01-02", date); err != nil {
			continue
		}
		dates = append(dates, date)
	}
	slices.Sort(dates)
	return dates, nil
}

// pruneSnapshots deletes snapshot files older than the configured retention period.
func pruneSnapshots(cfg *config) {
	if cfg.RetentionDays <= 0 {
		return
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -cfg.RetentionDays).Format("2006-01-02")
	dates, err := listDates()
	if err != nil {
		slog.Error("failed to list dates for pruning", "error", err)
		return
	}
	pruned := 0
	for _, date := range dates {
		if date < cutoff {
			path := filepath.Join(dataDir, date+".json")
			if err := os.Remove(path); err != nil {
				slog.Error("failed to prune snapshot", "date", date, "error", err)
			} else {
				pruned++
			}
		}
	}
	if pruned > 0 {
		slog.Info("pruned old snapshots", "count", pruned, "retention_days", cfg.RetentionDays)
	}
}

// --- HTTP API ---

// startServer creates and starts the HTTP API server in a background goroutine.
func startServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", handleHealth)
	mux.HandleFunc("GET /api/snapshot", handleSnapshot)
	mux.HandleFunc("GET /api/pulls", handlePulls)
	mux.HandleFunc("GET /api/pulls/daily", handlePullsDaily)
	mux.HandleFunc("GET /api/summary", handleSummary)

	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	go func() {
		slog.Info("http server starting", "addr", listenAddr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server error", "error", err)
		}
	}()

	return srv
}

// shutdownServer gracefully shuts down the HTTP server with a 5-second timeout.
func shutdownServer(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("http server shutdown error", "error", err)
	}
}

// dateToISO converts a YYYY-MM-DD date string to ISO 8601 format for Grafana.
func dateToISO(date string) string {
	return date + "T00:00:00Z"
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := os.Stat(healthFile); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"status":"unhealthy"}`)
		return
	}
	fmt.Fprint(w, `{"status":"ok"}`)
}

func handleSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := resolveSnapshot(r.URL.Query().Get("date"))
	if err != nil {
		slog.Warn("snapshot not found", "date", r.URL.Query().Get("date"), "error", err)
		http.Error(w, "snapshot not found", http.StatusNotFound)
		return
	}
	writeJSON(w, snap)
}

// repoPull is a repo name + pull count extracted from a snapshot.
type repoPull struct {
	Repo      string
	PullCount int64
}

// registryIncludes returns which registries to include based on the filter value.
// Unknown values (e.g. Grafana's "$__all") are treated as no filter (include both).
func registryIncludes(filter string) (hub, ghcr bool) {
	filter = strings.TrimPrefix(strings.TrimSuffix(filter, "}"), "{")
	switch filter {
	case registryHub:
		return true, false
	case registryGHCR:
		return false, true
	default:
		return true, true
	}
}

// filteredPulls extracts repo pull data from a snapshot, applying repo and registry filters.
// When the same package appears in both registries, their pull counts are summed.
func filteredPulls(snap *snapshot, repoFilter []string, registryFilter string) []repoPull {
	repos := parseRepoFilter(repoFilter)
	merged := map[string]int64{}
	includeHub, includeGHCR := registryIncludes(registryFilter)
	if includeHub {
		for _, dh := range snap.DockerHub {
			if repos != nil && !repos[dh.Repo] {
				continue
			}
			merged[dh.Repo] += dh.PullCount
		}
	}
	if includeGHCR {
		for _, gh := range snap.GHCR {
			if gh.DownloadCount == 0 {
				continue
			}
			if repos != nil && !repos[gh.Package] {
				continue
			}
			merged[gh.Package] += gh.DownloadCount
		}
	}
	out := make([]repoPull, 0, len(merged))
	for repo, pulls := range merged {
		out = append(out, repoPull{Repo: repo, PullCount: pulls})
	}
	return out
}

// parseRepoFilter builds a repo filter set from query params.
// Handles single value, comma-separated, and repeated params (?repo=a&repo=b).
// Returns nil (no filter) for empty input and Grafana's "$__all" placeholder.
func parseRepoFilter(values []string) map[string]bool {
	m := make(map[string]bool)
	for _, s := range values {
		s = strings.TrimPrefix(strings.TrimSuffix(s, "}"), "{")
		if s == "" || s == "$__all" {
			return nil
		}
		for p := range strings.SplitSeq(s, ",") {
			if p = strings.TrimSpace(p); p != "" {
				m[p] = true
			}
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func handlePulls(w http.ResponseWriter, r *http.Request) {
	repoFilter := r.URL.Query()["repo"]
	registryFilter := r.URL.Query().Get("registry")
	dates, err := listDates()
	if err != nil {
		slog.Error("failed to list dates", "error", err)
		http.Error(w, "failed to list dates", http.StatusInternalServerError)
		return
	}

	type row struct {
		Timestamp string `json:"timestamp"`
		Repo      string `json:"repo"`
		PullCount int64  `json:"pull_count"`
	}
	rows := []row{}

	for _, date := range dates {
		snap, err := loadSnapshot(date)
		if err != nil {
			slog.Warn("skipping corrupt snapshot", "date", date, "error", err)
			continue
		}
		ts := dateToISO(date)
		for _, rp := range filteredPulls(snap, repoFilter, registryFilter) {
			rows = append(rows, row{
				Timestamp: ts,
				Repo:      rp.Repo,
				PullCount: rp.PullCount,
			})
		}
	}

	slices.SortFunc(rows, func(a, b row) int {
		if a.Timestamp != b.Timestamp {
			return strings.Compare(a.Timestamp, b.Timestamp)
		}
		return strings.Compare(a.Repo, b.Repo)
	})

	writeJSON(w, rows)
}

func handlePullsDaily(w http.ResponseWriter, r *http.Request) {
	repoFilter := r.URL.Query()["repo"]
	registryFilter := r.URL.Query().Get("registry")
	dates, err := listDates()
	if err != nil {
		slog.Error("failed to list dates", "error", err)
		http.Error(w, "failed to list dates", http.StatusInternalServerError)
		return
	}

	type dateCount struct {
		date  string
		pulls int64
	}
	byRepo := map[string][]dateCount{}

	for _, date := range dates {
		snap, err := loadSnapshot(date)
		if err != nil {
			slog.Warn("skipping corrupt snapshot", "date", date, "error", err)
			continue
		}
		for _, rp := range filteredPulls(snap, repoFilter, registryFilter) {
			byRepo[rp.Repo] = append(byRepo[rp.Repo], dateCount{date: date, pulls: rp.PullCount})
		}
	}

	type row struct {
		Timestamp  string `json:"timestamp"`
		Repo       string `json:"repo"`
		DailyPulls int64  `json:"daily_pulls"`
	}
	rows := []row{}

	for repo, counts := range byRepo {
		for i, c := range counts {
			var delta int64
			if i > 0 {
				delta = max(0, c.pulls-counts[i-1].pulls)
			}
			rows = append(rows, row{
				Timestamp:  dateToISO(c.date),
				Repo:       repo,
				DailyPulls: delta,
			})
		}
	}

	slices.SortFunc(rows, func(a, b row) int {
		if a.Timestamp != b.Timestamp {
			return strings.Compare(a.Timestamp, b.Timestamp)
		}
		return strings.Compare(a.Repo, b.Repo)
	})

	writeJSON(w, rows)
}

func handleSummary(w http.ResponseWriter, r *http.Request) {
	repoFilter := parseRepoFilter(r.URL.Query()["repo"])
	registryFilter := r.URL.Query().Get("registry")
	snap, err := resolveSnapshot(r.URL.Query().Get("date"))
	if err != nil {
		slog.Warn("snapshot not found", "date", r.URL.Query().Get("date"), "error", err)
		http.Error(w, "snapshot not found", http.StatusNotFound)
		return
	}

	type row struct {
		Registry  string `json:"registry"`
		Name      string `json:"name"`
		PullCount int64  `json:"pull_count"`
		TagCount  int    `json:"tag_count"`
	}
	rows := []row{}

	includeHub, includeGHCR := registryIncludes(registryFilter)
	if includeHub {
		for _, dh := range snap.DockerHub {
			if repoFilter != nil && !repoFilter[dh.Repo] {
				continue
			}
			rows = append(rows, row{
				Registry:  registryHub,
				Name:      dh.Repo,
				PullCount: dh.PullCount,
				TagCount:  len(dh.Tags),
			})
		}
	}
	if includeGHCR {
		for _, gh := range snap.GHCR {
			if repoFilter != nil && !repoFilter[gh.Package] {
				continue
			}
			rows = append(rows, row{
				Registry:  registryGHCR,
				Name:      gh.Package,
				PullCount: gh.DownloadCount,
			})
		}
	}

	slices.SortFunc(rows, func(a, b row) int {
		if a.Registry != b.Registry {
			return strings.Compare(a.Registry, b.Registry)
		}
		return strings.Compare(a.Name, b.Name)
	})

	writeJSON(w, rows)
}

// --- Helpers ---

func resolveSnapshot(date string) (*snapshot, error) {
	if date != "" {
		return loadSnapshot(date)
	}
	dates, err := listDates()
	if err != nil {
		return nil, fmt.Errorf("list dates: %w", err)
	}
	if len(dates) == 0 {
		return nil, errors.New("no snapshots available")
	}
	return loadSnapshot(dates[len(dates)-1])
}

// drainBody reads and discards up to 8 KB of a response body to enable
// HTTP connection reuse.
func drainBody(body io.ReadCloser) {
	if _, err := io.CopyN(io.Discard, body, 8<<10); err != nil && !errors.Is(err, io.EOF) {
		slog.Warn("failed to drain response body", "error", err)
	}
}

func doGet(ctx context.Context, client *http.Client, reqURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		drainBody(resp.Body)
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, reqURL)
	}

	const maxBody = 10 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	return body, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("failed to write JSON response", "error", err)
	}
}

// setHealthy creates or removes the health marker file.
func setHealthy(ok bool) {
	if ok {
		if f, err := os.Create(healthFile); err == nil {
			f.Close()
		}
	} else {
		os.Remove(healthFile)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
