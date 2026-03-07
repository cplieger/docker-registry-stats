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
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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

var (
	healthFile = "/tmp/.healthy"
	dataDir    = "/data"
)

// --- Configuration ---

type config struct {
	// Docker Hub (public repos, owner/repo format e.g. "cplieger/fclones,linuxserver/sonarr")
	DockerHubRepos []repoRef

	// GHCR (public packages, owner/repo format e.g. "cplieger/caddy,home-assistant/home-assistant")
	GHCRRepos []repoRef

	// General
	PollInterval  time.Duration // time between collections (0 = one-shot, collect once then serve)
	RetentionDays int           // auto-delete snapshots older than this (0 = keep forever)
}

// repoRef is an owner/repo pair parsed from env var input.
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
	if len(os.Args) > 1 && os.Args[1] == "health" {
		if _, err := os.Stat(healthFile); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}

	log.SetOutput(os.Stdout)
	cfg := loadConfig()
	logConfig(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer os.Remove(healthFile)

	srv := startServer()

	setHealthy(collect(ctx, &cfg))
	pruneSnapshots(&cfg)

	if cfg.PollInterval > 0 {
		log.Printf("Scheduled mode: interval ~%s (±10%% jitter)", cfg.PollInterval)

		for {
			// Add ±10% jitter to avoid predictable access patterns
			jitter := time.Duration(rand.IntN(int(cfg.PollInterval / 5)))
			delay := cfg.PollInterval - cfg.PollInterval/10 + jitter
			timer := time.NewTimer(delay)

			select {
			case <-ctx.Done():
				timer.Stop()
				log.Println("Shutting down")
				shutdownServer(srv)
				return
			case <-timer.C:
				setHealthy(collect(ctx, &cfg))
				pruneSnapshots(&cfg)
			}
		}
	}

	<-ctx.Done()
	log.Println("Shutting down")
	shutdownServer(srv)
}

func loadConfig() config {
	retentionDays, err := strconv.Atoi(getEnv("RETENTION_DAYS", "90"))
	if err != nil {
		retentionDays = 90
	}
	pollIntervalHours, err := strconv.Atoi(getEnv("POLL_INTERVAL_HOURS", "1"))
	if err != nil {
		pollIntervalHours = 1
	}

	return config{
		DockerHubRepos: parseRepoRefs(getEnv("DOCKERHUB_REPOS", "")),
		GHCRRepos:      parseRepoRefs(getEnv("GHCR_REPOS", "")),
		PollInterval:   time.Duration(pollIntervalHours) * time.Hour,
		RetentionDays:  retentionDays,
	}
}

func logConfig(cfg *config) {
	log.Printf("Docker Hub: %d repos", len(cfg.DockerHubRepos))
	for _, r := range cfg.DockerHubRepos {
		log.Printf("  - %s/%s", r.Owner, r.Repo)
	}
	log.Printf("GHCR: %d packages", len(cfg.GHCRRepos))
	for _, r := range cfg.GHCRRepos {
		log.Printf("  - %s/%s", r.Owner, r.Repo)
	}
	log.Printf("Poll: %s | Retention: %d days", cfg.PollInterval, cfg.RetentionDays)
}

// parseRepoRefs parses a comma-separated list of "owner/repo" pairs.
// Invalid entries (missing slash, unsafe characters) are skipped with a warning.
func parseRepoRefs(s string) []repoRef {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	refs := make([]repoRef, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		owner, repo, ok := strings.Cut(p, "/")
		if !ok || owner == "" || repo == "" {
			log.Printf("WARNING: skipping invalid repo ref (expected owner/repo): %s", p)
			continue
		}
		if !isSafeURLSegment(owner) || !isSafeURLSegment(repo) {
			log.Printf("WARNING: skipping repo ref with unsafe characters: %s", p)
			continue
		}
		refs = append(refs, repoRef{Owner: owner, Repo: repo})
	}
	return refs
}

func isSafeURLSegment(s string) bool {
	return !strings.ContainsAny(s, "/%\\?#@:")
}

// --- Collection ---

func collect(ctx context.Context, cfg *config) bool {
	log.Println("Starting collection")
	client := &http.Client{Timeout: 30 * time.Second}
	snap := snapshot{Timestamp: time.Now().UTC()}
	ok := true

	if len(cfg.DockerHubRepos) > 0 {
		dh := collectDockerHub(ctx, client, cfg)
		if len(dh) == 0 {
			ok = false
		}
		snap.DockerHub = dh
	}

	if len(cfg.GHCRRepos) > 0 {
		gh, healthy := collectGHCR(ctx, client, cfg)
		if !healthy {
			ok = false
		}
		snap.GHCR = gh
	}

	// Don't save empty snapshots — they corrupt daily delta calculations
	if len(snap.DockerHub) == 0 && len(snap.GHCR) == 0 {
		log.Println("ERROR: all collections failed, skipping snapshot save")
		return false
	}

	if err := saveSnapshot(snap); err != nil {
		log.Printf("ERROR: failed to save snapshot: %v", err)
		return false
	}

	log.Printf("Collection complete: docker_hub=%d ghcr=%d", len(snap.DockerHub), len(snap.GHCR))
	return ok
}

func collectDockerHub(ctx context.Context, client *http.Client, cfg *config) []repoStats {
	results := make([]repoStats, 0, len(cfg.DockerHubRepos))

	for _, ref := range cfg.DockerHubRepos {
		repoURL := fmt.Sprintf("https://hub.docker.com/v2/repositories/%s/%s/",
			ref.Owner, ref.Repo)
		repoData, err := doGet(ctx, client, repoURL)
		if err != nil {
			log.Printf("ERROR: Docker Hub fetch failed for %s/%s: %v", ref.Owner, ref.Repo, err)
			continue
		}

		var repoResp struct {
			LastUpdated string `json:"last_updated"`
			PullCount   int64  `json:"pull_count"`
		}
		if err := json.Unmarshal(repoData, &repoResp); err != nil {
			log.Printf("ERROR: Docker Hub parse failed for %s/%s: %v", ref.Owner, ref.Repo, err)
			continue
		}

		tags := collectDockerHubTags(ctx, client, ref)

		results = append(results, repoStats{
			Repo:        ref.Owner + "/" + ref.Repo,
			PullCount:   repoResp.PullCount,
			LastUpdated: repoResp.LastUpdated,
			Tags:        tags,
		})

		log.Printf("Docker Hub: %s/%s — %d pulls, %d tags",
			ref.Owner, ref.Repo, repoResp.PullCount, len(tags))
	}

	return results
}

func collectDockerHubTags(ctx context.Context, client *http.Client, ref repoRef) []tagInfo {
	var tags []tagInfo
	const maxPages = 50

	for page := 1; page <= maxPages; page++ {
		tagsURL := fmt.Sprintf(
			"https://hub.docker.com/v2/repositories/%s/%s/tags/?page_size=100&page=%d",
			ref.Owner, ref.Repo, page)
		data, err := doGet(ctx, client, tagsURL)
		if err != nil {
			log.Printf("ERROR: Docker Hub tags fetch failed for %s/%s page %d: %v",
				ref.Owner, ref.Repo, page, err)
			break
		}

		var resp struct {
			Next    string `json:"next"`
			Results []struct {
				Name        string `json:"name"`
				LastUpdated string `json:"last_updated"`
				Digest      string `json:"digest"`
				Images      []struct {
					Architecture string `json:"architecture"`
					OS           string `json:"os"`
					Digest       string `json:"digest"`
					Size         int64  `json:"size"`
				} `json:"images"`
				FullSize int64 `json:"full_size"`
			} `json:"results"`
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			log.Printf("ERROR: Docker Hub tags parse failed for %s/%s: %v", ref.Owner, ref.Repo, err)
			break
		}

		for _, t := range resp.Results {
			ti := tagInfo{
				Name:        t.Name,
				LastUpdated: t.LastUpdated,
				FullSize:    t.FullSize,
				Digest:      t.Digest,
			}
			for _, img := range t.Images {
				ti.Images = append(ti.Images, imageInfo{
					Architecture: img.Architecture,
					OS:           img.OS,
					Size:         img.Size,
					Digest:       img.Digest,
				})
			}
			tags = append(tags, ti)
		}

		if resp.Next == "" {
			break
		}
	}

	return tags
}

func collectGHCR(ctx context.Context, client *http.Client, cfg *config) ([]ghcrStats, bool) {
	results := make([]ghcrStats, 0, len(cfg.GHCRRepos))
	failures := 0
	parseFailures := 0

	for i, ref := range cfg.GHCRRepos {
		// Space out requests with randomized delay to avoid rate limits
		// and reduce bot-like access patterns.
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

		downloads, err := scrapeGHCRDownloads(ctx, client, ref.Owner, ref.Repo)
		if err != nil {
			log.Printf("WARNING: GHCR scrape failed for %s/%s: %v", ref.Owner, ref.Repo, err)
			failures++
			if errors.Is(err, errHTMLFormatChanged) {
				parseFailures++
			}
		}

		results = append(results, ghcrStats{
			Package:       ref.Owner + "/" + ref.Repo,
			DownloadCount: downloads,
		})

		log.Printf("GHCR: %s/%s — %d downloads", ref.Owner, ref.Repo, downloads)
	}

	if parseFailures == len(cfg.GHCRRepos) {
		log.Println("ERROR: GHCR HTML format may have changed — all download scrapes failed")
		log.Println("ERROR: please open an issue at https://github.com/cplieger/docker-registry-stats/issues so the maintainer can update the scraper")
	}

	healthy := failures < len(cfg.GHCRRepos)
	return results, healthy
}

// errHTMLFormatChanged is a sentinel error indicating the GitHub package page
// HTML structure has changed and download counts can no longer be parsed.
var errHTMLFormatChanged = errors.New("GHCR HTML format changed")

// scrapeGHCRDownloads fetches the public GitHub package page and extracts
// the "Total downloads" count from the HTML.
func scrapeGHCRDownloads(ctx context.Context, client *http.Client, owner, pkg string) (int64, error) {
	pageURL := fmt.Sprintf("https://github.com/users/%s/packages/container/package/%s", owner, pkg)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, http.NoBody)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
	req.Header.Set("Accept", "text/html")
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetch package page: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("package page returned HTTP %d", resp.StatusCode)
	}

	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gr, gzErr := gzip.NewReader(resp.Body)
		if gzErr != nil {
			return 0, fmt.Errorf("gzip reader: %w", gzErr)
		}
		defer gr.Close()
		reader = gr
	}

	const maxBody = 2 << 20
	body, err := io.ReadAll(io.LimitReader(reader, maxBody))
	if err != nil {
		return 0, fmt.Errorf("read body: %w", err)
	}

	return parseGHCRDownloads(string(body))
}

// parseGHCRDownloads extracts the download count from GitHub package page HTML.
// Looks for "Total downloads" text, then extracts the number from the title
// attribute of the next line (e.g. <h3 title="12345">12.3K</h3>).
func parseGHCRDownloads(html string) (int64, error) {
	lines := strings.Split(html, "\n")
	for i, line := range lines {
		if !strings.Contains(line, "Total downloads") {
			continue
		}
		if i+1 >= len(lines) {
			return 0, errHTMLFormatChanged
		}
		nextLine := strings.TrimSpace(lines[i+1])

		titleStart := strings.Index(nextLine, `title="`)
		if titleStart == -1 {
			return 0, errHTMLFormatChanged
		}
		titleStart += len(`title="`)
		titleEnd := strings.Index(nextLine[titleStart:], `"`)
		if titleEnd == -1 {
			return 0, errHTMLFormatChanged
		}

		count, err := strconv.ParseInt(nextLine[titleStart:titleStart+titleEnd], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: parse count: %w", errHTMLFormatChanged, err)
		}
		return count, nil
	}

	return 0, errHTMLFormatChanged
}

// --- Storage ---

func saveSnapshot(snap snapshot) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	filename := snap.Timestamp.Format("2006-01-02") + ".json"
	path := filepath.Join(dataDir, filename)

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write temp snapshot: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename snapshot: %w", err)
	}

	log.Printf("Snapshot saved: %s", path)
	return nil
}

func loadSnapshot(date string) (*snapshot, error) {
	if _, err := time.Parse("2006-01-02", date); err != nil {
		return nil, fmt.Errorf("invalid date format: %s", date)
	}
	path := filepath.Join(dataDir, date+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

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
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		date := strings.TrimSuffix(e.Name(), ".json")
		if _, err := time.Parse("2006-01-02", date); err != nil {
			continue
		}
		dates = append(dates, date)
	}
	slices.Sort(dates)
	return dates, nil
}

func pruneSnapshots(cfg *config) {
	if cfg.RetentionDays <= 0 {
		return
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -cfg.RetentionDays).Format("2006-01-02")
	dates, err := listDates()
	if err != nil {
		log.Printf("ERROR: failed to list dates for pruning: %v", err)
		return
	}
	pruned := 0
	for _, date := range dates {
		if date < cutoff {
			path := filepath.Join(dataDir, date+".json")
			if err := os.Remove(path); err != nil {
				log.Printf("ERROR: failed to prune snapshot %s: %v", date, err)
			} else {
				pruned++
			}
		}
	}
	if pruned > 0 {
		log.Printf("Pruned %d old snapshots (retention: %d days)", pruned, cfg.RetentionDays)
	}
}

// --- HTTP API ---

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
	}

	go func() {
		log.Printf("HTTP server starting on %s", listenAddr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Printf("ERROR: HTTP server: %v", err)
		}
	}()

	return srv
}

func shutdownServer(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("ERROR: HTTP server shutdown: %v", err)
	}
}

func dateToISO(date string) string {
	return date + "T00:00:00Z"
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"status":"ok"}`)
}

func handleSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := resolveSnapshot(r.URL.Query().Get("date"))
	if err != nil {
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
// repoFilter can be empty (no filter), a single "owner/repo", or comma-separated for multi-select.
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

	writeJSON(w, rows)
}

func handlePullsDaily(w http.ResponseWriter, r *http.Request) {
	repoFilter := r.URL.Query()["repo"]
	registryFilter := r.URL.Query().Get("registry")
	dates, err := listDates()
	if err != nil {
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

	writeJSON(w, rows)
}

// --- Helpers ---

func resolveSnapshot(date string) (*snapshot, error) {
	if date != "" {
		return loadSnapshot(date)
	}
	dates, err := listDates()
	if err != nil || len(dates) == 0 {
		return nil, errors.New("no snapshots available")
	}
	return loadSnapshot(dates[len(dates)-1])
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
		log.Printf("ERROR: failed to write JSON response: %v", err)
	}
}

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
