package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- Helpers ---

// seedSnapshot writes a snapshot JSON file into the current dataDir.
func seedSnapshot(t *testing.T, date string, snap snapshot) {
	t.Helper()
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, date+".json"), data, 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
}

// testSnapshot returns a snapshot with both DockerHub and GHCR data.
func testSnapshot(pulls, downloads int64) snapshot {
	return snapshot{
		Timestamp: time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC),
		DockerHub: []repoStats{{
			Repo: "owner/app", PullCount: pulls,
			LastUpdated: "2026-03-06T12:00:00Z",
			Tags: []tagInfo{{
				Name: "latest", FullSize: 1024, Digest: "sha256:abc",
				Images: []imageInfo{
					{Architecture: "amd64", OS: "linux", Size: 512, Digest: "sha256:def"},
				},
			}},
		}},
		GHCR: []ghcrStats{{Package: "owner/pkg", DownloadCount: downloads}},
	}
}

// useTestDataDir points dataDir to a temp directory for the test's duration.
func useTestDataDir(t *testing.T) {
	t.Helper()
	orig := dataDir
	dataDir = t.TempDir()
	t.Cleanup(func() { dataDir = orig })
}

// --- Pure function tests ---

func TestParseRepoRefs(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"", 0},
		{"owner/repo", 1},
		{"a/b,c/d,e/f", 3},
		{" a/b , c/d ", 2},
		{"noslash", 0},
		{"a/b,,c/d", 2},
		{"a/b,bad%owner/repo,c/d", 2},
		{"a/b,owner/bad?repo,c/d", 2},
		{"/norepo", 0},
		{"noowner/", 0},
	}
	for _, tt := range tests {
		got := parseRepoRefs(tt.input)
		if len(got) != tt.want {
			t.Errorf("parseRepoRefs(%q) = %d items, want %d", tt.input, len(got), tt.want)
		}
	}
}

func TestParseRepoRefsWildcard(t *testing.T) {
	refs := parseRepoRefs("owner/repo,owner2/*,owner3/pkg")
	if len(refs) != 3 {
		t.Fatalf("len = %d, want 3", len(refs))
	}
	if refs[1].Owner != "owner2" || refs[1].Repo != "*" {
		t.Errorf("refs[1] = %+v, want owner2/*", refs[1])
	}
}

func TestIsSafeURLSegment(t *testing.T) {
	safe := []string{"cplieger", "fclones-scheduler", "home.assistant", "my_repo"}
	for _, s := range safe {
		if !isSafeURLSegment(s) {
			t.Errorf("isSafeURLSegment(%q) = false, want true", s)
		}
	}
	unsafe := []string{"a/b", "a%20b", "a\\b", "a?b", "a#b", "a@b", "a:b"}
	for _, s := range unsafe {
		if isSafeURLSegment(s) {
			t.Errorf("isSafeURLSegment(%q) = true, want false", s)
		}
	}
}

func TestDateToISO(t *testing.T) {
	if got := dateToISO("2026-03-06"); got != "2026-03-06T00:00:00Z" {
		t.Errorf("dateToISO = %q, want 2026-03-06T00:00:00Z", got)
	}
}

func TestParseGHCRDownloads(t *testing.T) {
	tests := []struct {
		name    string
		html    string
		want    int64
		wantErr error
	}{
		{
			name: "valid large number",
			html: "<span>Total downloads</span>\n<h3 title=\"176432\">176K</h3>",
			want: 176432,
		},
		{
			name: "small number",
			html: "<span>Total downloads</span>\n<h3 title=\"42\">42</h3>",
			want: 42,
		},
		{
			name:    "no Total downloads",
			html:    "<div>nothing</div>",
			wantErr: errHTMLFormatChanged,
		},
		{
			name:    "no title attribute",
			html:    "<span>Total downloads</span>\n<h3>176K</h3>",
			wantErr: errHTMLFormatChanged,
		},
		{
			name:    "non-numeric title",
			html:    "<span>Total downloads</span>\n<h3 title=\"abc\">N/A</h3>",
			wantErr: errHTMLFormatChanged,
		},
		{
			name:    "truncated at Total downloads",
			html:    "<span>Total downloads</span>",
			wantErr: errHTMLFormatChanged,
		},
		{
			name:    "negative download count",
			html:    "<span>Total downloads</span>\n<h3 title=\"-5\">-5</h3>",
			wantErr: errHTMLFormatChanged,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := parseGHCRDownloads(tt.html)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if count != tt.want {
				t.Errorf("count = %d, want %d", count, tt.want)
			}
		})
	}
}

func TestParseGHCRPackageList(t *testing.T) {
	tests := []struct {
		name    string
		html    string
		owner   string
		want    []string
		wantErr bool
	}{
		{
			name: "multiple packages",
			html: `<a href="/users/testowner/packages/container/package/app1">app1</a>
<a href="/users/testowner/packages/container/package/app2">app2</a>
<a href="/users/testowner/packages/container/package/app3">app3</a>`,
			owner: "testowner",
			want:  []string{"app1", "app2", "app3"},
		},
		{
			name: "deduplicates repeated links",
			html: `<a href="/users/owner/packages/container/package/pkg1">pkg1</a>
<a href="/users/owner/packages/container/package/pkg1">pkg1</a>`,
			owner: "owner",
			want:  []string{"pkg1"},
		},
		{
			name:    "no packages found",
			html:    `<div>nothing here</div>`,
			owner:   "owner",
			wantErr: true,
		},
		{
			name:    "ignores other owners",
			html:    `<a href="/users/other/packages/container/package/pkg1">pkg1</a>`,
			owner:   "testowner",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseGHCRPackageList(tt.html, tt.owner)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tt.want))
			}
			for i, w := range tt.want {
				if got[i] != w {
					t.Errorf("got[%d] = %s, want %s", i, got[i], w)
				}
			}
		})
	}
}

func TestSnapshotJSONRoundTrip(t *testing.T) {
	snap := testSnapshot(42, 100)

	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var loaded snapshot
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(loaded.DockerHub) != 1 || loaded.DockerHub[0].PullCount != 42 {
		t.Errorf("DockerHub: got %+v", loaded.DockerHub)
	}
	if len(loaded.GHCR) != 1 || loaded.GHCR[0].DownloadCount != 100 {
		t.Errorf("GHCR: got %+v", loaded.GHCR)
	}
	if len(loaded.DockerHub[0].Tags) != 1 || len(loaded.DockerHub[0].Tags[0].Images) != 1 {
		t.Errorf("Tags/Images not preserved: %+v", loaded.DockerHub[0].Tags)
	}
}

// --- Config tests ---

func TestLoadConfig(t *testing.T) {
	t.Setenv("DOCKERHUB_REPOS", "owner1/app1,owner2/app2")
	t.Setenv("GHCR_REPOS", "gh1/pkg1,gh2/pkg2,gh3/pkg3")
	t.Setenv("POLL_INTERVAL_HOURS", "12")
	t.Setenv("RETENTION_DAYS", "30")

	cfg := loadConfig()

	if len(cfg.DockerHubRepos) != 2 {
		t.Errorf("DockerHubRepos len = %d, want 2", len(cfg.DockerHubRepos))
	}
	if cfg.DockerHubRepos[0].Owner != "owner1" || cfg.DockerHubRepos[0].Repo != "app1" {
		t.Errorf("DockerHubRepos[0] = %+v, want owner1/app1", cfg.DockerHubRepos[0])
	}
	if len(cfg.GHCRRepos) != 3 {
		t.Errorf("GHCRRepos len = %d, want 3", len(cfg.GHCRRepos))
	}
	if cfg.PollInterval != 12*time.Hour {
		t.Errorf("PollInterval = %v, want 12h", cfg.PollInterval)
	}
	if cfg.RetentionDays != 30 {
		t.Errorf("RetentionDays = %d, want 30", cfg.RetentionDays)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	for _, key := range []string{"DOCKERHUB_REPOS", "GHCR_REPOS", "POLL_INTERVAL_HOURS", "RETENTION_DAYS"} {
		t.Setenv(key, "")
	}
	cfg := loadConfig()

	if cfg.PollInterval != 1*time.Hour {
		t.Errorf("PollInterval = %v, want 1h", cfg.PollInterval)
	}
	if cfg.RetentionDays != 90 {
		t.Errorf("RetentionDays = %d, want 90", cfg.RetentionDays)
	}
}

func TestLoadConfigInvalidNumbers(t *testing.T) {
	t.Setenv("POLL_INTERVAL_HOURS", "notanumber")
	t.Setenv("RETENTION_DAYS", "also-bad")
	cfg := loadConfig()

	if cfg.PollInterval != 1*time.Hour {
		t.Errorf("PollInterval = %v, want 1h fallback", cfg.PollInterval)
	}
	if cfg.RetentionDays != 90 {
		t.Errorf("RetentionDays = %d, want 90 fallback", cfg.RetentionDays)
	}
}

func TestLoadConfigWildcard(t *testing.T) {
	t.Setenv("DOCKERHUB_REPOS", "cplieger/*")
	t.Setenv("GHCR_REPOS", "cplieger/*,cplieger/fclones")
	t.Setenv("POLL_INTERVAL_HOURS", "1")
	t.Setenv("RETENTION_DAYS", "90")

	cfg := loadConfig()

	if len(cfg.DockerHubRepos) != 1 || cfg.DockerHubRepos[0].Repo != "*" {
		t.Errorf("DockerHubRepos = %+v, want [cplieger/*]", cfg.DockerHubRepos)
	}
	if len(cfg.GHCRRepos) != 2 {
		t.Errorf("GHCRRepos len = %d, want 2", len(cfg.GHCRRepos))
	}
}

// --- Storage tests ---

func TestSaveAndLoadSnapshot(t *testing.T) {
	useTestDataDir(t)
	snap := testSnapshot(100, 50)

	if err := saveSnapshot(snap); err != nil {
		t.Fatalf("saveSnapshot: %v", err)
	}

	date := snap.Timestamp.Format("2006-01-02")
	loaded, err := loadSnapshot(date)
	if err != nil {
		t.Fatalf("loadSnapshot: %v", err)
	}
	if loaded.DockerHub[0].PullCount != 100 {
		t.Errorf("PullCount = %d, want 100", loaded.DockerHub[0].PullCount)
	}
	if loaded.GHCR[0].DownloadCount != 50 {
		t.Errorf("DownloadCount = %d, want 50", loaded.GHCR[0].DownloadCount)
	}
}

func TestLoadSnapshotInvalidDate(t *testing.T) {
	if _, err := loadSnapshot("not-a-date"); err == nil {
		t.Error("expected error for invalid date format")
	}
}

func TestLoadSnapshotPathTraversal(t *testing.T) {
	_, err := loadSnapshot("../../etc/passwd")
	if err == nil {
		t.Error("expected error for path traversal date")
	}
}

func TestLoadSnapshotPathTraversalEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		date string
	}{
		{"dot-dot-slash", "../2026-03-06"},
		{"encoded dots", "..%2F..%2Fetc%2Fpasswd"},
		{"null byte", "2026-03-06\x00.json"},
		{"backslash traversal", `..\..\etc\passwd`},
		{"valid format but future", "9999-12-31"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadSnapshot(tt.date)
			if err == nil && tt.name != "valid format but future" {
				t.Errorf("expected error for %q", tt.date)
			}
		})
	}
}

func TestListDates(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-04", testSnapshot(10, 5))
	seedSnapshot(t, "2026-03-06", testSnapshot(20, 10))
	seedSnapshot(t, "2026-03-05", testSnapshot(15, 7))

	dates, err := listDates()
	if err != nil {
		t.Fatalf("listDates: %v", err)
	}
	if len(dates) != 3 {
		t.Fatalf("len = %d, want 3", len(dates))
	}
	// Should be sorted
	if dates[0] != "2026-03-04" || dates[1] != "2026-03-05" || dates[2] != "2026-03-06" {
		t.Errorf("dates = %v, want sorted", dates)
	}
}

func TestListDatesEmptyDir(t *testing.T) {
	useTestDataDir(t)
	dates, err := listDates()
	if err != nil {
		t.Fatalf("listDates: %v", err)
	}
	if len(dates) != 0 {
		t.Errorf("len = %d, want 0", len(dates))
	}
}

func TestListDatesIgnoresNonDateFiles(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-06", testSnapshot(10, 5))
	// Write a non-date JSON file that should be ignored
	os.WriteFile(filepath.Join(dataDir, "notes.json"), []byte("{}"), 0o600)
	os.WriteFile(filepath.Join(dataDir, "readme.txt"), []byte("hi"), 0o600)

	dates, err := listDates()
	if err != nil {
		t.Fatalf("listDates: %v", err)
	}
	if len(dates) != 1 || dates[0] != "2026-03-06" {
		t.Errorf("dates = %v, want [2026-03-06]", dates)
	}
}

func TestPruneSnapshots(t *testing.T) {
	useTestDataDir(t)
	old := time.Now().UTC().AddDate(0, 0, -100).Format("2006-01-02")
	recent := time.Now().UTC().Format("2006-01-02")
	seedSnapshot(t, old, testSnapshot(10, 5))
	seedSnapshot(t, recent, testSnapshot(20, 10))

	cfg := &config{RetentionDays: 90}
	pruneSnapshots(cfg)

	dates, _ := listDates()
	if len(dates) != 1 {
		t.Fatalf("expected 1 remaining snapshot, got %d", len(dates))
	}
	if dates[0] != recent {
		t.Errorf("remaining = %s, want %s", dates[0], recent)
	}
}

func TestPruneSnapshotsZeroRetention(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2020-01-01", testSnapshot(10, 5))

	cfg := &config{RetentionDays: 0}
	pruneSnapshots(cfg)

	dates, _ := listDates()
	if len(dates) != 1 {
		t.Error("retention=0 should keep all snapshots")
	}
}

// --- HTTP handler tests ---

func TestHandleHealth(t *testing.T) {
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/health", http.NoBody)
	w := httptest.NewRecorder()
	handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %s, want application/json", ct)
	}
}

func TestHandleSnapshot(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-06", testSnapshot(100, 50))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/snapshot", http.NoBody)
	w := httptest.NewRecorder()
	handleSnapshot(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var snap snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &snap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(snap.DockerHub) != 1 || snap.DockerHub[0].PullCount != 100 {
		t.Errorf("DockerHub = %+v", snap.DockerHub)
	}
}

func TestHandleSnapshotByDate(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-05", testSnapshot(80, 40))
	seedSnapshot(t, "2026-03-06", testSnapshot(100, 50))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/snapshot?date=2026-03-05", http.NoBody)
	w := httptest.NewRecorder()
	handleSnapshot(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var snap snapshot
	json.Unmarshal(w.Body.Bytes(), &snap)
	if snap.DockerHub[0].PullCount != 80 {
		t.Errorf("PullCount = %d, want 80 (from 2026-03-05)", snap.DockerHub[0].PullCount)
	}
}

func TestHandlePulls(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-05", testSnapshot(80, 40))
	seedSnapshot(t, "2026-03-06", testSnapshot(100, 50))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/pulls", http.NoBody)
	w := httptest.NewRecorder()
	handlePulls(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var rows []struct {
		Timestamp string `json:"timestamp"`
		Repo      string `json:"repo"`
		PullCount int64  `json:"pull_count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// 2 dates × (1 dockerhub + 1 ghcr) = 4 rows
	if len(rows) != 4 {
		t.Errorf("len = %d, want 4", len(rows))
	}
}

func TestHandlePullsWithRepoFilter(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-06", testSnapshot(100, 50))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/pulls?repo=owner/app", http.NoBody)
	w := httptest.NewRecorder()
	handlePulls(w, req)

	var rows []struct {
		Repo string `json:"repo"`
	}
	json.Unmarshal(w.Body.Bytes(), &rows)
	if len(rows) != 1 {
		t.Fatalf("len = %d, want 1 (filtered)", len(rows))
	}
	if rows[0].Repo != "owner/app" {
		t.Errorf("repo = %s, want owner/app", rows[0].Repo)
	}
}

func TestHandlePullsDaily(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-05", testSnapshot(80, 40))
	seedSnapshot(t, "2026-03-06", testSnapshot(100, 50))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/pulls/daily", http.NoBody)
	w := httptest.NewRecorder()
	handlePullsDaily(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var rows []struct {
		Repo       string `json:"repo"`
		DailyPulls int64  `json:"daily_pulls"`
	}
	json.Unmarshal(w.Body.Bytes(), &rows)

	// Find the day-2 delta for owner/app
	for _, r := range rows {
		if r.Repo == "owner/app" && r.DailyPulls == 20 {
			return
		}
	}
	t.Error("owner/app day-2 delta of 20 not found")
}

func TestHandlePullsDailyCounterReset(t *testing.T) {
	useTestDataDir(t)
	// Simulate a counter reset (pulls go down — e.g. registry migration)
	seedSnapshot(t, "2026-03-05", testSnapshot(100, 50))
	seedSnapshot(t, "2026-03-06", testSnapshot(10, 5))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/pulls/daily", http.NoBody)
	w := httptest.NewRecorder()
	handlePullsDaily(w, req)

	var rows []struct {
		Repo       string `json:"repo"`
		DailyPulls int64  `json:"daily_pulls"`
	}
	json.Unmarshal(w.Body.Bytes(), &rows)

	for _, r := range rows {
		if r.Repo == "owner/app" && r.DailyPulls != 0 {
			t.Errorf("daily delta on counter reset = %d, want 0 (clamped)", r.DailyPulls)
			return
		}
	}
}

func TestHandleSummary(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-06", testSnapshot(100, 50))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/summary", http.NoBody)
	w := httptest.NewRecorder()
	handleSummary(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var rows []struct {
		Registry  string `json:"registry"`
		Name      string `json:"name"`
		PullCount int64  `json:"pull_count"`
		TagCount  int    `json:"tag_count"`
	}
	json.Unmarshal(w.Body.Bytes(), &rows)
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	if rows[0].Registry != "dockerhub" || rows[0].TagCount != 1 {
		t.Errorf("dockerhub row = %+v", rows[0])
	}
	if rows[1].Registry != "ghcr" || rows[1].PullCount != 50 {
		t.Errorf("ghcr row = %+v", rows[1])
	}
}

// --- HTTP client tests ---

func TestDoGetSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	body, err := doGet(t.Context(), srv.Client(), srv.URL)
	if err != nil {
		t.Fatalf("doGet: %v", err)
	}
	if string(body) != `{"ok":true}` {
		t.Errorf("body = %s", body)
	}
}

func TestDoGetErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	_, err := doGet(t.Context(), srv.Client(), srv.URL)
	if err == nil {
		t.Error("expected error for 404")
	}
}

func TestDoGetBodySizeLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		buf := make([]byte, 1024)
		for range 11 * 1024 {
			w.Write(buf)
		}
	}))
	defer srv.Close()

	body, err := doGet(t.Context(), srv.Client(), srv.URL)
	if err != nil {
		t.Fatalf("doGet: %v", err)
	}
	const maxBody = 10 << 20
	if len(body) > maxBody {
		t.Errorf("body size = %d, exceeds %d", len(body), maxBody)
	}
}

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, map[string]int{"count": 42})

	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %s", ct)
	}
	var result map[string]int
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result["count"] != 42 {
		t.Errorf("count = %d, want 42", result["count"])
	}
}

func TestWriteJSONEmptySlice(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, []string{})

	if got := w.Body.String(); got != "[]\n" {
		t.Errorf("empty slice JSON = %q, want []\n", got)
	}
}

// --- Filter edge case tests ---

func TestRegistryIncludes(t *testing.T) {
	tests := []struct {
		filter   string
		wantHub  bool
		wantGHCR bool
	}{
		{"dockerhub", true, false},
		{"ghcr", false, true},
		{"", true, true},
		{"$__all", true, true},
		{"{dockerhub}", true, false},
		{"{ghcr}", false, true},
		{"unknown", true, true},
		{"{$__all}", true, true},
	}
	for _, tt := range tests {
		t.Run(tt.filter, func(t *testing.T) {
			hub, ghcr := registryIncludes(tt.filter)
			if hub != tt.wantHub || ghcr != tt.wantGHCR {
				t.Errorf("registryIncludes(%q) = (%v, %v), want (%v, %v)",
					tt.filter, hub, ghcr, tt.wantHub, tt.wantGHCR)
			}
		})
	}
}

func TestParseRepoFilter(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   int // -1 means nil (no filter)
	}{
		{"empty", nil, -1},
		{"single value", []string{"owner/app"}, 1},
		{"comma separated", []string{"a/b,c/d"}, 2},
		{"repeated params", []string{"a/b", "c/d"}, 2},
		{"grafana all", []string{"$__all"}, -1},
		{"curly braces", []string{"{a/b,c/d}"}, 2},
		{"empty string", []string{""}, -1},
		{"whitespace in values", []string{" a/b , c/d "}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseRepoFilter(tt.values)
			if tt.want == -1 {
				if got != nil {
					t.Errorf("expected nil filter, got %v", got)
				}
			} else if len(got) != tt.want {
				t.Errorf("len = %d, want %d", len(got), tt.want)
			}
		})
	}
}

func TestHandlePullsExcessiveRepoParams(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-06", testSnapshot(100, 50))

	// Build a request with many repo params — should not panic or OOM
	var b strings.Builder
	b.WriteByte('?')
	for i := range 100 {
		if i > 0 {
			b.WriteByte('&')
		}
		fmt.Fprintf(&b, "repo=fake/repo%d", i)
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/pulls"+b.String(), http.NoBody)
	w := httptest.NewRecorder()
	handlePulls(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	// All repos are filtered out (none match), so we should get an empty array
	var rows []any
	json.Unmarshal(w.Body.Bytes(), &rows)
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for non-matching filters, got %d", len(rows))
	}
}

func TestHandleSummaryWithRegistryFilter(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-06", testSnapshot(100, 50))

	t.Run("dockerhub only", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/summary?registry=dockerhub", http.NoBody)
		w := httptest.NewRecorder()
		handleSummary(w, req)

		var rows []struct{ Registry string }
		json.Unmarshal(w.Body.Bytes(), &rows)
		if len(rows) != 1 || rows[0].Registry != "dockerhub" {
			t.Errorf("expected 1 dockerhub row, got %v", rows)
		}
	})

	t.Run("ghcr only", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/summary?registry=ghcr", http.NoBody)
		w := httptest.NewRecorder()
		handleSummary(w, req)

		var rows []struct{ Registry string }
		json.Unmarshal(w.Body.Bytes(), &rows)
		if len(rows) != 1 || rows[0].Registry != "ghcr" {
			t.Errorf("expected 1 ghcr row, got %v", rows)
		}
	})
}

func TestHandleSnapshotNotFound(t *testing.T) {
	useTestDataDir(t)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/snapshot?date=2099-01-01", http.NoBody)
	w := httptest.NewRecorder()
	handleSnapshot(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestHandlePullsDailySingleDay(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-06", testSnapshot(100, 50))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/pulls/daily", http.NoBody)
	w := httptest.NewRecorder()
	handlePullsDaily(w, req)

	var rows []struct {
		DailyPulls int64 `json:"daily_pulls"`
	}
	json.Unmarshal(w.Body.Bytes(), &rows)

	// First day should always have delta 0 (no previous day to compare)
	for _, r := range rows {
		if r.DailyPulls != 0 {
			t.Errorf("single-day delta = %d, want 0", r.DailyPulls)
		}
	}
}

func TestFilteredPullsZeroDownloadsExcluded(t *testing.T) {
	snap := &snapshot{
		GHCR: []ghcrStats{
			{Package: "owner/active", DownloadCount: 100},
			{Package: "owner/empty", DownloadCount: 0},
		},
	}
	pulls := filteredPulls(snap, nil, "")
	for _, p := range pulls {
		if p.Repo == "owner/empty" {
			t.Error("GHCR packages with 0 downloads should be excluded from pulls")
		}
	}
	if len(pulls) != 1 {
		t.Errorf("expected 1 pull entry, got %d", len(pulls))
	}
}

// --- Collection mock tests ---

func TestCollectDockerHubMock(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v2/repositories/testuser/testrepo/", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"pull_count": 1234, "last_updated": "2026-03-06T12:00:00Z",
		})
	})
	mux.HandleFunc("GET /v2/repositories/testuser/testrepo/tags/", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				{
					"name": "latest", "last_updated": "2026-03-06T12:00:00Z",
					"digest": "sha256:abc123", "full_size": 1024,
					"images": []map[string]any{
						{"architecture": "amd64", "os": "linux", "digest": "sha256:def456", "size": 512},
					},
				},
			},
			"next": "",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Patch the Docker Hub URL by using doGet directly against the mock
	client := srv.Client()
	body, err := doGet(t.Context(), client, srv.URL+"/v2/repositories/testuser/testrepo/")
	if err != nil {
		t.Fatalf("doGet: %v", err)
	}
	var repoResp struct {
		PullCount int64 `json:"pull_count"`
	}
	if err := json.Unmarshal(body, &repoResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if repoResp.PullCount != 1234 {
		t.Errorf("PullCount = %d, want 1234", repoResp.PullCount)
	}
}

func TestScrapeGHCRDownloadsMock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<html>
<span>Total downloads</span>
<h3 title="98765">98.8K</h3>
</html>`))
	}))
	defer srv.Close()

	// Can't call scrapeGHCRDownloads directly (hardcoded URL), but we can
	// test the HTML parsing pipeline end-to-end via the mock response.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, http.NoBody)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	count, err := parseGHCRDownloads(string(buf[:n]))
	if err != nil {
		t.Fatalf("parseGHCRDownloads: %v", err)
	}
	if count != 98765 {
		t.Errorf("count = %d, want 98765", count)
	}
}

// --- Collect integration tests ---

func TestCollectSkipsEmptySnapshot(t *testing.T) {
	useTestDataDir(t)

	// No repos configured — both slices empty, should not save
	cfg := &config{}
	ok := collect(t.Context(), cfg)

	dates, _ := listDates()
	if len(dates) != 0 {
		t.Errorf("expected no snapshots saved, got %d", len(dates))
	}
	// Returns false because the empty snapshot guard fires
	if ok {
		t.Error("expected ok=false when no data collected")
	}
}

// --- Deduplication tests ---

func TestCollectGHCRDedup(t *testing.T) {
	// collectGHCR builds a deduped package list from wildcard + explicit refs.
	// We can't inject mock URLs, but we can verify the dedup logic by checking
	// that parseGHCRPackageList + seen map produces correct results.
	html := `<a href="/users/owner/packages/container/package/app1">app1</a>
<a href="/users/owner/packages/container/package/app2">app2</a>`

	names, err := parseGHCRPackageList(html, "owner")
	if err != nil {
		t.Fatalf("parseGHCRPackageList: %v", err)
	}

	// Simulate the dedup logic from collectGHCR
	seen := make(map[string]bool)
	var packages []repoRef

	// Wildcard expansion
	for _, name := range names {
		key := "owner/" + name
		if !seen[key] {
			seen[key] = true
			packages = append(packages, repoRef{Owner: "owner", Repo: name})
		}
	}

	// Explicit ref that overlaps with wildcard
	key := "owner/app1"
	if !seen[key] {
		seen[key] = true
		packages = append(packages, repoRef{Owner: "owner", Repo: "app1"})
	}

	if len(packages) != 2 {
		t.Errorf("len = %d, want 2 (app1 deduped)", len(packages))
	}
}

// --- Health tracking tests ---

func TestSetHealthy(t *testing.T) {
	orig := healthFile
	healthFile = filepath.Join(t.TempDir(), ".healthy")
	t.Cleanup(func() { healthFile = orig })

	setHealthy(true)
	if _, err := os.Stat(healthFile); err != nil {
		t.Errorf("health file should exist after setHealthy(true): %v", err)
	}

	setHealthy(false)
	if _, err := os.Stat(healthFile); err == nil {
		t.Error("health file should not exist after setHealthy(false)")
	}
}

func TestResolveSnapshotLatest(t *testing.T) {
	useTestDataDir(t)
	seedSnapshot(t, "2026-03-04", testSnapshot(10, 5))
	seedSnapshot(t, "2026-03-06", testSnapshot(30, 15))
	seedSnapshot(t, "2026-03-05", testSnapshot(20, 10))

	snap, err := resolveSnapshot("")
	if err != nil {
		t.Fatalf("resolveSnapshot: %v", err)
	}
	// Should return the latest (2026-03-06)
	if snap.DockerHub[0].PullCount != 30 {
		t.Errorf("PullCount = %d, want 30 (latest)", snap.DockerHub[0].PullCount)
	}
}

func TestResolveSnapshotNoData(t *testing.T) {
	useTestDataDir(t)
	_, err := resolveSnapshot("")
	if err == nil {
		t.Error("expected error when no snapshots exist")
	}
}
