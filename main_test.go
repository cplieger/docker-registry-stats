package main

// main_test.go is intentionally small: the tests below exercise the
// only behavior main.go owns that isn't already covered by a direct
// internal/* package test.
//
// Every other test this file used to host (handler/filter/writeJSON/
// accessLog/shutdown, httpx retry/drain/redirect, DockerHub + GHCR
// mock scraping, config parse + rapid/PBT coverage, collect
// orchestration) was migrated in cycles 1-2 to its new owning package
// alongside the corresponding main.go shim deletion. The legacy
// on-disk storage tests (Save/Load/Prune/Cache) were not migrated to a
// package -- they were deleted with the store when v2 became stateless.

import (
	"bytes"
	"context"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/registry-stats/v2/internal/collect"
	configpkg "github.com/cplieger/registry-stats/v2/internal/config"
	"github.com/cplieger/registry-stats/v2/internal/obs"
	"github.com/cplieger/registry-stats/v2/internal/registry"
)

// TestLogConfig smoke-tests logConfig on a populated *Config: it emits the
// per-repo and config-summary INFO lines and must not panic. The
// "no repos configured" ERROR branch is covered by TestLogConfig_noReposLogsError.
func TestLogConfig(t *testing.T) {
	cfg := &configpkg.Config{
		DockerHubRepos: []registry.RepoRef{{Owner: "a", Repo: "b"}},
		GHCRRepos:      []registry.RepoRef{{Owner: "c", Repo: "d"}},
		PollInterval:   time.Hour,
	}
	logConfig(cfg)
}

// mainFakeSource is a canned collect.Source for driving runCollect in
// isolation from any HTTP path.
type mainFakeSource struct {
	src     registry.ID
	entries []registry.Entry
	healthy bool
}

func (f *mainFakeSource) Source() registry.ID { return f.src }

func (f *mainFakeSource) Collect(_ context.Context, _ []registry.RepoRef) ([]registry.Entry, int, bool) {
	return f.entries, len(f.entries), f.healthy
}

// TestRunCollect_partialSuccessStaysHealthy pins the health-marker contract
// runCollect owns and that diverges from collect.Run's verdict: when one
// registry produces data and the other fails, Run reports the cycle degraded
// (healthy=false) but runCollect must still return true so the marker stays
// healthy ("partial failures stay healthy as long as one repo succeeds").
// Mutates process-global metrics, so no t.Parallel.
func TestRunCollect_partialSuccessStaysHealthy(t *testing.T) {
	dh := &mainFakeSource{
		src:     registry.DockerHub,
		entries: []registry.Entry{{Owner: "o", Repo: "app", Pulls: 1}},
		healthy: true,
	}
	gh := &mainFakeSource{src: registry.GHCR, healthy: false} // no entries, unhealthy
	cfg := &configpkg.Config{
		DockerHubRepos: []registry.RepoRef{{Owner: "o", Repo: "app"}},
		GHCRRepos:      []registry.RepoRef{{Owner: "o", Repo: "pkg"}},
	}
	if got := runCollect(t.Context(), cfg, []collect.Source{dh, gh}); !got {
		t.Error("runCollect() = false, want true (DockerHub produced data; partial success stays healthy)")
	}
}

// TestRunCollect_allEmptyIsUnhealthy pins the other side of the contract: when
// no registry produces an entry, runCollect returns false so the marker flips
// unhealthy. Mutates process-global metrics, so no t.Parallel.
func TestRunCollect_allEmptyIsUnhealthy(t *testing.T) {
	dh := &mainFakeSource{src: registry.DockerHub, healthy: false}
	cfg := &configpkg.Config{DockerHubRepos: []registry.RepoRef{{Owner: "o", Repo: "app"}}}
	if got := runCollect(t.Context(), cfg, []collect.Source{dh}); got {
		t.Error("runCollect() = true, want false (no repo collected)")
	}
}

// TestRunCollect_publishesImageMetrics pins the end-to-end metric contract
// through the composition root: with metrics enabled, one runCollect cycle
// flows each source's flat records into the {registry,owner,repo} gauge
// labels on the real /metrics output (the same boundary the collector
// scrapes, and the label set grafana-dashboard.json queries). Mutates
// process-global metrics, so no t.Parallel.
func TestRunCollect_publishesImageMetrics(t *testing.T) {
	dh := &mainFakeSource{
		src:     registry.DockerHub,
		entries: []registry.Entry{{Owner: "cplieger", Repo: "subflux", Pulls: 1234, TagCount: 2}},
		healthy: true,
	}
	gh := &mainFakeSource{
		src:     registry.GHCR,
		entries: []registry.Entry{{Owner: "cplieger", Repo: "vibekit", Pulls: 56}},
		healthy: true,
	}
	cfg := &configpkg.Config{
		DockerHubRepos: []registry.RepoRef{{Owner: "cplieger", Repo: "subflux"}},
		GHCRRepos:      []registry.RepoRef{{Owner: "cplieger", Repo: "vibekit"}},
		EnableMetrics:  true,
	}
	if got := runCollect(t.Context(), cfg, []collect.Source{dh, gh}); !got {
		t.Fatal("runCollect() = false, want true")
	}

	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	obs.Handler()(w, r)
	body := w.Body.String()

	want := []string{
		`registrystats_image_pulls_total{owner="cplieger",registry="dockerhub",repo="subflux"} 1234`,
		`registrystats_image_tags{owner="cplieger",registry="dockerhub",repo="subflux"} 2`,
		`registrystats_image_pulls_total{owner="cplieger",registry="ghcr",repo="vibekit"} 56`,
	}
	for _, line := range want {
		if !strings.Contains(body, line) {
			t.Errorf("metrics output missing %q\n got:\n%s", line, body)
		}
	}
}

// mainFakeMarker is a minimal healthSignal for asserting the health flag
// recoverAndMarkUnhealthy sets.
type mainFakeMarker struct{ healthy bool }

func (m *mainFakeMarker) Set(h bool)    { m.healthy = h }
func (m *mainFakeMarker) Healthy() bool { return m.healthy }

// saveLogGlobals captures the three globals slog.SetDefault mutates and restores
// them when the test ends; call it before the swap.
//
// SetDefault also aims the log package at the installed handler and skips that
// redirect for slog's own default handler, so reinstalling the previous logger
// cannot undo it; slog's default handler emits through log.Output, so a dead log
// writer silences the package. slog restores first because a non-default previous
// handler re-runs the redirect.
func saveLogGlobals(t *testing.T) {
	t.Helper()
	prevLogger, prevWriter, prevFlags := slog.Default(), log.Writer(), log.Flags()
	t.Cleanup(func() {
		slog.SetDefault(prevLogger)
		log.SetOutput(prevWriter)
		log.SetFlags(prevFlags)
	})
}

// TestSaveLogGlobals_restoresTheLogPackageToo red-checks the two restores saveLogGlobals owns
// beyond slog's own; drop either and this test fails.
func TestSaveLogGlobals_restoresTheLogPackageToo(t *testing.T) {
	prevWriter, prevFlags := log.Writer(), log.Flags()
	t.Cleanup(func() {
		log.SetOutput(prevWriter)
		log.SetFlags(prevFlags)
	})
	// Neither the process default nor what SetDefault installs (a slog
	// handlerWriter and 0), so neither assertion can pass by coincidence.
	log.SetOutput(io.Discard)
	log.SetFlags(log.Lshortfile)

	t.Run("swap", func(t *testing.T) {
		saveLogGlobals(t)
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	})

	if got := log.Writer(); got != io.Discard {
		t.Errorf("log.Writer() = %T, want the writer set before the swap: slog.SetDefault aimed log at its own handler and restoring slog alone leaves it there", got)
	}
	if got := log.Flags(); got != log.Lshortfile {
		t.Errorf("log.Flags() = %d, want %d: slog.SetDefault zeroes them and restoring slog alone leaves them at zero", got, log.Lshortfile)
	}
}

// TestRecoverAndMarkUnhealthy_onPanicMarksUnhealthyAndLogs pins the
// collect-goroutine panic safety net: a recovered panic must flip the marker
// unhealthy AND emit the "collect panicked" ERROR line (per the function
// docstring). Swaps slog.Default to capture, so no t.Parallel.
func TestRecoverAndMarkUnhealthy_onPanicMarksUnhealthyAndLogs(t *testing.T) {
	buf := &bytes.Buffer{}
	saveLogGlobals(t)
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelError})))

	m := &mainFakeMarker{healthy: true}
	func() {
		defer recoverAndMarkUnhealthy(m)
		panic("boom")
	}()

	if m.Healthy() {
		t.Error("recoverAndMarkUnhealthy did not flip marker to unhealthy after a recovered panic")
	}
	if !strings.Contains(buf.String(), "collect panicked") {
		t.Errorf("missing alert log line %q; logs:\n%s", "collect panicked", buf.String())
	}
}

// TestRecoverAndMarkUnhealthy_noPanicLeavesMarker pins the no-op path: with no
// panic in the deferred scope the marker is left untouched.
func TestRecoverAndMarkUnhealthy_noPanicLeavesMarker(t *testing.T) {
	m := &mainFakeMarker{healthy: true}
	func() {
		defer recoverAndMarkUnhealthy(m)
	}()
	if !m.Healthy() {
		t.Error("recoverAndMarkUnhealthy flipped marker with no panic")
	}
}

// TestLogConfig_noReposLogsError drives the no-repos ERROR branch that
// TestLogConfig's comment claims to cover but does not (it passes a populated
// *Config). With zero repos configured, logConfig must emit the operator-facing
// "no repos configured" ERROR that warns the healthcheck will fail after the
// first collect. Swaps slog.Default to capture, so no t.Parallel.
func TestLogConfig_noReposLogsError(t *testing.T) {
	buf := &bytes.Buffer{}
	saveLogGlobals(t)
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelError})))

	logConfig(&configpkg.Config{})

	if !strings.Contains(buf.String(), "no repos configured") {
		t.Errorf("logConfig with no repos did not emit the expected ERROR; logs:\n%s", buf.String())
	}
}

// TestConfiguredSources_preMintsConfiguredSourcesOnly pins the cold-start
// contract the shipped RegistryStatsSourceDegraded rule reads: both per-source
// collect counters carry a zero sample on /metrics before any collect has run,
// so increase() has an earlier sample and a source's FIRST failure fires the
// rule. A source with no configured refs gets no series, because a series would
// claim registry-stats polls that registry. Resets the process-global counters
// so the assertion does not depend on test order, hence no t.Parallel.
func TestConfiguredSources_preMintsConfiguredSourcesOnly(t *testing.T) {
	obs.CollectsTotal.Reset()
	obs.CollectErrors.Reset()

	cfg := &configpkg.Config{DockerHubRepos: []registry.RepoRef{{Owner: "o", Repo: "app"}}}
	sources := []collect.Source{
		&mainFakeSource{src: registry.DockerHub},
		&mainFakeSource{src: registry.GHCR},
	}

	obs.MintCollectSources(configuredSources(cfg, sources))

	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	obs.Handler()(w, r)
	body := w.Body.String()

	for _, line := range []string{
		`registrystats_collects_total{source="dockerhub"} 0`,
		`registrystats_collect_errors_total{source="dockerhub"} 0`,
	} {
		if !strings.Contains(body, line) {
			t.Errorf("pre-mint missing %q\n got:\n%s", line, body)
		}
	}
	if strings.Contains(body, `source="ghcr"`) {
		t.Errorf("unconfigured source ghcr has a series; want none\n got:\n%s", body)
	}
}
