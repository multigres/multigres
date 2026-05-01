// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command flakyaggregator collects JUnit test artifacts produced by recent
// CI runs on main, counts failure occurrences per test, and posts a Slack
// summary of tests with more than --threshold failures in the last
// --days days.
package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/go-github/v68/github"
)

const (
	defaultDays       = 7
	defaultThreshold  = 3
	defaultWorkers    = 8 // per-run fetch parallelism; well below GitHub's secondary concurrency limits
	httpTimeoutSec    = 60
	maxRunsToInspect  = 2000 // safety cap (covers main + all PR branches over a week)
	maxRetries        = 3
	maxBackoff        = 5 * time.Minute
	testArtifactGlob1 = "-test-logs"
)

// testWorkflowFiles enumerates the workflow files that run gotestsum with
// --rerun-fails and can therefore produce flake signal. Listing runs scoped
// per-file (instead of across the whole repo) is the largest single rate-
// limit saver: it skips lint, docker-build, pgbench, etc. runs that have
// nothing for us. coverage.yml is intentionally excluded because it
// deliberately omits --rerun-fails (a retry would truncate -coverprofile),
// so its event stream contains no fail-then-pass pairs to detect.
//
// Update this list when a new test workflow with --rerun-fails is added.
var testWorkflowFiles = []string{
	"test-short.yml",
	"test-race.yml",
	"test-integration.yml",
}

func main() {
	days := flag.Int("days", defaultDays, "lookback window in days")
	threshold := flag.Int("threshold", defaultThreshold, "report tests with strictly more failures than this")
	workers := flag.Int("workers", defaultWorkers, "per-run fetch parallelism")
	dryRun := flag.Bool("dry-run", false, "print Slack payload to stdout instead of posting")
	flag.Parse()

	if *workers < 1 {
		log.Fatalf("--workers must be >= 1, got %d", *workers)
	}

	repo := os.Getenv("GITHUB_REPOSITORY")
	token := os.Getenv("GITHUB_TOKEN")
	slackURL := os.Getenv("SLACK_WEBHOOK_URL")

	if repo == "" {
		log.Fatal("GITHUB_REPOSITORY is required")
	}
	if token == "" {
		log.Fatal("GITHUB_TOKEN is required")
	}
	if !*dryRun && slackURL == "" {
		log.Fatal("SLACK_WEBHOOK_URL is required (use --dry-run to skip posting)")
	}
	owner, name, ok := strings.Cut(repo, "/")
	if !ok {
		log.Fatalf("GITHUB_REPOSITORY must be owner/name, got %q", repo)
	}

	//nolint:gocritic // main is the program entry point; context.Background() is the canonical root.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	gh := github.NewClient(nil).WithAuthToken(token)

	since := time.Now().UTC().AddDate(0, 0, -*days)
	log.Printf("scanning runs across all branches since %s for repo %s", since.Format(time.RFC3339), repo)
	logRateLimit(ctx, gh, "start")

	runs, err := listFlakeCandidateRuns(ctx, gh, owner, name, since)
	if err != nil {
		log.Fatalf("list workflow runs: %v", err)
	}
	log.Printf("found %d completed-success runs (flake-candidate scope)", len(runs))

	stats := newAggregator()
	processRunsConcurrently(ctx, gh, owner, name, runs, stats, *workers)

	flaky := stats.filter(*threshold)
	payload := formatSlackPayload(flaky, *days, *threshold)
	logRateLimit(ctx, gh, "end")

	if *dryRun {
		buf, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Println(string(buf))
		return
	}

	if err := postSlack(ctx, slackURL, payload); err != nil {
		log.Fatalf("post slack: %v", err)
	}
	log.Printf("posted Slack message with %d flaky test(s)", len(flaky))
}

// processRunsConcurrently fans out per-run artifact fetch+parse work across
// a small worker pool. The aggregator is shared across workers; recordFailure
// is mutex-guarded so concurrent ingest is safe. Per-run errors are logged
// and skipped — they're independent and should not abort the whole run.
func processRunsConcurrently(ctx context.Context, gh *github.Client, owner, repo string, runs []*github.WorkflowRun, stats *aggregator, workers int) {
	runCh := make(chan *github.WorkflowRun)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for run := range runCh {
				processRun(ctx, gh, owner, repo, run, stats)
			}
		})
	}
	for _, run := range runs {
		select {
		case runCh <- run:
		case <-ctx.Done():
			close(runCh)
			wg.Wait()
			return
		}
	}
	close(runCh)
	wg.Wait()
}

func processRun(ctx context.Context, gh *github.Client, owner, repo string, run *github.WorkflowRun, stats *aggregator) {
	artifacts, err := listRunArtifacts(ctx, gh, owner, repo, run.GetID())
	if err != nil {
		log.Printf("WARN: list artifacts for run %d: %v", run.GetID(), err)
		return
	}
	for _, art := range artifacts {
		if !isTestArtifact(art.GetName()) {
			continue
		}
		body, err := downloadArtifact(ctx, gh, owner, repo, art.GetID())
		if err != nil {
			log.Printf("WARN: download artifact %d (%s): %v", art.GetID(), art.GetName(), err)
			continue
		}
		if err := stats.ingestArtifactZip(run, body); err != nil {
			log.Printf("WARN: parse artifact %d (%s): %v", art.GetID(), art.GetName(), err)
		}
	}
}

// --- Rate-limit handling ---

// withRetry runs fn, sleeping and retrying on go-github's rate-limit and
// abuse errors up to maxRetries times. RateLimitError carries the reset
// timestamp; AbuseRateLimitError carries an explicit Retry-After. We cap
// each sleep at maxBackoff so a misbehaving response can't stall the whole
// 30-minute run.
func withRetry[T any](ctx context.Context, label string, fn func() (T, *github.Response, error)) (T, *github.Response, error) {
	var zero T
	for attempt := 0; ; attempt++ {
		out, resp, err := fn()
		if err == nil {
			return out, resp, nil
		}

		var sleep time.Duration
		var rle *github.RateLimitError
		var arle *github.AbuseRateLimitError
		switch {
		case errors.As(err, &rle):
			sleep = time.Until(rle.Rate.Reset.Time)
		case errors.As(err, &arle):
			if arle.RetryAfter != nil {
				sleep = *arle.RetryAfter
			}
		default:
			return zero, resp, err
		}

		if attempt >= maxRetries {
			return zero, resp, fmt.Errorf("%s: rate-limited after %d retries: %w", label, maxRetries, err)
		}
		if sleep <= 0 {
			sleep = 30 * time.Second
		}
		if sleep > maxBackoff {
			sleep = maxBackoff
		}
		log.Printf("rate-limited on %s (attempt %d/%d); sleeping %s", label, attempt+1, maxRetries, sleep)
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return zero, resp, ctx.Err()
		}
	}
}

func logRateLimit(ctx context.Context, gh *github.Client, label string) {
	rl, _, err := gh.RateLimit.Get(ctx)
	if err != nil {
		log.Printf("rate-limit (%s): unknown: %v", label, err)
		return
	}
	core := rl.GetCore()
	log.Printf("rate-limit (%s): %d/%d remaining (resets %s)",
		label, core.Remaining, core.Limit, core.Reset.Format(time.RFC3339))
}

// --- GitHub API (thin wrappers around go-github) ---

// listFlakeCandidateRuns returns recent successful workflow runs across all
// branches, scoped to the workflow files listed in testWorkflowFiles. The
// per-file scoping is the largest single rate-limit saver: it skips lint,
// docker-build, etc. runs that have no JUnit artifacts. The conclusion=
// success filter is what makes it safe to include PR branches — if a test
// failed and the run still concluded success, gotestsum's --rerun-fails
// recovered, which is the textbook flake signal. Tests that fail every
// retry make the run conclude failure and are correctly excluded as
// real-bug noise rather than counted as flakes.
func listFlakeCandidateRuns(ctx context.Context, gh *github.Client, owner, repo string, since time.Time) ([]*github.WorkflowRun, error) {
	var all []*github.WorkflowRun
	for _, file := range testWorkflowFiles {
		opts := &github.ListWorkflowRunsOptions{
			Status:      "success",
			Created:     ">=" + since.Format("2006-01-02"),
			ListOptions: github.ListOptions{PerPage: 100},
		}
		for {
			label := fmt.Sprintf("ListWorkflowRunsByFileName(%s, page=%d)", file, max(opts.Page, 1))
			page, resp, err := withRetry(ctx, label, func() (*github.WorkflowRuns, *github.Response, error) {
				return gh.Actions.ListWorkflowRunsByFileName(ctx, owner, repo, file, opts)
			})
			if err != nil {
				return nil, err
			}
			for _, r := range page.WorkflowRuns {
				if r.GetCreatedAt().Time.Before(since) {
					continue
				}
				all = append(all, r)
			}
			if len(all) >= maxRunsToInspect {
				log.Printf("WARN: hit maxRunsToInspect=%d, truncating", maxRunsToInspect)
				return all, nil
			}
			if resp.NextPage == 0 {
				break
			}
			opts.Page = resp.NextPage
		}
	}
	return all, nil
}

func listRunArtifacts(ctx context.Context, gh *github.Client, owner, repo string, runID int64) ([]*github.Artifact, error) {
	opts := &github.ListOptions{PerPage: 100}
	var all []*github.Artifact
	for {
		label := fmt.Sprintf("ListWorkflowRunArtifacts(run=%d, page=%d)", runID, max(opts.Page, 1))
		page, resp, err := withRetry(ctx, label, func() (*github.ArtifactList, *github.Response, error) {
			return gh.Actions.ListWorkflowRunArtifacts(ctx, owner, repo, runID, opts)
		})
		if err != nil {
			return nil, err
		}
		all = append(all, page.Artifacts...)
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}
	return all, nil
}

func downloadArtifact(ctx context.Context, gh *github.Client, owner, repo string, artifactID int64) ([]byte, error) {
	label := fmt.Sprintf("DownloadArtifact(%d)", artifactID)
	dlURL, _, err := withRetry(ctx, label, func() (*url.URL, *github.Response, error) {
		return gh.Actions.DownloadArtifact(ctx, owner, repo, artifactID, 3)
	})
	if err != nil {
		return nil, err
	}
	// The redirect URL points to GitHub's blob storage; fetching it does NOT
	// count against the REST rate limit, so no withRetry wrapper here.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dlURL.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: httpTimeoutSec * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("download artifact %d: %s: %s", artifactID, resp.Status, body)
	}
	return io.ReadAll(resp.Body)
}

// --- Aggregation ---

// isTestArtifact returns true for artifact names that contain the gotestsum
// JSONL streams we feed into the flake detector. coverage-* is intentionally
// not matched: those workflows omit --rerun-fails (a retry would truncate
// -coverprofile), so their event streams contain no fail-then-pass pairs.
func isTestArtifact(name string) bool {
	return strings.Contains(name, testArtifactGlob1)
}

type testKey struct {
	pkg  string
	name string
}

type testStats struct {
	// runIDs tracks distinct workflow runs in which this test was observed
	// failing — a test that fails twice in one run only counts once.
	runIDs     map[int64]struct{}
	lastSeen   time.Time
	lastRunURL string
}

type aggregator struct {
	mu    sync.Mutex
	tests map[testKey]*testStats
}

func newAggregator() *aggregator {
	return &aggregator{tests: map[testKey]*testStats{}}
}

// recordFailure is safe for concurrent use; ingestArtifactZip is called from
// multiple worker goroutines.
func (a *aggregator) recordFailure(run *github.WorkflowRun, key testKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.tests[key]
	if s == nil {
		s = &testStats{runIDs: map[int64]struct{}{}}
		a.tests[key] = s
	}
	s.runIDs[run.GetID()] = struct{}{}
	if t := run.GetCreatedAt().Time; t.After(s.lastSeen) {
		s.lastSeen = t
		s.lastRunURL = run.GetHTMLURL()
	}
}

type flakyEntry struct {
	Pkg        string
	Name       string
	Failures   int
	LastSeen   time.Time
	LastRunURL string
}

func (a *aggregator) filter(threshold int) []flakyEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []flakyEntry
	for k, s := range a.tests {
		if len(s.runIDs) > threshold {
			out = append(out, flakyEntry{
				Pkg:        k.pkg,
				Name:       k.name,
				Failures:   len(s.runIDs),
				LastSeen:   s.lastSeen,
				LastRunURL: s.lastRunURL,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Failures != out[j].Failures {
			return out[i].Failures > out[j].Failures
		}
		if out[i].Pkg != out[j].Pkg {
			return out[i].Pkg < out[j].Pkg
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func (a *aggregator) ingestArtifactZip(run *github.WorkflowRun, data []byte) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	for _, f := range zr.File {
		if !strings.HasSuffix(f.Name, ".jsonl") {
			continue
		}
		// Defensive: skip any path traversal / nested files we don't expect.
		if strings.Contains(path.Clean(f.Name), "..") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("open %s: %w", f.Name, err)
		}
		flakes, err := parseFlakesFromJSONL(rc)
		rc.Close()
		if err != nil {
			log.Printf("WARN: parse %s: %v", f.Name, err)
			continue
		}
		for _, k := range flakes {
			a.recordFailure(run, k)
		}
	}
	return nil
}

// --- gotestsum JSONL parsing ---

// testEvent matches the subset of `go test -json` event fields we care about.
// The full event format also carries Time, Elapsed, and Output, but for
// flake detection we only need to know which test moved through which
// terminal state in which order.
type testEvent struct {
	Action  string `json:"Action"`
	Package string `json:"Package"`
	Test    string `json:"Test"`
}

// parseFlakesFromJSONL streams a gotestsum --jsonfile output and returns
// the set of tests that were flaky in this run. A test is flaky iff its
// event stream contains both a "fail" and a "pass" action — gotestsum's
// --rerun-fails appends the retry's events, so a recovered flake is
// recognizable as fail-followed-by-pass on the same (package, test) pair.
//
// Package-level events (Test == "") are ignored: they reflect the package's
// rolled-up status and would double-count tests that are already represented
// by their own events.
func parseFlakesFromJSONL(r io.Reader) ([]testKey, error) {
	type state struct{ sawFail, sawPass bool }
	seen := map[testKey]*state{}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024) // some events embed long Output strings
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var ev testEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			continue // tolerate occasional malformed lines rather than abort
		}
		if ev.Test == "" {
			continue
		}
		if ev.Action != "fail" && ev.Action != "pass" {
			continue
		}
		k := testKey{pkg: ev.Package, name: ev.Test}
		s := seen[k]
		if s == nil {
			s = &state{}
			seen[k] = s
		}
		switch ev.Action {
		case "fail":
			s.sawFail = true
		case "pass":
			s.sawPass = true
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan jsonl: %w", err)
	}

	var out []testKey
	for k, s := range seen {
		if s.sawFail && s.sawPass {
			out = append(out, k)
		}
	}
	return out, nil
}

// --- Slack ---

type slackPayload struct {
	Text string `json:"text"`
}

func formatSlackPayload(flaky []flakyEntry, days, threshold int) slackPayload {
	weekOf := time.Now().UTC().Format("2006-01-02")
	var b strings.Builder
	fmt.Fprintf(&b, "*Flaky Test Report — week of %s*\n", weekOf)
	fmt.Fprintf(&b, "Tests that failed-then-recovered in more than %d successful CI runs (`main` + PRs) over the last %d days:\n\n", threshold, days)
	if len(flaky) == 0 {
		b.WriteString("No flaky tests detected this week. :tada:")
		return slackPayload{Text: b.String()}
	}
	for _, f := range flaky {
		fqn := f.Pkg + "." + f.Name
		fmt.Fprintf(&b, "• `%s` — *%d* failures (last seen %s, <%s|run>)\n",
			fqn, f.Failures, f.LastSeen.Format("2006-01-02"), f.LastRunURL)
	}
	return slackPayload{Text: b.String()}
}

func postSlack(ctx context.Context, webhookURL string, p slackPayload) error {
	body, err := json.Marshal(p)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: httpTimeoutSec * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		out, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("slack POST: %s: %s", resp.Status, out)
	}
	return nil
}
