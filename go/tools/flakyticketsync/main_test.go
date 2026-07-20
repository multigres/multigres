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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testIDA = "aaaaaaaa-c7f9-504e-9aa6-4abef35cda7d"
	testIDB = "bbbbbbbb-c7f9-504e-9aa6-4abef35cda7d"
	testIDC = "cccccccc-c7f9-504e-9aa6-4abef35cda7d"
	testIDD = "dddddddd-c7f9-504e-9aa6-4abef35cda7d"
)

// trunkDescription mimics the body the Trunk ticketing integration writes,
// including its doubled-link markdown quirk.
func trunkDescription(testID string) string {
	return fmt.Sprintf("See all details on the [Trunk Test Detail page|[https://app.trunk.io/acme/flaky-tests/repo/11111111-2222-3333-4444-555555555555/test/%s](<https://app.trunk.io/acme/flaky-tests/repo/11111111-2222-3333-4444-555555555555/test/%s>)]\n\nTransition time: 2026-07-15T11:20:10.476013Z\n\nOwnership: this test is owned by", testID, testID)
}

func TestExtractTestID(t *testing.T) {
	tests := []struct {
		name        string
		description string
		want        string
	}{
		{
			name:        "trunk integration body",
			description: trunkDescription(testIDA),
			want:        testIDA,
		},
		{
			name:        "plain url",
			description: "https://app.trunk.io/acme/flaky-tests/repo/11111111-2222-3333-4444-555555555555/test/" + testIDB,
			want:        testIDB,
		},
		{
			name:        "uppercase hex is normalized",
			description: "app.trunk.io/acme/flaky-tests/repo/11111111-2222-3333-4444-555555555555/test/" + strings.ToUpper(testIDA),
			want:        testIDA,
		},
		{
			name:        "no trunk link",
			description: "hand-written ticket about a flaky test",
			want:        "",
		},
		{
			name:        "empty description",
			description: "",
			want:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractTestID(tt.description); got != tt.want {
				t.Errorf("extractTestID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func mkIssue(identifier string, created time.Time, stateType string, archived bool) linearIssue {
	is := linearIssue{
		ID:         "id-" + identifier,
		Identifier: identifier,
		Title:      "Flaky Test: Test" + identifier,
		CreatedAt:  created,
		State:      linearState{ID: "state-of-" + identifier, Type: stateType},
	}
	if archived {
		at := created.Add(time.Hour)
		is.ArchivedAt = &at
	}
	return is
}

func TestPlanGroup(t *testing.T) {
	t0 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.AddDate(0, 0, 10)
	t2 := t0.AddDate(0, 0, 20)

	tests := []struct {
		name           string
		tickets        []linearIssue
		wantCanonical  string // identifier, "" for none
		wantDuplicates []string
		wantFirstSeen  time.Time
	}{
		{
			name:          "single open ticket",
			tickets:       []linearIssue{mkIssue("ENG-1", t0, "unstarted", false)},
			wantCanonical: "ENG-1",
			wantFirstSeen: t0,
		},
		{
			name: "newer open ticket is the duplicate",
			tickets: []linearIssue{
				mkIssue("ENG-2", t1, "backlog", false),
				mkIssue("ENG-1", t0, "unstarted", false),
			},
			wantCanonical:  "ENG-1",
			wantDuplicates: []string{"ENG-2"},
			wantFirstSeen:  t0,
		},
		{
			name: "human-resolved oldest yields newer canonical",
			tickets: []linearIssue{
				mkIssue("ENG-1", t0, "duplicate", false),
				mkIssue("ENG-2", t1, "unstarted", false),
			},
			wantCanonical: "ENG-2",
			wantFirstSeen: t0,
		},
		{
			name: "archived oldest still anchors first seen",
			tickets: []linearIssue{
				mkIssue("ENG-1", t0, "completed", true),
				mkIssue("ENG-2", t2, "unstarted", false),
			},
			wantCanonical: "ENG-2",
			wantFirstSeen: t0,
		},
		{
			name: "resolved tickets are not re-marked as duplicates",
			tickets: []linearIssue{
				mkIssue("ENG-1", t0, "unstarted", false),
				mkIssue("ENG-2", t1, "completed", false),
			},
			wantCanonical: "ENG-1",
			wantFirstSeen: t0,
		},
		{
			name: "all terminal falls back to oldest active",
			tickets: []linearIssue{
				mkIssue("ENG-1", t0, "completed", false),
				mkIssue("ENG-2", t1, "duplicate", false),
			},
			wantCanonical: "ENG-1",
			wantFirstSeen: t0,
		},
		{
			name: "all archived yields no canonical",
			tickets: []linearIssue{
				mkIssue("ENG-1", t0, "completed", true),
				mkIssue("ENG-2", t1, "duplicate", true),
			},
			wantCanonical: "",
			wantFirstSeen: t0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := planGroup(tt.tickets)
			got := ""
			if p.canonical != nil {
				got = p.canonical.Identifier
			}
			if got != tt.wantCanonical {
				t.Errorf("canonical = %q, want %q", got, tt.wantCanonical)
			}
			var dups []string
			for _, d := range p.duplicates {
				dups = append(dups, d.Identifier)
			}
			if fmt.Sprint(dups) != fmt.Sprint(tt.wantDuplicates) {
				t.Errorf("duplicates = %v, want %v", dups, tt.wantDuplicates)
			}
			if !p.firstSeen.Equal(tt.wantFirstSeen) {
				t.Errorf("firstSeen = %v, want %v", p.firstSeen, tt.wantFirstSeen)
			}
		})
	}
}

func TestDecideLifecycle(t *testing.T) {
	tests := []struct {
		name        string
		status      string
		quarantined bool
		stateType   string
		want        lifecycle
	}{
		{"healthy open ticket closes", "healthy", false, "unstarted", lifecycleClose},
		{"healthy started ticket closes", "healthy", false, "started", lifecycleClose},
		{"healthy triage ticket closes", "healthy", false, "triage", lifecycleClose},
		{"healthy but quarantined stays open", "healthy", true, "unstarted", lifecycleNone},
		{"healthy completed ticket untouched", "healthy", false, "completed", lifecycleNone},
		{"flaky completed ticket reopens", "flaky", false, "completed", lifecycleReopen},
		{"broken completed ticket reopens", "broken", false, "completed", lifecycleReopen},
		{"flaky open ticket untouched", "flaky", false, "unstarted", lifecycleNone},
		{"flaky canceled ticket stays canceled", "flaky", false, "canceled", lifecycleNone},
		{"flaky duplicate ticket stays duplicate", "flaky", false, "duplicate", lifecycleNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &testDetails{Status: testStatus{Value: tt.status}, Quarantined: tt.quarantined}
			if got := decideLifecycle(d, tt.stateType); got != tt.want {
				t.Errorf("decideLifecycle(%s, quarantined=%v, %s) = %v, want %v",
					tt.status, tt.quarantined, tt.stateType, got, tt.want)
			}
		})
	}
}

func TestFormatRate(t *testing.T) {
	tests := []struct {
		fraction float64
		want     string
	}{
		{0.0025, "0.25"},
		{0.25, "25"},
		{0.015, "1.5"},
		{1, "100"},
		{0, "0"},
		{0.00001, "0"}, // below display precision
	}
	for _, tt := range tests {
		if got := formatRate(tt.fraction); got != tt.want {
			t.Errorf("formatRate(%v) = %q, want %q", tt.fraction, got, tt.want)
		}
	}
}

func sampleDetails() *testDetails {
	return &testDetails{
		ID:        testIDA,
		Name:      "TestPreparedStatementTransactionSemantics/multigateway/backend_prepared_connstate_survives_reserved_rollback",
		HTMLURL:   "https://app.trunk.io/acme/flaky-tests/test/" + testIDA,
		Classname: "github.com/multigres/multigres/go/test/endtoend/queryserving",
		Status: testStatus{
			Value:     "flaky",
			Timestamp: time.Date(2026, 6, 30, 10, 0, 0, 0, time.UTC),
		},
		MostCommonFailures: []testFailure{
			{
				Summary:         "prepared_statement_test.go:812: expected connstate to survive rollback",
				OccurrenceCount: 5,
				LastOccurrence:  time.Date(2026, 6, 30, 16, 40, 0, 0, time.UTC),
			},
		},
		FailureRateLast7d:          0.0025,
		PullRequestsImpactedLast7d: 0,
		Quarantined:                true,
	}
}

func TestRenderStatusSection(t *testing.T) {
	firstSeen := time.Date(2026, 6, 28, 9, 0, 0, 0, time.UTC)

	t.Run("quarantined flaky test with failures", func(t *testing.T) {
		got := renderStatusSection(sampleDetails(), firstSeen)
		marker, body, found := strings.Cut(got, "\n")
		if !found {
			t.Fatalf("renderStatusSection() has no marker line:\n%s", got)
		}
		if !regexp.MustCompile(`^<!-- flaky-ticket-sync:begin content:[0-9a-f]{16} -->$`).MatchString(marker) {
			t.Errorf("begin marker = %q, want content-hash form", marker)
		}
		if wantMarker := beginMarker(sectionHash(strings.TrimSuffix(body, managedEnd))); marker != wantMarker {
			t.Errorf("begin marker = %q, want %q (hash of body)", marker, wantMarker)
		}
		got = body
		want := sectionHeading + "\n\n" +
			"Test: `github.com/multigres/multigres/go/test/endtoend/queryserving TestPreparedStatementTransactionSemantics/multigateway/backend_prepared_connstate_survives_reserved_rollback`\n\n" +
			"- **Status:** flaky (since Jun 30, 2026)\n" +
			"- **First seen:** Jun 28, 2026\n" +
			"- **Latest failure:** Jun 30, 2026\n" +
			"- **Severity (last 7 days):** 0.25% failure rate, impacting 0 PRs\n" +
			"- **Quarantined:** yes — test failures are ignored while in this state\n" +
			"\n**Most common failure reason** (5 occurrences):\n\n" +
			"```\nprepared_statement_test.go:812: expected connstate to survive rollback\n```\n" +
			"\n[Trunk test details](https://app.trunk.io/acme/flaky-tests/test/" + testIDA + ")\n" +
			managedEnd
		if got != want {
			t.Errorf("renderStatusSection() mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, want)
		}
	})

	t.Run("healthy test with no failures omits failure lines", func(t *testing.T) {
		d := &testDetails{
			Name:      "TestSomething",
			Classname: "github.com/multigres/multigres/go/services/multipooler",
			Status:    testStatus{Value: "healthy"},
		}
		got := renderStatusSection(d, time.Time{})
		for _, unwanted := range []string{"Latest failure", "Most common failure", "Quarantined", "First seen", "since"} {
			if strings.Contains(got, unwanted) {
				t.Errorf("renderStatusSection() should omit %q, got:\n%s", unwanted, got)
			}
		}
		if !strings.Contains(got, "- **Status:** healthy\n") {
			t.Errorf("renderStatusSection() missing bare status line:\n%s", got)
		}
		if !strings.Contains(got, "impacting 0 PRs") {
			t.Errorf("renderStatusSection() missing severity line:\n%s", got)
		}
	})

	t.Run("multiple failure reasons are all rendered", func(t *testing.T) {
		d := sampleDetails()
		d.MostCommonFailures = append(d.MostCommonFailures, testFailure{
			Summary:         "context deadline exceeded waiting for pooler",
			OccurrenceCount: 1,
			LastOccurrence:  time.Date(2026, 7, 2, 8, 0, 0, 0, time.UTC),
		})
		got := renderStatusSection(d, firstSeen)
		for _, want := range []string{
			"**Most common failure reasons** (2 identified):",
			"Reason #1 (5 occurrences):",
			"prepared_statement_test.go:812: expected connstate to survive rollback",
			"Reason #2 (1 occurrence):",
			"context deadline exceeded waiting for pooler",
			"- **Latest failure:** Jul 2, 2026", // newest across all reasons
		} {
			if !strings.Contains(got, want) {
				t.Errorf("renderStatusSection() missing %q:\n%s", want, got)
			}
		}
	})

	t.Run("code fence in summary is defused", func(t *testing.T) {
		d := sampleDetails()
		d.MostCommonFailures[0].Summary = "before ``` after"
		got := renderStatusSection(d, firstSeen)
		if strings.Contains(got, "before ``` after") {
			t.Errorf("summary fence not sanitized:\n%s", got)
		}
	})
}

func TestReplaceManagedSection(t *testing.T) {
	section := renderStatusSection(sampleDetails(), time.Date(2026, 6, 28, 0, 0, 0, 0, time.UTC))
	original := trunkDescription(testIDA)

	t.Run("appends to descriptions without a section", func(t *testing.T) {
		got := replaceManagedSection(original, section)
		if !strings.HasPrefix(got, original) {
			t.Errorf("original description not preserved:\n%s", got)
		}
		if !strings.HasSuffix(got, section) {
			t.Errorf("section not appended:\n%s", got)
		}
	})

	t.Run("empty description becomes the section", func(t *testing.T) {
		if got := replaceManagedSection("", section); got != section {
			t.Errorf("got:\n%s", got)
		}
	})

	t.Run("existing section is replaced in place", func(t *testing.T) {
		stale := beginMarker("0123456789abcdef") + "\nstale content\n" + managedEnd
		desc := original + "\n\n" + stale + "\n\nhuman note below"
		got := replaceManagedSection(desc, section)
		want := original + "\n\n" + section + "\n\nhuman note below"
		if got != want {
			t.Errorf("got:\n%s\nwant:\n%s", got, want)
		}
	})

	t.Run("old hashless marker is migrated", func(t *testing.T) {
		stale := "<!-- flaky-ticket-sync:begin -->\nstale content\n" + managedEnd
		desc := original + "\n\n" + stale
		got := replaceManagedSection(desc, section)
		want := original + "\n\n" + section
		if got != want {
			t.Errorf("got:\n%s\nwant:\n%s", got, want)
		}
	})

	t.Run("heading anchors replacement when markers were stripped", func(t *testing.T) {
		desc := original + "\n\n" + sectionHeading + "\n\nstale content without markers"
		got := replaceManagedSection(desc, section)
		want := original + "\n\n" + section
		if got != want {
			t.Errorf("got:\n%s\nwant:\n%s", got, want)
		}
	})

	t.Run("idempotent", func(t *testing.T) {
		once := replaceManagedSection(original, section)
		twice := replaceManagedSection(once, section)
		if once != twice {
			t.Errorf("not idempotent:\n--- once ---\n%s\n--- twice ---\n%s", once, twice)
		}
	})
}

func TestManagedSectionHash(t *testing.T) {
	section := renderStatusSection(sampleDetails(), time.Date(2026, 6, 28, 0, 0, 0, 0, time.UTC))

	t.Run("round-trips through the description", func(t *testing.T) {
		desc := replaceManagedSection(trunkDescription(testIDA), section)
		if got, want := managedSectionHash(desc), managedSectionHash(section); got == "" || got != want {
			t.Errorf("managedSectionHash(desc) = %q, want %q", got, want)
		}
	})

	t.Run("immune to Linear markdown normalization", func(t *testing.T) {
		// Linear rewrites the saved markdown body (e.g. link targets gain
		// angle brackets); the hash lives in the marker, so an unchanged
		// section must still compare equal.
		desc := replaceManagedSection(trunkDescription(testIDA), section)
		normalized := strings.ReplaceAll(desc, "](https://", "](<https://")
		if got, want := managedSectionHash(normalized), managedSectionHash(section); got != want {
			t.Errorf("managedSectionHash(normalized) = %q, want %q", got, want)
		}
	})

	t.Run("changed content changes the hash", func(t *testing.T) {
		d := sampleDetails()
		d.FailureRateLast7d = 0.5
		changed := renderStatusSection(d, time.Date(2026, 6, 28, 0, 0, 0, 0, time.UTC))
		if managedSectionHash(changed) == managedSectionHash(section) {
			t.Error("hash did not change with content")
		}
	})

	t.Run("missing or hashless markers yield empty", func(t *testing.T) {
		for _, desc := range []string{
			"",
			"no markers here",
			"<!-- flaky-ticket-sync:begin -->\nold format\n" + managedEnd,
		} {
			if got := managedSectionHash(desc); got != "" {
				t.Errorf("managedSectionHash(%q) = %q, want empty", desc, got)
			}
		}
	})
}

// --- End-to-end sync against fake Linear and Trunk servers ---

type fakeLinear struct {
	t      *testing.T
	issues []linearIssue

	mu        sync.Mutex
	filters   []map[string]any
	comments  []struct{ IssueID, Body string }
	deletions []string
	updates   []struct {
		ID    string
		Input map[string]any
	}
}

func (f *fakeLinear) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			f.t.Errorf("fake linear: decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		respond := func(data map[string]any) {
			if err := json.NewEncoder(w).Encode(map[string]any{"data": data}); err != nil {
				f.t.Errorf("fake linear: encode response: %v", err)
			}
		}
		switch {
		case strings.Contains(req.Query, "query FlakyTickets"):
			filter, _ := req.Variables["filter"].(map[string]any)
			f.mu.Lock()
			f.filters = append(f.filters, filter)
			f.mu.Unlock()
			respond(map[string]any{"issues": map[string]any{
				"nodes":    f.issues,
				"pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
			}})
		case strings.Contains(req.Query, "query TeamStates"):
			respond(map[string]any{"teams": map[string]any{"nodes": []map[string]any{{
				"key": "ENG",
				"states": map[string]any{"nodes": []linearState{
					{ID: "state-backlog", Name: "Backlog", Type: "backlog", Position: 0},
					{ID: "state-todo", Name: "Todo", Type: "unstarted", Position: 2},
					{ID: "state-progress", Name: "In Progress", Type: "started", Position: 3},
					{ID: "state-done", Name: "Done", Type: "completed", Position: 4},
					{ID: "state-canceled", Name: "Canceled", Type: "canceled", Position: 5},
					{ID: "state-dup", Name: "Duplicate", Type: "duplicate", Position: 6},
				}},
			}}}})
		case strings.Contains(req.Query, "mutation IssueUpdate"):
			f.mu.Lock()
			f.updates = append(f.updates, struct {
				ID    string
				Input map[string]any
			}{req.Variables["id"].(string), req.Variables["input"].(map[string]any)})
			f.mu.Unlock()
			respond(map[string]any{"issueUpdate": map[string]any{"success": true}})
		case strings.Contains(req.Query, "mutation IssueDelete"):
			f.mu.Lock()
			f.deletions = append(f.deletions, req.Variables["id"].(string))
			f.mu.Unlock()
			respond(map[string]any{"issueDelete": map[string]any{"success": true}})
		case strings.Contains(req.Query, "mutation CommentCreate"):
			input := req.Variables["input"].(map[string]any)
			f.mu.Lock()
			f.comments = append(f.comments, struct{ IssueID, Body string }{
				input["issueId"].(string), input["body"].(string),
			})
			f.mu.Unlock()
			respond(map[string]any{"commentCreate": map[string]any{"success": true}})
		default:
			f.t.Errorf("fake linear: unexpected query: %s", req.Query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}
}

func (f *fakeLinear) stateSetTo(t *testing.T, issueID string) string {
	t.Helper()
	var state string
	for _, u := range f.updates {
		if u.ID == issueID {
			if s, ok := u.Input["stateId"].(string); ok {
				state = s
			}
		}
	}
	return state
}

func (f *fakeLinear) descriptionSetTo(t *testing.T, issueID string) string {
	t.Helper()
	var desc string
	for _, u := range f.updates {
		if u.ID == issueID {
			if d, ok := u.Input["description"].(string); ok {
				desc = d
			}
		}
	}
	return desc
}

func fakeTrunkHandler(t *testing.T, details map[string]*testDetails) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/flaky-tests/get-test-details" {
			t.Errorf("fake trunk: unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("x-api-token") != "trunk-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var req testDetailsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("fake trunk: decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		d, ok := details[req.TestID]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"test": d}); err != nil {
			t.Errorf("fake trunk: encode response: %v", err)
		}
	}
}

func TestRunEndToEnd(t *testing.T) {
	t0 := time.Date(2026, 6, 28, 9, 0, 0, 0, time.UTC)

	withDescription := func(is linearIssue, testID string) linearIssue {
		is.Description = trunkDescription(testID)
		return is
	}

	fake := &fakeLinear{t: t, issues: []linearIssue{
		// Test A: two open tickets → newer is a duplicate; flaky+quarantined
		// → canonical gets a status section, stays open.
		withDescription(mkIssue("ENG-1", t0, "unstarted", false), testIDA),
		withDescription(mkIssue("ENG-2", t0.AddDate(0, 0, 2), "backlog", false), testIDA),
		// Test B: healthy and unquarantined → ticket closes.
		withDescription(mkIssue("ENG-3", t0, "started", false), testIDB),
		// Test C: Done ticket but the test is flaky again → reopens.
		withDescription(mkIssue("ENG-4", t0, "completed", false), testIDC),
		// No Trunk link → left alone.
		mkIssue("ENG-5", t0, "unstarted", false),
	}}
	// Test D: already synced, and Linear has since normalized the stored
	// markdown. The marker hash still matches → no rewrite.
	synced := withDescription(mkIssue("ENG-6", t0, "unstarted", false), testIDD)
	synced.Description = replaceManagedSection(synced.Description, renderStatusSection(sampleDetails(), t0))
	synced.Description = strings.ReplaceAll(synced.Description, "](https://", "](<https://")
	fake.issues = append(fake.issues, synced)
	linearSrv := httptest.NewServer(fake.handler())
	defer linearSrv.Close()

	trunkSrv := httptest.NewServer(fakeTrunkHandler(t, map[string]*testDetails{
		testIDA: sampleDetails(),
		testIDD: sampleDetails(),
		testIDB: {
			Name:      "TestHealthyAgain",
			Classname: "github.com/multigres/multigres/go/services/multipooler",
			Status:    testStatus{Value: "healthy", Timestamp: time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)},
		},
		testIDC: {
			Name:              "TestRegressed",
			Classname:         "github.com/multigres/multigres/go/services/multiorch",
			Status:            testStatus{Value: "flaky", Timestamp: time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)},
			FailureRateLast7d: 0.04,
		},
	}))
	defer trunkSrv.Close()

	cfg := config{
		teamKey:        "ENG",
		label:          "flaky-test",
		project:        "Flaky Tests",
		orgSlug:        "acme",
		repoHost:       "github.com",
		repoOwner:      "multigres",
		repoName:       "multigres",
		linearToken:    "linear-token",
		trunkToken:     "trunk-token",
		linearEndpoint: linearSrv.URL,
		trunkEndpoint:  trunkSrv.URL,
	}
	if err := run(context.Background(), cfg); err != nil {
		t.Fatalf("run() = %v", err)
	}

	// The issue query is scoped to team, label, and project.
	if len(fake.filters) == 0 {
		t.Fatal("no issue-list filter recorded")
	}
	filterJSON, _ := json.Marshal(fake.filters[0])
	want := `{"labels":{"name":{"eq":"flaky-test"}},"project":{"name":{"eq":"Flaky Tests"}},"team":{"key":{"eq":"ENG"}}}`
	if string(filterJSON) != want {
		t.Errorf("issue filter = %s, want %s", filterJSON, want)
	}

	// ENG-2 is deleted as a duplicate of ENG-1; no comment or state change.
	if fmt.Sprint(fake.deletions) != fmt.Sprint([]string{"id-ENG-2"}) {
		t.Errorf("deletions = %v, want [id-ENG-2]", fake.deletions)
	}
	if got := fake.stateSetTo(t, "id-ENG-2"); got != "" {
		t.Errorf("ENG-2 state changed to %q, want no change", got)
	}
	for _, c := range fake.comments {
		if c.IssueID == "id-ENG-2" {
			t.Errorf("ENG-2 unexpectedly commented on: %s", c.Body)
		}
	}

	// ENG-1 gets the status section with first-seen anchored at the oldest
	// ticket, and no state change (flaky tests stay open).
	desc := fake.descriptionSetTo(t, "id-ENG-1")
	for _, want := range []string{
		managedBeginPrefix,
		"- **Status:** flaky (since Jun 30, 2026)",
		"- **First seen:** Jun 28, 2026",
		"- **Severity (last 7 days):** 0.25% failure rate, impacting 0 PRs",
		"- **Quarantined:** yes",
	} {
		if !strings.Contains(desc, want) {
			t.Errorf("ENG-1 description missing %q:\n%s", want, desc)
		}
	}
	if !strings.HasPrefix(desc, trunkDescription(testIDA)) {
		t.Errorf("ENG-1 original description not preserved:\n%s", desc)
	}
	if got := fake.stateSetTo(t, "id-ENG-1"); got != "" {
		t.Errorf("ENG-1 state changed to %q, want no change", got)
	}

	// ENG-3 closes as healthy; ENG-4 reopens as flaky.
	if got := fake.stateSetTo(t, "id-ENG-3"); got != "state-done" {
		t.Errorf("ENG-3 state = %q, want state-done", got)
	}
	if got := fake.stateSetTo(t, "id-ENG-4"); got != "state-todo" {
		t.Errorf("ENG-4 state = %q, want state-todo", got)
	}

	// ENG-5 has no Trunk link; ENG-6 is already synced (normalized body,
	// matching marker hash). Both must be untouched.
	for _, u := range fake.updates {
		if u.ID == "id-ENG-5" || u.ID == "id-ENG-6" {
			t.Errorf("%s unexpectedly updated: %+v", u.ID, u.Input)
		}
	}
	for _, c := range fake.comments {
		if c.IssueID == "id-ENG-5" || c.IssueID == "id-ENG-6" {
			t.Errorf("%s unexpectedly commented on: %s", c.IssueID, c.Body)
		}
	}
}

func TestRunDryRunPerformsNoMutations(t *testing.T) {
	t0 := time.Date(2026, 6, 28, 9, 0, 0, 0, time.UTC)
	is := mkIssue("ENG-1", t0, "unstarted", false)
	is.Description = trunkDescription(testIDA)
	older := mkIssue("ENG-2", t0.AddDate(0, 0, 2), "backlog", false)
	older.Description = trunkDescription(testIDA)

	fake := &fakeLinear{t: t, issues: []linearIssue{is, older}}
	linearSrv := httptest.NewServer(fake.handler())
	defer linearSrv.Close()
	trunkSrv := httptest.NewServer(fakeTrunkHandler(t, map[string]*testDetails{testIDA: sampleDetails()}))
	defer trunkSrv.Close()

	cfg := config{
		teamKey: "ENG", label: "flaky-test", orgSlug: "acme",
		repoHost: "github.com", repoOwner: "multigres", repoName: "multigres",
		linearToken: "linear-token", trunkToken: "trunk-token",
		linearEndpoint: linearSrv.URL, trunkEndpoint: trunkSrv.URL,
		dryRun: true,
	}
	if err := run(context.Background(), cfg); err != nil {
		t.Fatalf("run() = %v", err)
	}
	if len(fake.updates) != 0 || len(fake.comments) != 0 || len(fake.deletions) != 0 {
		t.Errorf("dry run performed mutations: updates=%+v comments=%+v deletions=%+v",
			fake.updates, fake.comments, fake.deletions)
	}

	// An empty project must not add a project clause to the filter.
	if len(fake.filters) == 0 {
		t.Fatal("no issue-list filter recorded")
	}
	if _, ok := fake.filters[0]["project"]; ok {
		t.Errorf("filter unexpectedly scoped to a project: %+v", fake.filters[0])
	}
}
