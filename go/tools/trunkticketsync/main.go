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

// Command trunkticketsync reconciles Linear tickets created by the Trunk
// Flaky Tests integration. Trunk opens a ticket when a test transitions to
// flaky and never touches it again, so over time the backlog accumulates
// duplicates with no status. This tool groups tickets by the Trunk test ID
// embedded in their description, deletes newer duplicates in a group in
// favor of the oldest, refreshes a managed status section on the canonical
// ticket from Trunk's get-test-details API (status, first-seen/latest
// failure, severity, quarantine, most common failure reasons), closes
// tickets whose tests are healthy again, and reopens completed tickets
// whose tests regressed.
//
// Deployment-specific configuration (Linear team/label/project, Trunk org
// slug, API tokens) comes exclusively from environment variables so no
// organization-internal identifiers live in this public repository.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultRepoHost = "github.com"

	linearEndpoint = "https://api.linear.app/graphql"
	trunkEndpoint  = "https://api.trunk.io"

	httpTimeout = 60 * time.Second
	runTimeout  = 15 * time.Minute
)

// Managed-section markers. The section between the markers is rewritten
// when its content changes; everything outside it (Trunk's original ticket
// body, human notes) is preserved. sectionHeading doubles as a fallback
// anchor in case a UI edit strips the HTML comments.
//
// The begin marker carries a hash of the section body ("<!--
// flaky-ticket-sync:begin content:<hash> -->"). Linear normalizes markdown
// when it saves a description (link syntax, escapes), so comparing the
// stored text against a fresh render never converges — the marker hash is
// the only reliable change signal.
//
// The "flaky-ticket-sync" token is a persisted format written into live
// ticket descriptions; it intentionally survives tool renames.
const (
	managedBeginPrefix = "<!-- flaky-ticket-sync:begin"
	managedEnd         = "<!-- flaky-ticket-sync:end -->"
	sectionHeading     = "### Flaky test status"
)

func beginMarker(hash string) string {
	return managedBeginPrefix + " content:" + hash + " -->"
}

func sectionHash(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:8])
}

// managedSectionHash returns the content hash recorded in a description's
// begin marker, or "" when there is no marker or an old marker without a
// hash (both trigger one rewrite, after which the hash is in place).
func managedSectionHash(desc string) string {
	_, rest, found := strings.Cut(desc, managedBeginPrefix)
	if !found {
		return ""
	}
	attrs, _, found := strings.Cut(rest, "-->")
	if !found {
		return ""
	}
	hash, found := strings.CutPrefix(strings.TrimSpace(attrs), "content:")
	if !found {
		return ""
	}
	return hash
}

// trunkTestURLRe matches the Trunk test-detail link that the ticketing
// integration embeds in every ticket description; the capture group is the
// stable test ID used as the dedupe key.
var trunkTestURLRe = regexp.MustCompile(
	`app\.trunk\.io/[^/\s]+/flaky-tests/repo/[0-9a-fA-F-]{36}/test/` +
		`([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})`)

type config struct {
	teamKey   string
	label     string
	project   string
	orgSlug   string
	repoHost  string
	repoOwner string
	repoName  string

	linearToken    string
	trunkToken     string
	linearEndpoint string
	trunkEndpoint  string

	dryRun  bool
	verbose bool
}

func main() {
	repoHost := flag.String("repo-host", defaultRepoHost, "repository host as registered in Trunk")
	dryRun := flag.Bool("dry-run", false, "log mutations instead of performing them")
	verbose := flag.Bool("verbose", false, "log every ticket found, grouped by test")
	flag.Parse()

	repo := requireEnv("GITHUB_REPOSITORY")
	owner, name, ok := strings.Cut(repo, "/")
	if !ok {
		// #nosec G706 -- repo is GITHUB_REPOSITORY from the CI runner; this is a dev/CI tool, not a request handler.
		log.Fatalf("GITHUB_REPOSITORY must be owner/name, got %q", repo)
	}

	cfg := config{
		teamKey:        requireEnv("LINEAR_TEAM_KEY"),
		label:          requireEnv("LINEAR_LABEL"),
		project:        requireEnv("LINEAR_PROJECT"),
		orgSlug:        requireEnv("TRUNK_ORG_SLUG"),
		repoHost:       *repoHost,
		repoOwner:      owner,
		repoName:       name,
		linearToken:    requireEnv("LINEAR_API_TOKEN"),
		trunkToken:     requireEnv("TRUNK_API_TOKEN"),
		linearEndpoint: linearEndpoint,
		trunkEndpoint:  trunkEndpoint,
		dryRun:         *dryRun,
		verbose:        *verbose,
	}

	//nolint:gocritic // main is the program entry point; context.Background() is the canonical root.
	ctx, cancel := context.WithTimeout(context.Background(), runTimeout)
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		log.Fatalf("sync failed: %v", err)
	}
}

func requireEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		log.Fatalf("%s is required", name)
	}
	return v
}

func run(ctx context.Context, cfg config) error {
	linear := newLinearClient(cfg.linearEndpoint, cfg.linearToken)
	trunk := newTrunkClient(cfg.trunkEndpoint, cfg.trunkToken)

	issues, err := linear.listLabeledIssues(ctx, cfg.teamKey, cfg.label, cfg.project)
	if err != nil {
		return fmt.Errorf("list Linear issues: %w", err)
	}
	// Deliberately not echoing the team/label/project values: this runs in
	// public CI logs and they are configured as secrets.
	log.Printf("found %d ticket(s) in the configured team/label/project scope", len(issues))

	states, err := linear.teamStates(ctx, cfg.teamKey)
	if err != nil {
		return fmt.Errorf("resolve team states: %w", err)
	}
	transitions, err := resolveTransitions(states)
	if err != nil {
		return err
	}

	groups := groupByTestID(issues)
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	plans := make(map[string]groupPlan, len(groups))
	for _, k := range keys {
		plans[k] = planGroup(groups[k])
	}
	if cfg.verbose {
		logGroups(keys, plans)
	}

	s := &syncer{cfg: cfg, linear: linear, trunk: trunk, transitions: transitions}
	for _, testID := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.syncGroup(ctx, testID, plans[testID])
	}

	log.Printf("done: %d group(s), %d duplicate(s) deleted, %d description(s) updated, %d closed, %d reopened, %d error(s)",
		len(groups), s.duplicatesDeleted, s.descriptionsUpdated, s.closed, s.reopened, s.errs)
	if s.errs > 0 {
		return fmt.Errorf("%d ticket(s) failed to sync; see log", s.errs)
	}
	return nil
}

// logGroups dumps every ticket grouped by test, annotated with how the
// planner classifies it. Purely informational.
func logGroups(keys []string, plans map[string]groupPlan) {
	for _, testID := range keys {
		plan := plans[testID]
		log.Printf("test %s: %d ticket(s)", testID, len(plan.tickets))
		for _, tk := range plan.tickets {
			var role string
			switch {
			case tk.ArchivedAt != nil:
				role = "archived"
			case plan.canonical != nil && tk.ID == plan.canonical.ID:
				role = "canonical"
			case isTerminal(tk.State.Type):
				role = "resolved"
			default:
				role = "duplicate"
			}
			log.Printf("  %-9s %s [%s] created %s — %s",
				role, tk.Identifier, tk.State.Name, tk.CreatedAt.UTC().Format("2006-01-02"), tk.Title)
		}
	}
}

// --- Grouping and planning (pure logic) ---

// groupByTestID buckets tickets by the Trunk test ID in their description.
// Tickets without a recognizable Trunk link (e.g. hand-written ones that
// share the label) are logged and left untouched.
func groupByTestID(issues []linearIssue) map[string][]linearIssue {
	groups := map[string][]linearIssue{}
	for _, is := range issues {
		id := extractTestID(is.Description)
		if id == "" {
			log.Printf("skipping %s: no Trunk test link in description", is.Identifier)
			continue
		}
		groups[id] = append(groups[id], is)
	}
	return groups
}

func extractTestID(description string) string {
	m := trunkTestURLRe.FindStringSubmatch(description)
	if m == nil {
		return ""
	}
	return strings.ToLower(m[1])
}

// isTerminal reports whether a workflow state type means the ticket is
// resolved (no further sync actions except a possible reopen).
func isTerminal(stateType string) bool {
	switch stateType {
	case "completed", "canceled", "duplicate":
		return true
	}
	return false
}

type groupPlan struct {
	tickets    []linearIssue // all tickets in the group, oldest first
	canonical  *linearIssue
	duplicates []linearIssue
	firstSeen  time.Time
}

// planGroup picks the canonical ticket for a test and the open duplicates to
// fold into it. The canonical is the oldest non-archived ticket that is not
// already resolved — so if a human marked the oldest ticket Duplicate of a
// newer one, their direction is respected. firstSeen is the oldest creation
// time across all tickets, archived included, so history survives dedupe.
func planGroup(tickets []linearIssue) groupPlan {
	sorted := append([]linearIssue(nil), tickets...)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].CreatedAt.Equal(sorted[j].CreatedAt) {
			return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
		}
		return sorted[i].Identifier < sorted[j].Identifier
	})

	p := groupPlan{tickets: sorted, firstSeen: sorted[0].CreatedAt}
	var firstActive *linearIssue
	for i := range sorted {
		if sorted[i].ArchivedAt != nil {
			continue
		}
		if firstActive == nil {
			firstActive = &sorted[i]
		}
		if !isTerminal(sorted[i].State.Type) {
			p.canonical = &sorted[i]
			break
		}
	}
	if p.canonical == nil {
		p.canonical = firstActive
	}
	if p.canonical == nil {
		return p // every ticket in the group is archived
	}
	for _, t := range sorted {
		if t.ArchivedAt != nil || t.ID == p.canonical.ID || isTerminal(t.State.Type) {
			continue
		}
		p.duplicates = append(p.duplicates, t)
	}
	return p
}

type lifecycle int

const (
	lifecycleNone lifecycle = iota
	lifecycleClose
	lifecycleReopen
)

// decideLifecycle maps Trunk's view of the test onto a ticket state change.
// A healthy, unquarantined test closes its ticket; a quarantined test stays
// open even when healthy because the quarantine still masks failures. A
// flaky or broken test reopens a Done ticket, but not one a human resolved
// as Canceled or Duplicate — those are deliberate decisions to stop
// tracking.
func decideLifecycle(d *testDetails, stateType string) lifecycle {
	switch {
	case d.Status.Value == "healthy" && !d.Quarantined && !isTerminal(stateType):
		return lifecycleClose
	case (d.Status.Value == "flaky" || d.Status.Value == "broken") && stateType == "completed":
		return lifecycleReopen
	}
	return lifecycleNone
}

// --- Sync execution ---

// stateTransitions holds the resolved workflow state IDs used for ticket
// state changes in one team. Duplicates need no state here: creating a
// duplicate relation moves the ticket into Linear's reserved Duplicate
// state automatically.
type stateTransitions struct {
	done   linearState
	reopen linearState
}

// resolveTransitions picks target states by type so the tool works with
// renamed states: the first (by position) completed-type state for closing,
// and the first unstarted-type state (falling back to backlog) for
// reopening.
func resolveTransitions(states []linearState) (stateTransitions, error) {
	sorted := append([]linearState(nil), states...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Position < sorted[j].Position })
	firstOfType := func(types ...string) *linearState {
		for _, typ := range types {
			for i := range sorted {
				if sorted[i].Type == typ {
					return &sorted[i]
				}
			}
		}
		return nil
	}

	done := firstOfType("completed")
	reopen := firstOfType("unstarted", "backlog")
	if done == nil || reopen == nil {
		return stateTransitions{}, fmt.Errorf("team is missing required workflow states (completed: %v, unstarted/backlog: %v)",
			done != nil, reopen != nil)
	}
	return stateTransitions{done: *done, reopen: *reopen}, nil
}

type syncer struct {
	cfg         config
	linear      *linearClient
	trunk       *trunkClient
	transitions stateTransitions

	duplicatesDeleted   int
	descriptionsUpdated int
	closed              int
	reopened            int
	errs                int
}

// syncGroup reconciles all tickets that point at one Trunk test: first
// delete duplicates of the canonical ticket (needs no Trunk data), then refresh
// the canonical's status section and lifecycle from get-test-details. A
// failed Trunk lookup skips the enrich/close/reopen steps — never act on
// unknown test state.
func (s *syncer) syncGroup(ctx context.Context, testID string, plan groupPlan) {
	if plan.canonical == nil {
		log.Printf("test %s: all %d ticket(s) archived, skipping", testID, len(plan.tickets))
		return
	}
	canonical := plan.canonical

	for _, dup := range plan.duplicates {
		if s.deleteDuplicate(ctx, dup, *canonical) {
			s.duplicatesDeleted++
		}
	}

	details, err := s.trunk.getTestDetails(ctx, testDetailsRequest{
		Repo:       trunkRepo{Host: s.cfg.repoHost, Owner: s.cfg.repoOwner, Name: s.cfg.repoName},
		OrgURLSlug: s.cfg.orgSlug,
		TestID:     testID,
	})
	if err != nil {
		log.Printf("WARN: %s: get Trunk details for test %s: %v", canonical.Identifier, testID, err)
		s.errs++
		return
	}
	if details == nil {
		log.Printf("%s: test %s no longer known to Trunk, leaving ticket as-is", canonical.Identifier, testID)
		return
	}

	// Compare marker hashes, not text: Linear normalizes the saved markdown,
	// so the stored section never matches a fresh render byte-for-byte even
	// when nothing changed.
	section := renderStatusSection(details, plan.firstSeen)
	if managedSectionHash(section) != managedSectionHash(canonical.Description) {
		newDesc := replaceManagedSection(canonical.Description, section)
		if s.updateDescription(ctx, *canonical, newDesc) {
			s.descriptionsUpdated++
		}
	}

	switch decideLifecycle(details, canonical.State.Type) {
	case lifecycleClose:
		comment := "Trunk reports this test as healthy and it is not quarantined — closed by trunk-ticket-sync."
		if s.mutate(ctx, *canonical, "close as healthy", comment, s.transitions.done.ID) {
			s.closed++
		}
	case lifecycleReopen:
		comment := fmt.Sprintf("Trunk reports this test as %s again — reopened by trunk-ticket-sync.", details.Status.Value)
		if s.mutate(ctx, *canonical, "reopen as "+details.Status.Value, comment, s.transitions.reopen.ID) {
			s.reopened++
		}
	case lifecycleNone:
	}
}

// deleteDuplicate trashes a newer Trunk-created ticket for a test that
// already has a canonical ticket, keeping the backlog to one ticket per
// test. Linear's trash keeps deleted issues recoverable for a grace period,
// and the history the duplicate represented lives on in the canonical
// ticket's first-seen/latest-failure fields.
func (s *syncer) deleteDuplicate(ctx context.Context, dup, canonical linearIssue) bool {
	what := "delete as duplicate of " + canonical.Identifier
	if s.cfg.dryRun {
		log.Printf("DRY-RUN: would %s: %s (%s)", what, dup.Identifier, dup.Title)
		return true
	}
	if err := s.linear.deleteIssue(ctx, dup.ID); err != nil {
		log.Printf("WARN: %s: %s: %v", dup.Identifier, what, err)
		s.errs++
		return false
	}
	log.Printf("%s: %s", dup.Identifier, what)
	return true
}

// mutate posts an explanatory comment and then moves the ticket to stateID,
// honoring dry-run. Comment first: if the state change fails the next run
// retries both, whereas a state change without its comment would leave an
// unexplained transition.
func (s *syncer) mutate(ctx context.Context, issue linearIssue, what, comment, stateID string) bool {
	if s.cfg.dryRun {
		log.Printf("DRY-RUN: would %s: %s (%s)", what, issue.Identifier, issue.Title)
		return true
	}
	if err := s.linear.createComment(ctx, issue.ID, comment); err != nil {
		log.Printf("WARN: %s: comment: %v", issue.Identifier, err)
		s.errs++
		return false
	}
	if err := s.linear.updateIssue(ctx, issue.ID, map[string]any{"stateId": stateID}); err != nil {
		log.Printf("WARN: %s: %s: %v", issue.Identifier, what, err)
		s.errs++
		return false
	}
	log.Printf("%s: %s", issue.Identifier, what)
	return true
}

func (s *syncer) updateDescription(ctx context.Context, issue linearIssue, desc string) bool {
	if s.cfg.dryRun {
		log.Printf("DRY-RUN: would update status section: %s (%s)", issue.Identifier, issue.Title)
		return true
	}
	if err := s.linear.updateIssue(ctx, issue.ID, map[string]any{"description": desc}); err != nil {
		log.Printf("WARN: %s: update description: %v", issue.Identifier, err)
		s.errs++
		return false
	}
	log.Printf("%s: status section updated", issue.Identifier)
	return true
}

// --- Rendering ---

// renderStatusSection builds the managed markdown block for a ticket
// description from Trunk's test details. The output is deterministic for
// unchanged inputs so repeated syncs produce no description churn.
func renderStatusSection(d *testDetails, firstSeen time.Time) string {
	var b strings.Builder
	b.WriteString(sectionHeading + "\n\n")
	if full := fullTestName(d); full != "" {
		fmt.Fprintf(&b, "Test: `%s`\n\n", full)
	}
	if d.Status.Timestamp.IsZero() {
		fmt.Fprintf(&b, "- **Status:** %s\n", d.Status.Value)
	} else {
		fmt.Fprintf(&b, "- **Status:** %s (since %s)\n", d.Status.Value, formatDate(d.Status.Timestamp))
	}
	if !firstSeen.IsZero() {
		fmt.Fprintf(&b, "- **First seen:** %s\n", formatDate(firstSeen))
	}
	if last, ok := latestFailure(d); ok {
		fmt.Fprintf(&b, "- **Latest failure:** %s\n", formatDate(last))
	}
	fmt.Fprintf(&b, "- **Severity (last 7 days):** %s%% failure rate, impacting %d %s\n",
		formatRate(d.FailureRateLast7d), d.PullRequestsImpactedLast7d, pluralize(d.PullRequestsImpactedLast7d, "PR"))
	if d.Quarantined {
		b.WriteString("- **Quarantined:** yes — test failures are ignored while in this state\n")
	}
	switch n := len(d.MostCommonFailures); {
	case n == 1:
		top := d.MostCommonFailures[0]
		fmt.Fprintf(&b, "\n**Most common failure reason** (%d %s):\n\n```\n%s\n```\n",
			top.OccurrenceCount, pluralize(top.OccurrenceCount, "occurrence"), sanitizeSummary(top.Summary))
	case n > 1:
		fmt.Fprintf(&b, "\n**Most common failure reasons** (%d identified):\n", n)
		for i, f := range d.MostCommonFailures {
			fmt.Fprintf(&b, "\nReason #%d (%d %s):\n\n```\n%s\n```\n",
				i+1, f.OccurrenceCount, pluralize(f.OccurrenceCount, "occurrence"), sanitizeSummary(f.Summary))
		}
	}
	if d.HTMLURL != "" {
		fmt.Fprintf(&b, "\n[Trunk test details](%s)\n", d.HTMLURL)
	}
	body := b.String()
	return beginMarker(sectionHash(body)) + "\n" + body + managedEnd
}

// replaceManagedSection swaps the managed block inside a ticket description
// for the freshly rendered one, preserving all surrounding content. If a UI
// edit stripped the marker comments, the section heading anchors the
// replacement (to end of description, since the section is always appended
// last); with no trace of the section it is appended.
func replaceManagedSection(desc, section string) string {
	if begin := strings.Index(desc, managedBeginPrefix); begin >= 0 {
		if end := strings.Index(desc[begin:], managedEnd); end >= 0 {
			return desc[:begin] + section + desc[begin+end+len(managedEnd):]
		}
	}
	prefix, _, _ := strings.Cut(desc, sectionHeading)
	prefix = strings.TrimRight(prefix, " \t\n")
	if prefix == "" {
		return section
	}
	return prefix + "\n\n" + section
}

// fullTestName renders "package TestName" the way our JUnit uploads register
// tests in Trunk: gotestsum writes the Go package path as the classname (and
// suite name), so classname is preferred with parent and file path as
// fallbacks.
func fullTestName(d *testDetails) string {
	pkg := d.Classname
	if pkg == "" {
		pkg = d.Parent
	}
	if pkg == "" {
		pkg = d.FilePath
	}
	return strings.TrimSpace(pkg + " " + d.Name)
}

func latestFailure(d *testDetails) (time.Time, bool) {
	var last time.Time
	for _, f := range d.MostCommonFailures {
		if f.LastOccurrence.After(last) {
			last = f.LastOccurrence
		}
	}
	return last, !last.IsZero()
}

func formatDate(t time.Time) string {
	return t.UTC().Format("Jan 2, 2006")
}

// formatRate renders a 0-1 failure-rate fraction as a percentage with up to
// two decimals and no trailing zeros ("0.25", "1.5", "25").
func formatRate(fraction float64) string {
	s := strconv.FormatFloat(fraction*100, 'f', 2, 64)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" {
		return "0"
	}
	return s
}

func pluralize(n int, singular string) string {
	if n == 1 {
		return singular
	}
	return singular + "s"
}

// sanitizeSummary makes a failure summary safe to embed in a fenced code
// block and keeps it to a readable length.
func sanitizeSummary(s string) string {
	s = strings.ReplaceAll(s, "```", "` ` `")
	s = strings.TrimSpace(s)
	const maxLen = 1000
	if len(s) > maxLen {
		s = s[:maxLen] + "…"
	}
	return s
}
