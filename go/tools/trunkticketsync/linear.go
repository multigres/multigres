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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/multigres/multigres/go/tools/retry"
)

// linearClient is a minimal Linear GraphQL API client covering the queries
// and mutations this tool needs. Authentication uses a personal or
// workspace API key passed bare in the Authorization header.
type linearClient struct {
	endpoint string
	token    string
	http     *http.Client
}

func newLinearClient(endpoint, token string) *linearClient {
	return &linearClient{
		endpoint: endpoint,
		token:    token,
		http:     &http.Client{Timeout: httpTimeout},
	}
}

type linearState struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Type     string  `json:"type"`
	Position float64 `json:"position"`
}

type linearIssue struct {
	ID          string      `json:"id"`
	Identifier  string      `json:"identifier"`
	Title       string      `json:"title"`
	Description string      `json:"description"`
	CreatedAt   time.Time   `json:"createdAt"`
	ArchivedAt  *time.Time  `json:"archivedAt"`
	State       linearState `json:"state"`
}

const listLabeledIssuesQuery = `
query FlakyTickets($filter: IssueFilter!, $after: String) {
  issues(
    first: 100
    after: $after
    includeArchived: true
    filter: $filter
  ) {
    nodes {
      id
      identifier
      title
      description
      createdAt
      archivedAt
      state { id name type position }
    }
    pageInfo { hasNextPage endCursor }
  }
}`

// listLabeledIssues returns every issue in the team carrying the label,
// optionally scoped to one project, archived and resolved ones included —
// closed tickets anchor first-seen times and reopen decisions. An empty
// project disables project scoping.
func (c *linearClient) listLabeledIssues(ctx context.Context, teamKey, label, project string) ([]linearIssue, error) {
	filter := map[string]any{
		"team":   map[string]any{"key": map[string]any{"eq": teamKey}},
		"labels": map[string]any{"name": map[string]any{"eq": label}},
	}
	if project != "" {
		filter["project"] = map[string]any{"name": map[string]any{"eq": project}}
	}

	var all []linearIssue
	var after *string
	for {
		var out struct {
			Issues struct {
				Nodes    []linearIssue `json:"nodes"`
				PageInfo struct {
					HasNextPage bool   `json:"hasNextPage"`
					EndCursor   string `json:"endCursor"`
				} `json:"pageInfo"`
			} `json:"issues"`
		}
		vars := map[string]any{"filter": filter}
		if after != nil {
			vars["after"] = *after
		}
		if err := c.gql(ctx, listLabeledIssuesQuery, vars, &out); err != nil {
			return nil, err
		}
		all = append(all, out.Issues.Nodes...)
		if !out.Issues.PageInfo.HasNextPage {
			return all, nil
		}
		cursor := out.Issues.PageInfo.EndCursor
		after = &cursor
	}
}

const teamStatesQuery = `
query TeamStates($team: String!) {
  teams(filter: { key: { eq: $team } }) {
    nodes {
      states { nodes { id name type position } }
    }
  }
}`

func (c *linearClient) teamStates(ctx context.Context, teamKey string) ([]linearState, error) {
	var out struct {
		Teams struct {
			Nodes []struct {
				States struct {
					Nodes []linearState `json:"nodes"`
				} `json:"states"`
			} `json:"nodes"`
		} `json:"teams"`
	}
	if err := c.gql(ctx, teamStatesQuery, map[string]any{"team": teamKey}, &out); err != nil {
		return nil, err
	}
	if len(out.Teams.Nodes) == 0 {
		return nil, fmt.Errorf("no Linear team with key %q", teamKey)
	}
	return out.Teams.Nodes[0].States.Nodes, nil
}

const issueUpdateMutation = `
mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) {
  issueUpdate(id: $id, input: $input) { success }
}`

func (c *linearClient) updateIssue(ctx context.Context, id string, input map[string]any) error {
	return c.runMutation(ctx, issueUpdateMutation, map[string]any{"id": id, "input": input}, "issueUpdate")
}

const issueDeleteMutation = `
mutation IssueDelete($id: String!) {
  issueDelete(id: $id) { success }
}`

// deleteIssue moves an issue to Linear's trash, where it stays recoverable
// for a grace period before permanent deletion.
func (c *linearClient) deleteIssue(ctx context.Context, id string) error {
	return c.runMutation(ctx, issueDeleteMutation, map[string]any{"id": id}, "issueDelete")
}

const commentCreateMutation = `
mutation CommentCreate($input: CommentCreateInput!) {
  commentCreate(input: $input) { success }
}`

func (c *linearClient) createComment(ctx context.Context, issueID, body string) error {
	input := map[string]any{"issueId": issueID, "body": body}
	return c.runMutation(ctx, commentCreateMutation, map[string]any{"input": input}, "commentCreate")
}

// runMutation executes a mutation whose selection is "<field> { success }"
// and fails when Linear reports success=false.
func (c *linearClient) runMutation(ctx context.Context, mutation string, vars map[string]any, field string) error {
	var out map[string]struct {
		Success bool `json:"success"`
	}
	if err := c.gql(ctx, mutation, vars, &out); err != nil {
		return err
	}
	if !out[field].Success {
		return fmt.Errorf("%s reported success=false", field)
	}
	return nil
}

// gql posts one GraphQL request and decodes data into out, retrying
// rate-limit and server errors with fixed backoff.
func (c *linearClient) gql(ctx context.Context, query string, variables map[string]any, out any) error {
	payload, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return err
	}
	body, err := postWithRetry(ctx, c.http, c.endpoint, payload, func(req *http.Request) {
		req.Header.Set("Authorization", c.token)
		req.Header.Set("Content-Type", "application/json")
	})
	if err != nil {
		return err
	}

	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode GraphQL response: %w", err)
	}
	if len(envelope.Errors) > 0 {
		return fmt.Errorf("GraphQL error: %s", envelope.Errors[0].Message)
	}
	return json.Unmarshal(envelope.Data, out)
}

// httpError is a terminal non-2xx response from postWithRetry. The status
// code is structured data so callers can branch on it with errors.As
// (getTestDetails treats 404 as "test unknown") instead of matching the
// message text.
type httpError struct {
	url    string
	status int
	body   string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("POST %s: HTTP %d: %s", e.url, e.status, e.body)
}

// postWithRetry POSTs a JSON payload, retrying 429/5xx responses and
// transport errors a few times with jittered exponential backoff. Other
// non-2xx statuses are terminal: retrying a bad token or malformed query
// cannot succeed.
func postWithRetry(ctx context.Context, client *http.Client, url string, payload []byte, decorate func(*http.Request)) ([]byte, error) {
	const maxAttempts = 4
	var lastErr error
	for attempt, rerr := range retry.New(time.Second, 10*time.Second).Attempts(ctx) {
		if rerr != nil {
			return nil, fmt.Errorf("POST %s: %w", url, rerr)
		}
		// #nosec G704 -- url is the operator-configured API endpoint (flag/env); this is a dev/CI tool, not a request handler.
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		decorate(req)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("POST %s: %w", url, err)
		} else {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
			resp.Body.Close()
			switch {
			case readErr != nil:
				lastErr = fmt.Errorf("POST %s: read body: %w", url, readErr)
			case resp.StatusCode/100 == 2:
				return body, nil
			default:
				lastErr = &httpError{url: url, status: resp.StatusCode, body: truncate(body, 512)}
				if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode/100 != 5 {
					return nil, lastErr
				}
			}
		}
		if attempt == maxAttempts-1 {
			return nil, fmt.Errorf("giving up after %d attempts: %w", maxAttempts, lastErr)
		}
	}
	// Unreachable: Attempts only stops when the loop body returns.
	return nil, ctx.Err()
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}
