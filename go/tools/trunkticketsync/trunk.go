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
	"errors"
	"fmt"
	"net/http"
	"time"
)

// trunkClient is a minimal client for the Trunk Flaky Tests API.
// https://docs.trunk.io/flaky-tests/reference/api-reference
type trunkClient struct {
	endpoint string
	token    string
	http     *http.Client
}

func newTrunkClient(endpoint, token string) *trunkClient {
	return &trunkClient{
		endpoint: endpoint,
		token:    token,
		http:     &http.Client{Timeout: httpTimeout},
	}
}

type trunkRepo struct {
	Host  string `json:"host"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

type testDetailsRequest struct {
	Repo       trunkRepo `json:"repo"`
	OrgURLSlug string    `json:"org_url_slug"`
	TestID     string    `json:"test_id"`
}

type testStatus struct {
	// Value is one of "healthy", "flaky", or "broken".
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

type testFailure struct {
	Summary         string    `json:"summary"`
	OccurrenceCount int       `json:"occurrence_count"`
	LastOccurrence  time.Time `json:"last_occurrence"`
}

type testDetails struct {
	Name     string     `json:"name"`
	HTMLURL  string     `json:"html_url"`
	Status   testStatus `json:"status"`
	FilePath string     `json:"file_path"`
	Parent   string     `json:"parent"`
	// Classname carries the Go package path for tests uploaded from
	// gotestsum JUnit output.
	Classname          string        `json:"classname"`
	MostCommonFailures []testFailure `json:"most_common_failures"`
	// FailureRateLast7d is a fraction in [0, 1].
	FailureRateLast7d          float64 `json:"failure_rate_last_7d"`
	PullRequestsImpactedLast7d int     `json:"pull_requests_impacted_last_7d"`
	Quarantined                bool    `json:"quarantined"`
}

// getTestDetails fetches a test's current status. A 404 returns (nil, nil):
// the test is no longer known to Trunk, which callers must treat as unknown
// state rather than acting on it.
func (c *trunkClient) getTestDetails(ctx context.Context, req testDetailsRequest) (*testDetails, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	url := c.endpoint + "/v1/flaky-tests/get-test-details"
	body, err := postWithRetry(ctx, c.http, url, payload, func(r *http.Request) {
		r.Header.Set("x-api-token", c.token)
		r.Header.Set("Content-Type", "application/json")
	})
	if err != nil {
		var he *httpError
		if errors.As(err, &he) && he.status == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}

	var out struct {
		Test testDetails `json:"test"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode test details: %w", err)
	}
	return &out.Test, nil
}
