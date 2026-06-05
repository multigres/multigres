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

package suiteutil

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// WriteJSON marshals v with two-space indent and writes it to
// <outputDir>/<filename>. The output directory is created if missing.
// Returns the absolute path that was written.
func WriteJSON(outputDir, filename string, v any) (string, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", outputDir, err)
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal JSON: %w", err)
	}
	path := filepath.Join(outputDir, filename)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}

// WriteMarkdown writes content to <outputDir>/<filename> and, when running in
// CI, appends the same content to $GITHUB_STEP_SUMMARY so the summary is
// visible on the job page without downloading artifacts. The output directory
// is created if missing. Returns the absolute path that was written.
//
// Failing to append to GITHUB_STEP_SUMMARY is non-fatal — the markdown file
// on disk is the authoritative copy.
func WriteMarkdown(outputDir, filename, content string) (string, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", outputDir, err)
	}
	path := filepath.Join(outputDir, filename)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}

	if f := os.Getenv("GITHUB_STEP_SUMMARY"); f != "" {
		if fh, err := os.OpenFile(f, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			_, _ = fh.WriteString(content)
			_ = fh.Close()
		}
	}
	return path, nil
}

// BadgeColor picks a shields.io badge colour from a pass rate.
//
//	100%  → brightgreen
//	 80+% → yellow
//	 50+% → orange
//	  <50 → red
//	 0/0  → lightgrey (no data)
func BadgeColor(passed, total int) string {
	if total == 0 {
		return "lightgrey"
	}
	if passed == total {
		return "brightgreen"
	}
	pct := passed * 100 / total
	switch {
	case pct >= 80:
		return "yellow"
	case pct >= 50:
		return "orange"
	default:
		return "red"
	}
}

// BadgeMarkdown renders a shields.io badge as an `![alt](url)` markdown image.
//
// When expected > total, the badge reads "P/T_passed_(of_E)" so timed-out or
// partial runs show the intended denominator; otherwise it reads "P/T_passed".
// A timed-out run is flagged with a "_(timed_out)" suffix and downgraded one
// colour level from brightgreen so 100% timed-out runs don't visually appear
// identical to a clean pass.
func BadgeMarkdown(label string, passed, total, expected int, timedOut bool) string {
	colour := BadgeColor(passed, total)
	value := fmt.Sprintf("%d%%2F%d_passed", passed, total)
	if expected > 0 && expected > total {
		value = fmt.Sprintf("%d%%2F%d_passed_(of_%d)", passed, total, expected)
	}
	if timedOut {
		value += "_(timed_out)"
		if colour == "brightgreen" {
			colour = "yellow"
		}
	}
	// The label goes into the shields.io URL path, where a raw space breaks the
	// image (e.g. a multi-word label like "Contrib Extension"). Escape it per
	// shields.io rules so multi-word labels render. The alt text keeps the raw
	// label for readability.
	return fmt.Sprintf("![%s](https://img.shields.io/badge/%s-%s-%s)", label, encodeBadgeField(label), value, colour)
}

// BadgeEndpoint is the shields.io "endpoint" badge schema. Serialized to JSON
// and published to a stable public URL, it lets a README or blog badge
// re-render the current pass count at view time — republishing the JSON updates
// every badge that references it, with no markdown edits.
// See https://shields.io/badges/endpoint-badge.
type BadgeEndpoint struct {
	SchemaVersion int    `json:"schemaVersion"`
	Label         string `json:"label"`
	Message       string `json:"message"`
	Color         string `json:"color"`
}

// NewBadgeEndpoint builds a shields.io endpoint badge payload from a suite's
// pass count. It mirrors BadgeMarkdown's message and colour rules (the expected
// denominator when a run is partial, and the timed-out downgrade) but renders
// the message as plain text suitable for the endpoint "message" field — no URL
// escaping, since shields reads it from JSON rather than the URL path.
func NewBadgeEndpoint(label string, passed, total, expected int, timedOut bool) BadgeEndpoint {
	colour := BadgeColor(passed, total)
	message := fmt.Sprintf("%d/%d passed", passed, total)
	if expected > 0 && expected > total {
		message = fmt.Sprintf("%d/%d passed (of %d)", passed, total, expected)
	}
	if timedOut {
		message += " (timed out)"
		if colour == "brightgreen" {
			colour = "yellow"
		}
	}
	return BadgeEndpoint{SchemaVersion: 1, Label: label, Message: message, Color: colour}
}

// BadgeSlug converts a suite label into a stable, URL-safe filename stem for its
// endpoint JSON (e.g. "Contrib Extension" -> "contrib-extension"). The slug is
// the published URL path, so keep it stable: README/blog badge URLs depend on
// it.
func BadgeSlug(label string) string {
	s := strings.ToLower(strings.TrimSpace(label))
	return strings.ReplaceAll(s, " ", "-")
}

// encodeBadgeField escapes a string for use in a shields.io badge URL path
// segment: literal underscores and dashes are doubled, then spaces become
// single underscores (which shields.io renders back as spaces). Order matters —
// the space substitution runs last so it is not re-escaped.
func encodeBadgeField(s string) string {
	s = strings.ReplaceAll(s, "_", "__")
	s = strings.ReplaceAll(s, "-", "--")
	s = strings.ReplaceAll(s, " ", "_")
	return s
}
