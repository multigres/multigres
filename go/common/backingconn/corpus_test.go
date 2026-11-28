// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backingconn

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// corpusEntry is one connection string of testdata/corpus.json, which the tests of this package
// and the test that runs the ones we accept through libpq (go/test/endtoend/backingconn) read.
type corpusEntry struct {
	DSN string `json:"dsn"`
	// Accepted is whether a connection with this string (and a password) is created.
	Accepted bool `json:"accepted"`
	// Source is "review" for a string of the review of the parser (verbatim, including the host h
	// that stands for a reachable one), and "ours" for one added with it.
	Source string `json:"source"`
	// Why is what is wrong with a string we refuse, as libpq words it where libpq refuses it too,
	// and otherwise the rule we are stricter by; for one we accept, what it is.
	Why string `json:"why"`
}

func readCorpus(t testing.TB) []corpusEntry {
	t.Helper()
	data, err := os.ReadFile("testdata/corpus.json")
	require.NoError(t, err)
	var corpus []corpusEntry
	require.NoError(t, json.Unmarshal(data, &corpus))
	require.NotEmpty(t, corpus)
	return corpus
}

// withCredentials adds what a connection must have and the string leaves out, a password, so
// that a string is judged on what the corpus says is wrong with it and not on that.
func withCredentials(dsn string) string {
	if strings.HasPrefix(dsn, "postgres://u@") {
		return strings.Replace(dsn, "postgres://u@", "postgres://u:pw@", 1)
	}
	if strings.HasPrefix(dsn, "password=") || strings.Contains(dsn, " password=") {
		return dsn
	}
	return dsn + " password=pw"
}

// The strings of the review of the parser, verbatim, and ours: each that must be refused is
// refused when the connection is created, for the rule it is about and not for a missing
// password; each that must be accepted is, given the password a connection requires.
func TestCorpus(t *testing.T) {
	for _, e := range readCorpus(t) {
		t.Run(strings.ReplaceAll(e.DSN, "\x00", "<NUL>"), func(t *testing.T) {
			// As written.
			_, err := FromDSN(e.DSN)
			require.Error(t, err, "a connection needs a password, which no string of the corpus has")
			if e.Accepted {
				require.ErrorContains(t, err, "password is required", "the only thing wrong with it")
			}

			// With a password.
			conn, err := FromDSN(withCredentials(e.DSN))
			if e.Accepted {
				require.NoError(t, err, e.Why)
				require.NoError(t, conn.Validate())
				back, err := FromDSN(conn.DSN())
				require.NoError(t, err)
				require.Equal(t, conn, back, "what is stored reads back as the same connection")
				return
			}
			require.Error(t, err, e.Why)
			require.NotContains(t, err.Error(), "password is required", "refused for what is wrong with it: %s", e.Why)
			require.NotContains(t, err.Error(), "pw\"", "an error never carries the string")
		})
	}
}

// The corpus has the strings of the review of the parser, as written there: the two tables.
func TestCorpusHasTheReviewTables(t *testing.T) {
	var refused, accepted int
	for _, e := range readCorpus(t) {
		if e.Source != "review" {
			continue
		}
		if e.Accepted {
			accepted++
		} else {
			refused++
		}
	}
	require.Equal(t, 21, refused)
	require.Equal(t, 5, accepted)
}
