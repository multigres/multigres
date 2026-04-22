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

package pgparity

import (
	"path/filepath"
	"testing"
)

// TestCorpusParses ensures every test file in testdata/ is parseable and
// contains at least one runnable record. Catches corpus typos without needing
// to spin up the integration cluster.
func TestCorpusParses(t *testing.T) {
	files, err := filepath.Glob("testdata/*.slt")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no .slt files found in testdata/")
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			tf, err := ParseFile(f)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if len(tf.Records) == 0 {
				t.Fatalf("no records parsed")
			}
		})
	}
}
