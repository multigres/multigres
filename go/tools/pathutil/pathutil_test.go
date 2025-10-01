// Copyright 2025 Supabase, Inc.
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

package pathutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendPath(t *testing.T) {
	originalPath := os.Getenv("PATH")
	defer os.Setenv("PATH", originalPath)

	testPath := "test-path"
	err := os.Mkdir(testPath, 0o755)
	if err != nil {
		t.Fatalf("could not create test directory: %v", err)
	}
	defer os.RemoveAll(testPath)

	absTestPath, err := filepath.Abs(testPath)
	if err != nil {
		t.Fatalf("could not get absolute path: %v", err)
	}

	// Test appending to an existing PATH
	os.Setenv("PATH", "/bin")
	AppendPath(testPath)
	newPath := os.Getenv("PATH")
	separator := string(os.PathListSeparator)
	expectedPath := "/bin" + separator + absTestPath
	if newPath != expectedPath {
		t.Errorf("PATH not updated correctly, expected %q, got %q", expectedPath, newPath)
	}
}
