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
	PrependPath(testPath)
	newPath := os.Getenv("PATH")
	separator := string(os.PathListSeparator)
	expectedPath := absTestPath + separator + "/bin"
	if newPath != expectedPath {
		t.Errorf("PATH not updated correctly, expected %q, got %q", expectedPath, newPath)
	}
}

func TestFindModuleRoot(t *testing.T) {
	// This test should find the go.mod file at the root of the multigres project
	root, err := findModuleRoot()
	if err != nil {
		t.Fatalf("findModuleRoot() failed: %v", err)
	}

	// Verify that go.mod exists at the found root
	goModPath := filepath.Join(root, "go.mod")
	if _, err := os.Stat(goModPath); os.IsNotExist(err) {
		t.Errorf("go.mod not found at %q", goModPath)
	}

	// The root should be an absolute path
	if !filepath.IsAbs(root) {
		t.Errorf("module root should be absolute path, got %q", root)
	}
}

func TestPrependBinToPath(t *testing.T) {
	originalPath := os.Getenv("PATH")
	defer os.Setenv("PATH", originalPath)

	// Reset PATH to a known state
	os.Setenv("PATH", "/usr/bin:/bin")

	// Call PrependBinToPath
	err := PrependBinToPath()
	if err != nil {
		t.Fatalf("PrependBinToPath() failed: %v", err)
	}

	// Get the new PATH
	newPath := os.Getenv("PATH")

	// Find the expected module root
	root, err := findModuleRoot()
	if err != nil {
		t.Fatalf("findModuleRoot() failed: %v", err)
	}

	expectedBinPath := filepath.Join(root, "bin")

	// Verify that the bin path is prepended to PATH
	if !filepath.IsAbs(newPath) {
		// Check if the new path starts with the expected bin path
		separator := string(os.PathListSeparator)
		expectedPrefix := expectedBinPath + separator
		if len(newPath) < len(expectedPrefix) || newPath[:len(expectedPrefix)] != expectedPrefix {
			t.Errorf("PATH does not start with expected bin path. Expected to start with %q, got %q", expectedPrefix, newPath)
		}
	}
}
