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

// Package pathutil provides a command to append a path to PATH.
package pathutil

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// PrependPath prepends a given path to the PATH environment variable.
// It ensures the path is absolute and takes precedence over existing paths.
func PrependPath(path string) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return
	}
	_ = os.Setenv("PATH", absPath+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// findModuleRoot finds the root directory of the Go module by walking up
// the directory tree looking for go.mod. It starts from the current working
// directory and walks up until it finds go.mod or reaches the filesystem root.
// Returns the directory containing go.mod, or an error if not found.
//
// This approach is borrowed from the Go standard library's module loading logic:
// https://github.com/golang/go/blob/9e3b1d53a012e98cfd02de2de8b1bd53522464d4/src/cmd/go/internal/modload/init.go#L1504-L1522
func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("cannot get working directory: %w", err)
	}

	dir = filepath.Clean(dir)

	// Look for enclosing go.mod
	for {
		goModPath := filepath.Join(dir, "go.mod")
		if fi, err := os.Stat(goModPath); err == nil && !fi.IsDir() {
			return dir, nil
		}

		// Move to parent directory
		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached the filesystem root
			break
		}
		dir = parent
	}

	return "", errors.New("go.mod not found in any parent directory")
}

// prependModuleSubdirsToPath finds the module root and prepends multiple
// subdirectories to PATH. Each subdir is joined with the module root and
// prepended to PATH in order (first argument will have highest precedence).
// Subdirs are processed in reverse order so that the first argument ends up
// first in the resulting PATH.
func prependModuleSubdirsToPath(subdirs ...string) error {
	moduleRoot, err := findModuleRoot()
	if err != nil {
		return fmt.Errorf("failed to find module root: %w", err)
	}

	// Iterate in reverse order so first argument ends up first in PATH
	for i := len(subdirs) - 1; i >= 0; i-- {
		targetPath := filepath.Join(moduleRoot, subdirs[i])
		PrependPath(targetPath)
	}
	return nil
}

// PrependBinToPath finds the module root and prepends the bin directory to PATH.
// This is useful for tests that need to use binaries built in the project.
// go/test/endtoend is also added because there are scripts there to help with
// detecting and killing orphan subprocesses during endtoend tests.
//
// If GOCOVERDIR is set (indicating coverage collection is desired), bin/cov/ is
// prepended before bin/ to enable automatic coverage collection from subprocess
// executions. If bin/cov/ doesn't exist, PATH lookup will skip it harmlessly.
func PrependBinToPath() error {
	// Check if coverage collection is requested via GOCOVERDIR
	if gocoverdir := os.Getenv("GOCOVERDIR"); gocoverdir != "" {
		// GOCOVERDIR is set, so prepend bin/cov before bin
		// This allows coverage-instrumented binaries to be found first
		return prependModuleSubdirsToPath("bin/cov", "bin", "go/test/endtoend")
	}

	// Normal case: just prepend bin
	return prependModuleSubdirsToPath("bin", "go/test/endtoend")
}
