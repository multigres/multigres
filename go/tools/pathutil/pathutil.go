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
	"os"
	"path/filepath"
)

// AppendPath appends a given path to the PATH environment variable.
// It ensures the path is absolute.
func AppendPath(path string) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return
	}
	_ = os.Setenv("PATH", os.Getenv("PATH")+string(os.PathListSeparator)+absPath)
}

// PrependPath prepends a given path to the PATH environment variable.
// It ensures the path is absolute and takes precedence over existing paths.
func PrependPath(path string) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return
	}
	_ = os.Setenv("PATH", absPath+string(os.PathListSeparator)+os.Getenv("PATH"))
}
