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

package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DumpServiceLogs prints service log files to help debug test failures.
// It walks the logs directory tree in the given tempDir and prints all .log files.
func DumpServiceLogs(tempDir string) {
	if tempDir == "" {
		return
	}

	logsDir := filepath.Join(tempDir, "logs")
	if _, err := os.Stat(logsDir); os.IsNotExist(err) {
		fmt.Printf("No logs directory found at %s\n", logsDir)
		return
	}

	fmt.Println("\n==== SERVICE LOGS (test failure) ====")
	fmt.Printf("Logs directory: %s\n", logsDir)

	err := filepath.Walk(logsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// Only print .log files
		if !strings.HasSuffix(path, ".log") {
			return nil
		}

		relPath, _ := filepath.Rel(logsDir, path)
		fmt.Printf("\n--- %s ---\n", relPath)

		content, err := os.ReadFile(path)
		if err != nil {
			fmt.Printf("  [error reading log: %v]\n", err)
			return nil
		}
		if len(content) == 0 {
			fmt.Println("  [empty log file]")
			return nil
		}
		fmt.Println(string(content))
		return nil
	})
	if err != nil {
		fmt.Printf("Error walking logs directory: %v\n", err)
	}

	fmt.Println("\n==== END SERVICE LOGS ====")
}
