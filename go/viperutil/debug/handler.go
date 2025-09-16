// Copyright 2023 The Vitess Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Modifications Copyright 2025 Supabase, Inc.

package debug

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/multigres/multigres/go/viperutil/internal/registry"

	"github.com/spf13/pflag"
)

// HandlerFunc provides an http.HandlerFunc that renders the combined config
// registry (both static and dynamic) for debugging purposes.
//
// Example requests:
//   - GET /debug/config
//   - GET /debug/config?format=json
func HandlerFunc(w http.ResponseWriter, r *http.Request) {
	v := registry.Combined()
	format := strings.ToLower(r.URL.Query().Get("format"))

	// Collect command-line flags
	cmdlineFlags := make(map[string]string)
	pflag.CommandLine.VisitAll(func(flag *pflag.Flag) {
		if flag.Changed {
			cmdlineFlags[flag.Name] = flag.Value.String()
		}
	})

	// Handle default format (debug text)
	if format == "" {
		fmt.Fprintf(w, "=== Command-line Flags (parsed) ===\n")
		for name, value := range cmdlineFlags {
			fmt.Fprintf(w, "%s=%s\n", name, value)
		}
		fmt.Fprintf(w, "\n=== Viper Configuration ===\n")
		v.DebugTo(w)
		return
	}

	// Handle JSON format specially to include both cmdline flags and viper config
	if format == "json" {
		w.Header().Set("Content-Type", "application/json")

		response := map[string]interface{}{
			"command_line_flags": cmdlineFlags,
			"viper_config":       v.AllSettings(),
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(response); err != nil {
			http.Error(w, fmt.Sprintf("failed to encode JSON: %v", err), http.StatusInternalServerError)
		}
		return
	}
}
