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
	"os"
	"strings"

	"github.com/multigres/multigres/go/viperutil"
	"github.com/multigres/multigres/go/web"

	"github.com/spf13/pflag"
)

// HandlerFunc returns an http.HandlerFunc that renders the combined config
// registry (both static and dynamic) for debugging purposes.
//
// Example requests:
//   - GET /debug/config
//   - GET /debug/config?format=json
func HandlerFunc(reg *viperutil.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		v := reg.Combined()
		format := strings.ToLower(r.URL.Query().Get("format"))

		// Collect command-line flags
		type ConfigData struct {
			Title   string
			Options map[string]string
			Config  map[string]string
		}
		configData := ConfigData{
			Title:   os.Args[0],
			Options: make(map[string]string),
			Config:  make(map[string]string),
		}
		pflag.CommandLine.VisitAll(func(flag *pflag.Flag) {
			if flag.Changed {
				configData.Options[flag.Name] = flag.Value.String()
			}
		})

		// Handle default format (debug text)
		if format == "" {
			for _, k := range v.AllKeys() {
				value := v.Get(k)
				if value == nil {
					// should not happen
					continue
				}
				configData.Config[k] = fmt.Sprintf("%v", value)
			}
			_ = web.Templates.ExecuteTemplate(w, "config.html", configData)
			return
		}

		// Handle JSON format specially to include both cmdline flags and viper config
		if format == "json" {
			w.Header().Set("Content-Type", "application/json")

			response := map[string]any{
				"command_line_flags": configData.Options,
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
}
