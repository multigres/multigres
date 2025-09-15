// Copyright 2025 Supabase, Inc.
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

package servenv

import (
	"net/http"
	"os"
	"path"
	"time"

	viperdebug "github.com/multigres/multigres/go/viperutil/debug"
	"github.com/multigres/multigres/go/web"

	"github.com/spf13/pflag"
)

func init() {
	HTTPHandleFunc("/css/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css")
		cssPath := path.Join("templates", r.URL.Path)
		content, err := web.TemplateFS.ReadFile(cssPath)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(content)
	})

	HTTPHandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		type CommandOption struct {
			Flag  string
			Value string
		}

		type IndexData struct {
			Title    string
			Options  map[string]string
			LiveLink string
		}

		cmdOptions := make(map[string]string)
		pflag.CommandLine.VisitAll(func(flag *pflag.Flag) {
			if flag.Changed {
				cmdOptions[flag.Name] = flag.Value.String()
			}
		})

		indexData := IndexData{
			Title:    os.Args[0],
			Options:  cmdOptions,
			LiveLink: "/live",
		}
		_ = web.Templates.ExecuteTemplate(w, "index.html", indexData)
	})

	HTTPHandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		// StatusContent represents a piece of content used in StatusTemplate.
		type StatusContent struct {
			Style   string
			Message string
		}
		_ = web.Templates.ExecuteTemplate(w, "live.html", []StatusContent{
			{Style: "status", Message: "✓"},
			{Style: "message", Message: "ok"},
			{Style: "timestamp", Message: time.Now().Format(time.RFC3339)},
		})
	})

	HTTPHandleFunc("/debug/config", viperdebug.HandlerFunc)
}
