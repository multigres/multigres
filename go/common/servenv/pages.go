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
	"encoding/json"
	"net/http"
	"path"
	"time"

	viperdebug "github.com/multigres/multigres/go/common/servenv/viperdebug"
	"github.com/multigres/multigres/go/common/version"
	"github.com/multigres/multigres/go/common/web"
)

// The RegisterCommonHTTPEndpoints function sets up all the necessary frameworks for serving pages.
// You can define go html templates in go/web/templates, and they can use css
// files from go/web/templates/css, which contains a minimal pico download.
// You can use an existing html file as an example to create your own.
// Currently: Headings, tables and grids are supported. If other tags don't
// render correctly, we may need to add more css styles.
// We are using a classless css approach to minimize complexity.

func (sv *ServEnv) RegisterCommonHTTPEndpoints() {
	sv.HTTPHandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/x-icon")
		cssPath := path.Join("templates", r.URL.Path)
		content, err := web.TemplateFS.ReadFile(cssPath)
		if err != nil {
			http.NotFound(w, r)
			// #nosec G705 -- written after http.NotFound (text/plain + nosniff), not rendered as HTML.
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		// #nosec G705 -- content is from the embedded template FS served as image/x-icon, not user input.
		_, _ = w.Write(content)
	})
	sv.HTTPHandleFunc("/css/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css")
		cssPath := path.Join("templates", r.URL.Path)
		content, err := web.TemplateFS.ReadFile(cssPath)
		if err != nil {
			http.NotFound(w, r)
			// #nosec G705 -- written after http.NotFound (text/plain + nosniff), not rendered as HTML.
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		// #nosec G705 -- content is from the embedded template FS served as text/css, not user input.
		_, _ = w.Write(content)
	})

	sv.HTTPHandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		_ = web.Templates.ExecuteTemplate(w, "isok.html", true)
	})

	sv.HTTPHandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		sv.readyMu.RLock()
		checks := sv.readyChecks
		sv.readyMu.RUnlock()
		for _, check := range checks {
			if err := check(); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = web.Templates.ExecuteTemplate(w, "isok.html", false)
				return
			}
		}
		_ = web.Templates.ExecuteTemplate(w, "isok.html", true)
	})

	sv.HTTPHandleFunc("/config", viperdebug.HandlerFunc(sv.reg))

	sv.HTTPHandleFunc("/version", versionHandler)
}

// versionHandler renders the binary's build identity as JSON. Uniform
// across every multigres service since servenv registers it for all.
func versionHandler(w http.ResponseWriter, _ *http.Request) {
	snap := version.Read()
	payload := struct {
		Version    string `json:"version"`
		Revision   string `json:"revision,omitempty"`
		Modified   bool   `json:"modified"`
		CommitTime string `json:"commit_time,omitempty"`
		GoVersion  string `json:"go_version,omitempty"`
		MainPath   string `json:"main_path,omitempty"`
	}{
		Version:   version.Version,
		Revision:  snap.Revision,
		Modified:  snap.Modified,
		GoVersion: snap.GoVersion,
		MainPath:  snap.MainPath,
	}
	if !snap.CommitTime.IsZero() {
		payload.CommitTime = snap.CommitTime.UTC().Format(time.RFC3339)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}
