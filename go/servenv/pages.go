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
	"path/filepath"

	viperdebug "github.com/multigres/multigres/go/viperutil/debug"
	"github.com/multigres/multigres/go/web"
)

// The init function sets up all the necessary frameworks for serving pages.
// You can define go html templates in go/web/templates, and they can use css
// files from go/web/templates/css, which contains a minimal pico download.
// You can use an existing html file as an example to create your own.
// Currently: Headings, tables and grids are supported. If other tags don't
// render correctly, we may need to add more css styles.
// We are using a classless css approach to minimize complexity.

func init() {
	HTTPHandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/x-icon")
		cssPath := path.Join("templates", r.URL.Path)
		content, err := web.TemplateFS.ReadFile(cssPath)
		if err != nil {
			http.NotFound(w, r)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = w.Write(content)
	})
	HTTPHandleFunc("/css/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css")
		cssPath := path.Join("templates", r.URL.Path)
		content, err := web.TemplateFS.ReadFile(cssPath)
		if err != nil {
			http.NotFound(w, r)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = w.Write(content)
	})

	HTTPHandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		type Link struct {
			Title       string
			Description string
			Link        string
		}
		type IndexData struct {
			Title string
			Links []Link
		}

		indexData := IndexData{
			Title: filepath.Base(os.Args[0]),
			Links: []Link{
				{"Config", "Server configuration details", "/config"},
				{"Live", "URL for liveness check", "/live"},
			},
		}
		_ = web.Templates.ExecuteTemplate(w, "index.html", indexData)
	})

	HTTPHandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		_ = web.Templates.ExecuteTemplate(w, "isok.html", true)
	})

	HTTPHandleFunc("/config", viperdebug.HandlerFunc)
}
