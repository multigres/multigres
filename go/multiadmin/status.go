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

package multiadmin

import (
	"fmt"
	"net/http"

	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/web"
)

// Link represents a link on the status page.
type Link struct {
	Title       string
	Description string
	Link        string
}

// Status represents the response from the temporary status endpoint
type Status struct {
	Title string `json:"title"`

	InitError string `json:"init_error"`

	Links []Link `json:"links"`
}

var serverStatus = Status{
	Title: "Multiadmin",
	Links: []Link{
		{"Config", "Server configuration details", "/config"},
		{"Live", "URL for liveness check", "/live"},
		{"Ready", "URL for readiness check", "/ready"},
	},
}

func init() {
	servenv.HTTPHandleFunc("/", handleIndex)
	servenv.HTTPHandleFunc("/ready", handleReady)
}

// handleIndex serves the index page
func handleIndex(w http.ResponseWriter, r *http.Request) {
	err := web.Templates.ExecuteTemplate(w, "admin_index.html", serverStatus)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

func handleReady(w http.ResponseWriter, r *http.Request) {
	isReady := (len(serverStatus.InitError) == 0)
	if !isReady {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	if err := web.Templates.ExecuteTemplate(w, "isok.html", isReady); err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}
