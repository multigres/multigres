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

package web

import (
	"embed"
	"text/template"
)

//go:embed templates
var TemplateFS embed.FS
var Templates *template.Template

func init() {
	var err error
	Templates, err = template.ParseFS(TemplateFS, "templates/*.html")
	if err != nil {
		panic(err)
	}
}
