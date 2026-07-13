// Copyright 2026 Supabase, Inc.
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

package servenv

// versionName is the Multigres release version. It is a property of the source
// tree — bumped in this file when cutting a release, carrying a "-SNAPSHOT"
// suffix in between — deliberately NOT derived from git tags at build time: a
// tag is created after (and often on a different branch from) the commit it
// names, so a build of a given commit cannot reliably infer its own release
// version from `git describe`. Keeping it in source makes the version a stable
// property of the commit, independent of the build environment or branch.
const versionName = "0.1.0-SNAPSHOT"
