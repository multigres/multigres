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

//go:build ruleguard

package gorules

import (
	"github.com/quasilyte/go-ruleguard/dsl"
)

func disallowUnderscoreInFlags(m dsl.Matcher) {
	m.Import("github.com/spf13/pflag")

	m.Match(
		`viperutil.Configure($_, $name, $*_)`,
		`viperutil.Options[$_]{$*_, FlagName: $name, $*_}`).
		Where(m["name"].Text.Matches("_")).
		Report("viper flag name contains an underscore; use dashes instead")

	m.Match(`$fs.$_($name, $*_)`).
		Where(
			m["fs"].Type.Is("*pflag.FlagSet") && m["name"].Text.Matches("_")).
		Report("FlagSet flag name contains an underscore; use dashes instead")

	m.Match(`$fs.$_($ptr, $name, $*_)`).
		Where(
			m["fs"].Type.Is("*pflag.FlagSet") && m["ptr"].Type.HasPointers() && m["name"].Text.Matches("_")).
		Report("FlagSet flag name contains an underscore; use dashes instead")
}
