// Copyright 2026 Supabase, Inc.
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

package pgsettings

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRejectTempSchemaSearchPath(t *testing.T) {
	rejected := []string{
		"pg_temp",
		"pg_temp, public",
		"public, pg_temp",
		"PG_TEMP",
		" pg_temp ",
		`"pg_temp"`,
		"pg_temp_3",
		`"$user", pg_temp, public`,
	}
	for _, value := range rejected {
		assert.ErrorContains(t, RejectTempSchemaSearchPath(value), "pg_temp", "value %q", value)
	}

	allowed := []string{
		"",
		"public",
		`"$user", public`,
		"public, extensions",
		"mypg_temp",
		"temp_pg",
	}
	for _, value := range allowed {
		assert.NoError(t, RejectTempSchemaSearchPath(value), "value %q", value)
	}
}
