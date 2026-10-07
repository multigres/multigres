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

package readonly

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModes(t *testing.T) {
	var nilModes *Modes
	require.Equal(t, Mode{}, nilModes.Get("db"), "nil Modes reads as read-write")

	m := New()
	require.Equal(t, Mode{}, m.Get("db"))

	require.Equal(t, Mode{}, m.Set("db", Mode{Enabled: true}))
	require.Equal(t, Mode{Enabled: true}, m.Get("db"))
	require.Equal(t, Mode{}, m.Get("other"), "modes are per database")

	require.Equal(t, Mode{Enabled: true}, m.Set("db", Mode{Enabled: true, Force: true}))
	require.Equal(t, Mode{Enabled: true, Force: true}, m.Set("db", Mode{}))
	require.Equal(t, Mode{}, m.Get("db"))
	require.Empty(t, m.dbs, "clearing a mode drops the entry")
}
