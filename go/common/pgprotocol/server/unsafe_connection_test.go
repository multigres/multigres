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

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
)

// TestExtractUnsafeConnectionParam covers the connect-time parse: the parameter
// is stripped from the params map (so it never reaches a backend), a truthy value
// latches unsafe connection on the connection, a falsy value does not, and a
// malformed value is a FATAL. It runs against both the current parameter name and
// the deprecated multigres.direct_connection alias, which must behave identically.
func TestExtractUnsafeConnectionParam(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		present   bool
		wantLatch bool
		wantErr   bool
	}{
		{name: "absent", present: false},
		{name: "on", value: "on", present: true, wantLatch: true},
		{name: "true", value: "true", present: true, wantLatch: true},
		{name: "1", value: "1", present: true, wantLatch: true},
		{name: "off", value: "off", present: true, wantLatch: false},
		{name: "false", value: "false", present: true, wantLatch: false},
		{name: "garbage", value: "maybe", present: true, wantErr: true},
	}
	params := map[string]string{
		"unsafe_connection":        constants.UnsafeConnectionParam,
		"direct_connection(alias)": constants.DirectConnectionParam,
	}
	for paramLabel, paramName := range params {
		for _, tt := range tests {
			t.Run(paramLabel+"/"+tt.name, func(t *testing.T) {
				c := &Conn{params: map[string]string{}}
				if tt.present {
					c.params[paramName] = tt.value
				}

				err := c.extractUnsafeConnectionParam()

				if tt.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tt.wantLatch, c.UnsafeConnection())
				// Always stripped, whatever the value, so it never flows to a backend.
				_, stillPresent := c.params[paramName]
				assert.False(t, stillPresent, "param must be stripped from c.params")
			})
		}
	}
}
