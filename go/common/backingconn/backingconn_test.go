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

package backingconn

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backingconn/backingconntest"
)

func TestValidate(t *testing.T) {
	for _, tc := range backingconntest.Cases() {
		t.Run(tc.Name, func(t *testing.T) {
			err := Validate(tc.Host, tc.Port, tc.Database, tc.User)
			if tc.Refused == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.Refused)
		})
	}
}
