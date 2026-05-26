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

package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputeProcessLimits(t *testing.T) {
	tests := []struct {
		cpus int
		want ProcessLimits
	}{
		{0, ProcessLimits{Global: 1, Backup: 1, Get: 1, Push: 1}},
		{1, ProcessLimits{Global: 1, Backup: 1, Get: 1, Push: 1}},
		{4, ProcessLimits{Global: 1, Backup: 2, Get: 3, Push: 1}},
		{8, ProcessLimits{Global: 2, Backup: 4, Get: 6, Push: 2}},
		{16, ProcessLimits{Global: 4, Backup: 8, Get: 8, Push: 4}},
		{128, ProcessLimits{Global: 4, Backup: 8, Get: 8, Push: 4}},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := ComputeProcessLimits(tt.cpus)
			assert.Equal(t, tt.want, got, "cpus=%d", tt.cpus)
		})
	}
}
