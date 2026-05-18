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

package protoutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReasonLogicalReplication(t *testing.T) {
	// Value must be the next power of two after the existing reasons.
	assert.Equal(t, uint32(32), ReasonLogicalReplication)

	// Must be part of the valid reasons mask so ValidateReasons accepts it.
	assert.NoError(t, ValidateReasons(ReasonLogicalReplication))

	// Helper introspection.
	assert.True(t, HasLogicalReplicationReason(ReasonLogicalReplication))
	assert.False(t, HasLogicalReplicationReason(ReasonTransaction))

	// Combined with other reasons should still work.
	combined := AddReason(ReasonTransaction, ReasonLogicalReplication)
	assert.True(t, HasLogicalReplicationReason(combined))
	assert.True(t, HasTransactionReason(combined))

	// String representation.
	assert.Equal(t, "logical_replication", ReasonsString(ReasonLogicalReplication))
	assert.Equal(t, "transaction|logical_replication", ReasonsString(combined))
}
