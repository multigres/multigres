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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
)

// TestUnloggedTableWarningEmitsNotice verifies the primitive emits a single
// WARNING NoticeResponse (no CommandComplete) pointing at the failover doc.
func TestUnloggedTableWarningEmitsNotice(t *testing.T) {
	p := NewUnloggedTableWarning("CREATE UNLOGGED TABLE x (i int)")

	var got []*sqltypes.Result
	err := p.StreamExecute(context.Background(), nil, nil, nil, nil,
		func(_ context.Context, r *sqltypes.Result) error {
			got = append(got, r)
			return nil
		})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Empty(t, got[0].CommandTag, "notice-only result must not emit a CommandComplete")
	require.Len(t, got[0].Notices, 1)

	n := got[0].Notices[0]
	assert.True(t, n.IsNotice(), "must be a NoticeResponse, not an ErrorResponse")
	assert.Equal(t, "WARNING", n.Severity)
	assert.Equal(t, "01000", n.Code)
	assert.Contains(t, n.Message, "failover")
	assert.Contains(t, n.Hint, "docs/query_serving/unlogged_tables.md")
}
