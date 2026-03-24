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

package poolergateway

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

func TestClassifyError(t *testing.T) {
	primaryTarget := &query.Target{
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	replicaTarget := &query.Target{
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}

	tests := []struct {
		name   string
		err    error
		target *query.Target
		want   errorAction
	}{
		{
			name:   "MTF01 on PRIMARY triggers buffering",
			err:    mterrors.MTF01.New(),
			target: primaryTarget,
			want:   actionBuffer,
		},
		{
			name:   "MTF01 on REPLICA does not buffer",
			err:    mterrors.MTF01.New(),
			target: replicaTarget,
			want:   actionFail,
		},
		{
			name:   "generic error on PRIMARY does not buffer",
			err:    errors.New("connection refused"),
			target: primaryTarget,
			want:   actionFail,
		},
		{
			name:   "nil error on PRIMARY does not buffer",
			err:    nil,
			target: primaryTarget,
			want:   actionFail,
		},
		{
			name:   "read_only_sql_transaction on PRIMARY triggers buffering",
			err:    mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction, "cannot execute INSERT in a read-only transaction", ""),
			target: primaryTarget,
			want:   actionBuffer,
		},
		{
			name:   "read_only_sql_transaction on REPLICA does not buffer",
			err:    mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction, "cannot execute INSERT in a read-only transaction", ""),
			target: replicaTarget,
			want:   actionFail,
		},
		{
			name:   "other MT error on PRIMARY does not buffer",
			err:    mterrors.MTB01.New(),
			target: primaryTarget,
			want:   actionFail,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyError(tt.err, tt.target)
			assert.Equal(t, tt.want, got)
		})
	}
}
