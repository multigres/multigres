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

package mterrors

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// TestFromContext checks that an in-process error gets the meaning FromGRPC
// gives the same situation after crossing gRPC.
func TestFromContext(t *testing.T) {
	live := context.Background()
	expired, cancelExpired := context.WithTimeout(context.Background(), 0)
	defer cancelExpired()
	<-expired.Done()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	pgErr := NewPgError("ERROR", "42P01", "relation \"t\" does not exist", "")
	fatal := NewPgError("FATAL", PgSSConnectionFailure, "connection lost", "")
	plain := New(mtrpcpb.Code_INTERNAL, "boom")

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want error
	}{
		{"nil stays nil", expired, nil, nil},
		{"deadline passed is a statement timeout", expired, pgErr, NewStatementTimeout()},
		{"deadline error is a statement timeout", live, context.DeadlineExceeded, NewStatementTimeout()},
		{"FATAL is kept at the deadline", expired, fatal, fatal},
		{"cancelled is a cancelled query", cancelled, pgErr, NewQueryCanceled()},
		{"cancellation error is a cancelled query", live, context.Canceled, NewQueryCanceled()},
		{"PostgreSQL error passes through", live, pgErr, pgErr},
		{"plain error passes through", live, plain, plain},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FromContext(tt.ctx, tt.err)
			if tt.want == nil {
				assert.NoError(t, got)
				return
			}
			assert.Equal(t, tt.want.Error(), got.Error())
		})
	}

	// The same situations, as the gRPC path sees them.
	assert.Equal(t, NewStatementTimeout().Error(), FromGRPC(status.Error(codes.DeadlineExceeded, "x")).Error())
	assert.Equal(t, NewQueryCanceled().Error(), FromGRPC(status.Error(codes.Canceled, "x")).Error())
	assert.True(t, errors.Is(FromContext(live, pgErr), pgErr), "a passed-through error keeps its identity")
}
