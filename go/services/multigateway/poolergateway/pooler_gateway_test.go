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
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/pb/query"
)

func TestClassifyError(t *testing.T) {
	primaryTarget := &query.Target{Mode: query.Mode_MODE_WRITABLE}
	replicaTarget := &query.Target{Mode: query.Mode_MODE_INCONSISTENT}

	tests := []struct {
		name               string
		err                error
		target             *query.Target
		retryReadOnlyError bool
		want               errorAction
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
			name:               "read_only_sql_transaction on retryable PRIMARY triggers buffering",
			err:                mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction, "cannot execute INSERT in a read-only transaction", ""),
			target:             primaryTarget,
			retryReadOnlyError: true,
			want:               actionBuffer,
		},
		{
			name:   "read_only_sql_transaction on stateful PRIMARY does not buffer",
			err:    mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction, "cannot execute INSERT in a read-only transaction", ""),
			target: primaryTarget,
			want:   actionFail,
		},
		{
			name:               "read_only_sql_transaction on REPLICA does not buffer",
			err:                mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction, "cannot execute INSERT in a read-only transaction", ""),
			target:             replicaTarget,
			retryReadOnlyError: true,
			want:               actionFail,
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
			got := classifyError(tt.err, tt.target, tt.retryReadOnlyError)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestIsSingleQuery covers the classification that decides whether a request
// skips proactive failover buffering. Only a request with no existing reserved
// connection AND that will not create one is a single query. The scenarios are
// labelled by the handler input that produces each (reservedConnID, willReserve)
// pair, so this also documents what each handler passes:
//
//   - StreamExecute:        willReserve = ReservationOptions != nil
//   - ExecuteQuery/Describe: willReserve = false (no reservation path)
//   - PortalStreamExecute:   willReserve = MaxRows > 0 (suspendable cursor)
//   - CopyReady/CopyOutReady/GetAuthCredentials: always proactively buffered
//     (pass singleQuery=false directly; not via this helper)
func TestIsSingleQuery(t *testing.T) {
	tests := []struct {
		name           string
		reservedConnID uint64
		willReserve    bool
		want           bool
	}{
		{"StreamExecute autocommit (no reservation, no conn)", 0, false, true},
		{"ExecuteQuery/Describe standalone (no conn)", 0, false, true},
		{"PortalStreamExecute fetch-all (MaxRows==0, no conn)", 0, false, true},
		{"StreamExecute new transaction (reservation requested)", 0, true, false},
		{"PortalStreamExecute cursor (MaxRows>0)", 0, true, false},
		{"on an existing reserved connection (never a single query)", 42, false, false},
		{"existing reserved conn + would reserve", 42, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isSingleQuery(tt.reservedConnID, tt.willReserve))
		})
	}
}

func TestRetryReadOnlyError(t *testing.T) {
	txn := func(begin string) *query.ReservationOptions {
		return &query.ReservationOptions{Reasons: protoutil.ReasonTransaction, BeginQuery: begin}
	}
	readOnlyDefault := &query.ExecuteOptions{SessionSettings: map[string]string{"default_transaction_read_only": "on"}}
	readOnlyPrefixDefault := &query.ExecuteOptions{SessionSettings: map[string]string{"default_transaction_read_only": "tr"}}

	tests := []struct {
		name           string
		reservedConnID uint64
		willReserve    bool
		opts           *query.ReservationOptions
		execOptions    *query.ExecuteOptions
		want           bool
	}{
		{"single autocommit query", 0, false, nil, nil, true},
		{"single autocommit query with read-only default", 0, false, nil, readOnlyDefault, false},
		{"single autocommit query with read-only prefix default", 0, false, nil, readOnlyPrefixDefault, false},
		{"deferred read-write transaction", 0, true, txn("START TRANSACTION READ WRITE"), nil, true},
		{"deferred read-write transaction overrides read-only default", 0, true, txn("START TRANSACTION READ WRITE"), readOnlyDefault, true},
		{"deferred plain transaction", 0, true, txn("BEGIN"), nil, true},
		{"deferred plain transaction with read-only default", 0, true, txn("BEGIN"), readOnlyDefault, false},
		{"deferred read-only transaction", 0, true, txn("START TRANSACTION READ ONLY"), nil, false},
		{"deferred read-only transaction with semicolon", 0, true, txn("START TRANSACTION READ ONLY;"), nil, false},
		{"deferred read-only transaction with isolation", 0, true, txn("START TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY;"), nil, false},
		{"deferred read-write transaction with isolation", 0, true, txn("START TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE;"), nil, true},
		{"deferred transaction uses last read-only mode", 0, true, txn("BEGIN READ WRITE READ ONLY"), nil, false},
		{"deferred transaction uses last read-write mode", 0, true, txn("BEGIN READ ONLY READ WRITE"), nil, true},
		{"deferred transaction with unknown begin", 0, true, txn(""), nil, false},
		{"existing reserved transaction", 42, false, nil, nil, false},
		{"non-transaction reservation", 0, true, &query.ReservationOptions{Reasons: protoutil.ReasonTempTable}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, retryReadOnlyError(tt.reservedConnID, tt.willReserve, tt.opts, tt.execOptions))
		})
	}
}
