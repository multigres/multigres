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
	"context"
	"errors"

	"github.com/google/uuid"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

type restoreOperationKey struct{}

// NewRestoreOperation snapshots the CAS predecessor. This is NOT a permit to
// delete PGDATA: only the sidecar can admit it and perform cleanup under its
// exclusion boundary. The manager persists it before sending Run.
func (e *Engine) NewRestoreOperation(ctx context.Context, cleanup bool) (*pb.RestoreOperation, error) {
	st, err := e.settings.Sidecar.Status(ctx)
	if err != nil {
		return nil, err
	}
	if st.GetRestoreIncarnation() == "" {
		return nil, errors.New("sidecar does not support fenced restore")
	}
	return &pb.RestoreOperation{Id: uuid.NewString(), Sequence: st.GetRestoreResult().GetOperation().GetSequence() + 1, Predecessor: st.GetRestoreResult().GetOperation().GetId(), Incarnation: st.RestoreIncarnation, Cleanup: cleanup}, nil
}

func WithRestoreOperation(ctx context.Context, op *pb.RestoreOperation) context.Context {
	return context.WithValue(ctx, restoreOperationKey{}, op)
}

func RestoreOperationFromContext(ctx context.Context) *pb.RestoreOperation {
	op, _ := ctx.Value(restoreOperationKey{}).(*pb.RestoreOperation)
	return op
}
