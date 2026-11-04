// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/tools/latencydiag"
)

// diagnosticCallback preserves the child context so downstream work is nested
// under delivery, rather than mistaken for time waiting on PostgreSQL.
// Callers only install this wrapper for a selected diagnostic operation.
func diagnosticCallback(callback func(context.Context, *sqltypes.Result) error) func(context.Context, *sqltypes.Result) error {
	if callback == nil {
		return nil
	}
	return func(ctx context.Context, result *sqltypes.Result) error {
		ctx, span := latencydiag.Start(ctx, "postgres.result_callback")
		err := callback(ctx, result)
		span.End(err, 0)
		return err
	}
}
