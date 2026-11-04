// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package latencydiag

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationPropagationPreservesBaggageAndDoesNotLeak(t *testing.T) {
	c := capture(t, 1, 100)
	root := Message(context.Background(), 19, 9)
	carrier := map[string]string{"baggage": "existing=value"}
	InjectCarrier(root.Context(), carrier)
	require.Contains(t, carrier["baggage"], "existing=value")
	ctx, span := ExtractCarrier(context.Background(), carrier)
	require.NotNil(t, span)
	_, child := Start(ctx, "backend")
	child.End(nil, 0)
	span.End(nil, 0)
	root.End(nil, 0)
	_, next := ExtractCarrier(context.Background(), map[string]string{})
	require.Nil(t, next, "identity must not persist across reused operations")
	report := c.Stop()
	require.Len(t, report.Events, 3)
	require.NotEmpty(t, report.Events[1].RemoteParent)
	_, late := ExtractCarrier(context.Background(), carrier)
	require.Nil(t, late)
}
