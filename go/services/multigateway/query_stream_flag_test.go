// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package multigateway

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestQueryStreamReuseFlag(t *testing.T) {
	mg := NewMultigateway()
	require.True(t, mg.queryStreamReuse.Default())
	fs := pflag.NewFlagSet("query-stream-test", pflag.ContinueOnError)
	mg.RegisterFlags(fs)
	require.NoError(t, fs.Parse([]string{"--query-stream-reuse=false"}))
	require.False(t, mg.queryStreamReuse.Get())
}
