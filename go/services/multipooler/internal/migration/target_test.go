// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
)

func TestDropTablesSQL(t *testing.T) {
	// No tables → no statement (DropTables no-ops on this).
	require.Equal(t, "", dropTablesSQL(nil))
	require.Equal(t, "", dropTablesSQL([]string{}))

	// Safe lowercase identifiers are not quoted; the drop is one IF EXISTS …
	// CASCADE over all tables so a re-run over pre-existing target tables (and any
	// dependents) succeeds.
	require.Equal(t, "DROP TABLE IF EXISTS public.orders CASCADE",
		dropTablesSQL([]string{"public.orders"}))
	require.Equal(t, "DROP TABLE IF EXISTS public.orders, app.items CASCADE",
		dropTablesSQL([]string{"public.orders", "app.items"}))

	// Identifiers needing quoting (mixed case / reserved) are quoted per part.
	got := dropTablesSQL([]string{"public.Order"})
	require.Contains(t, got, `"Order"`)
	require.True(t, strings.HasPrefix(got, "DROP TABLE IF EXISTS "))
	require.True(t, strings.HasSuffix(got, " CASCADE"))
}

func TestCreateLogicalSlot_RequestsFailover(t *testing.T) {
	qs := &fakeQS{}
	tg := newTarget(qs)

	require.NoError(t, tg.CreateLogicalSlot(context.Background(), "mt_sub_1"))

	require.Len(t, qs.adminArgs, 1)
	require.Equal(t, "SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)",
		qs.adminArgs[0].sql)
	require.Equal(t, []any{"mt_sub_1"}, qs.adminArgs[0].args)
}

func TestWaitSlotConfirmed(t *testing.T) {
	const slotConfirmedSQL = "SELECT confirmed_flush_lsn >= $1::pg_lsn FROM pg_replication_slots WHERE slot_name = $2"

	t.Run("missing slot fails fast, not retried", func(t *testing.T) {
		// emptyResult() (0 rows) is what fakeQS returns for any un-scripted query —
		// exactly what pg_replication_slots reports once the slot is gone, so this
		// also doubles as the regression case for the old "no rows == not yet"
		// behavior that polled forever instead of erroring.
		qs := &fakeQS{}
		tg := newTarget(qs)

		err := tg.WaitSlotConfirmed(context.Background(), "mt_sub_1", "0/100")
		require.ErrorContains(t, err, `replication slot "mt_sub_1" is missing`)
	})

	t.Run("not yet confirmed keeps polling until ctx is done", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			slotConfirmedSQL: {Rows: []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value("f")}}}},
		}}
		tg := newTarget(qs)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := tg.WaitSlotConfirmed(ctx, "mt_sub_1", "0/100")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("confirmed returns nil", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
			slotConfirmedSQL: {Rows: []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value("t")}}}},
		}}
		tg := newTarget(qs)

		require.NoError(t, tg.WaitSlotConfirmed(context.Background(), "mt_sub_1", "0/100"))
	})
}

func TestSlotReady(t *testing.T) {
	const slotReadySQL = "SELECT NOT temporary AND invalidation_reason IS NULL FROM pg_replication_slots WHERE slot_name = $1"

	cases := []struct {
		name string
		rows []*sqltypes.Row
		want bool
	}{
		{"missing slot is not ready", nil, false},
		{"ready", []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value("t")}}}, true},
		{"not ready (still temporary or invalidated)", []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value("f")}}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
				slotReadySQL: {Rows: c.rows},
			}}
			tg := newTarget(qs)
			ready, err := tg.SlotReady(context.Background(), "mt_sub_1")
			require.NoError(t, err)
			require.Equal(t, c.want, ready)
		})
	}
}

func TestCreateSubscriptionSQL(t *testing.T) {
	dsn := "host=gw port=5432 dbname=postgres"

	// Default (slotName ""): the subscription creates its own slot. Safe
	// lowercase identifiers are not quoted.
	got := createSubscriptionSQL("mt_sub", dsn, "mt_pub", true, "")
	require.Contains(t, got, "CREATE SUBSCRIPTION mt_sub ")
	require.Contains(t, got, "PUBLICATION mt_pub ")
	require.Contains(t, got, "copy_data = true")
	require.NotContains(t, got, "create_slot")
	require.NotContains(t, got, "slot_name")

	// EXPORT reverse subscription: attaches to a pre-created slot, no copy.
	got = createSubscriptionSQL("mt_sub", dsn, "mt_pub", false, "mt_sub")
	require.Contains(t, got, "copy_data = false")
	require.Contains(t, got, "create_slot = false")
	require.Contains(t, got, "slot_name = mt_sub")
}
