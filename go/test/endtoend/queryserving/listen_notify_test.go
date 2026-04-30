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

package queryserving

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// TestListenNotify verifies LISTEN/NOTIFY works through the multigateway.
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestListenNotify(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")

			t.Run("basic_listen_notify", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "LISTEN test_channel")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "NOTIFY test_channel, 'hello'")
				require.NoError(t, err)

				notification, err := listener.WaitForNotification(ctx)
				require.NoError(t, err)
				assert.Equal(t, "test_channel", notification.Channel)
				assert.Equal(t, "hello", notification.Payload)
			})

			t.Run("notify_with_empty_payload", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "LISTEN empty_payload")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "NOTIFY empty_payload")
				require.NoError(t, err)

				notification, err := listener.WaitForNotification(ctx)
				require.NoError(t, err)
				assert.Equal(t, "empty_payload", notification.Channel)
				assert.Equal(t, "", notification.Payload)
			})

			t.Run("multiple_channels", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "LISTEN chan_a")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "LISTEN chan_b")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "NOTIFY chan_a, 'from_a'")
				require.NoError(t, err)
				_, err = notifier.Exec(ctx, "NOTIFY chan_b, 'from_b'")
				require.NoError(t, err)

				n1, err := listener.WaitForNotification(ctx)
				require.NoError(t, err)
				n2, err := listener.WaitForNotification(ctx)
				require.NoError(t, err)

				channels := map[string]string{n1.Channel: n1.Payload, n2.Channel: n2.Payload}
				assert.Equal(t, "from_a", channels["chan_a"])
				assert.Equal(t, "from_b", channels["chan_b"])
			})

			t.Run("unlisten", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "LISTEN unlisten_test")
				require.NoError(t, err)

				_, err = listener.Exec(ctx, "UNLISTEN unlisten_test")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "NOTIFY unlisten_test, 'should_not_arrive'")
				require.NoError(t, err)

				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 2*time.Second)
				defer timeoutCancel()
				_, err = listener.WaitForNotification(timeoutCtx)
				assert.Error(t, err, "should not receive notification after UNLISTEN")
			})

			t.Run("unlisten_all", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "LISTEN ua_chan1")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "LISTEN ua_chan2")
				require.NoError(t, err)

				_, err = listener.Exec(ctx, "UNLISTEN *")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "NOTIFY ua_chan1")
				require.NoError(t, err)
				_, err = notifier.Exec(ctx, "NOTIFY ua_chan2")
				require.NoError(t, err)

				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 2*time.Second)
				defer timeoutCancel()
				_, err = listener.WaitForNotification(timeoutCtx)
				assert.Error(t, err, "should not receive notifications after UNLISTEN *")
			})

			t.Run("listen_in_transaction_commit", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "BEGIN")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "LISTEN txn_channel")
				require.NoError(t, err)

				// Notify before commit — listener shouldn't receive yet (LISTEN not committed)
				_, err = notifier.Exec(ctx, "NOTIFY txn_channel, 'before_commit'")
				require.NoError(t, err)

				_, err = listener.Exec(ctx, "COMMIT")
				require.NoError(t, err)

				// Send after commit
				_, err = notifier.Exec(ctx, "NOTIFY txn_channel, 'after_commit'")
				require.NoError(t, err)

				notification, err := listener.WaitForNotification(ctx)
				require.NoError(t, err)
				assert.Equal(t, "txn_channel", notification.Channel)
				assert.Equal(t, "after_commit", notification.Payload)
			})

			t.Run("listen_in_transaction_rollback", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "BEGIN")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "LISTEN rollback_channel")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "ROLLBACK")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "NOTIFY rollback_channel, 'should_not_arrive'")
				require.NoError(t, err)

				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 2*time.Second)
				defer timeoutCancel()
				_, err = listener.WaitForNotification(timeoutCtx)
				assert.Error(t, err, "should not receive notification after ROLLBACK")
			})

			t.Run("listen_then_unlisten_in_transaction", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				// LISTEN then UNLISTEN same channel in one transaction — net: not subscribed.
				_, err = listener.Exec(ctx, "BEGIN")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "LISTEN cancel_ch")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "UNLISTEN cancel_ch")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "COMMIT")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "NOTIFY cancel_ch, 'should_not_arrive'")
				require.NoError(t, err)

				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 2*time.Second)
				defer timeoutCancel()
				_, err = listener.WaitForNotification(timeoutCtx)
				assert.Error(t, err, "should not receive notification when LISTEN is cancelled by UNLISTEN in same transaction")
			})

			t.Run("listen_then_unlisten_all_in_transaction", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				// LISTEN then UNLISTEN * in one transaction — net: not subscribed.
				_, err = listener.Exec(ctx, "BEGIN")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "LISTEN cancel_all_ch")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "UNLISTEN *")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "COMMIT")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "NOTIFY cancel_all_ch, 'should_not_arrive'")
				require.NoError(t, err)

				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 2*time.Second)
				defer timeoutCancel()
				_, err = listener.WaitForNotification(timeoutCtx)
				assert.Error(t, err, "should not receive notification when LISTEN is cancelled by UNLISTEN * in same transaction")
			})

			t.Run("unlisten_all_then_listen_in_transaction", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				// Pre-listen on a channel, then in a transaction: UNLISTEN * + LISTEN new channel.
				_, err = listener.Exec(ctx, "LISTEN old_ch")
				require.NoError(t, err)

				_, err = listener.Exec(ctx, "BEGIN")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "UNLISTEN *")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "LISTEN new_ch")
				require.NoError(t, err)
				_, err = listener.Exec(ctx, "COMMIT")
				require.NoError(t, err)

				// old_ch should no longer deliver.
				_, err = notifier.Exec(ctx, "NOTIFY old_ch, 'should_not_arrive'")
				require.NoError(t, err)

				// new_ch should deliver.
				_, err = notifier.Exec(ctx, "NOTIFY new_ch, 'hello_new'")
				require.NoError(t, err)

				notification, err := listener.WaitForNotification(ctx)
				require.NoError(t, err)
				assert.Equal(t, "new_ch", notification.Channel)
				assert.Equal(t, "hello_new", notification.Payload)
			})

			t.Run("multiple_listeners_same_channel", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener1, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener1.Close(ctx)

				listener2, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener2.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener1.Exec(ctx, "LISTEN shared_channel")
				require.NoError(t, err)
				_, err = listener2.Exec(ctx, "LISTEN shared_channel")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "NOTIFY shared_channel, 'broadcast'")
				require.NoError(t, err)

				n1, err := listener1.WaitForNotification(ctx)
				require.NoError(t, err)
				assert.Equal(t, "broadcast", n1.Payload)

				n2, err := listener2.WaitForNotification(ctx)
				require.NoError(t, err)
				assert.Equal(t, "broadcast", n2.Payload)
			})

			t.Run("notify_via_pg_notify", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "LISTEN pg_notify_test")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "SELECT pg_notify('pg_notify_test', 'via_function')")
				require.NoError(t, err)

				notification, err := listener.WaitForNotification(ctx)
				require.NoError(t, err)
				assert.Equal(t, "pg_notify_test", notification.Channel)
				assert.Equal(t, "via_function", notification.Payload)
			})

			t.Run("pg_notify_null_payload", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "LISTEN null_payload")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "SELECT pg_notify('null_payload', NULL)")
				require.NoError(t, err)

				notification, err := listener.WaitForNotification(ctx)
				require.NoError(t, err)
				assert.Equal(t, "null_payload", notification.Channel)
				assert.Equal(t, "", notification.Payload)
			})

			t.Run("pg_notify_empty_channel_error", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				conn, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer conn.Close(ctx)

				_, err = conn.Exec(ctx, "SELECT pg_notify('', 'msg')")
				assert.Error(t, err, "empty channel name should be rejected")
			})

			t.Run("pg_notify_null_channel_error", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				conn, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer conn.Close(ctx)

				_, err = conn.Exec(ctx, "SELECT pg_notify(NULL, 'msg')")
				assert.Error(t, err, "NULL channel name should be rejected")
			})

			t.Run("pg_notification_queue_usage", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				conn, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer conn.Close(ctx)

				var usage float64
				err = conn.QueryRow(ctx, "SELECT pg_notification_queue_usage()").Scan(&usage)
				require.NoError(t, err)
				assert.Equal(t, float64(0), usage)
			})

			t.Run("channel_name_too_long", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				// PostgreSQL truncates channel names to NAMEDATALEN-1 (63 chars).
				// Both LISTEN and NOTIFY should truncate to the same value, so delivery works.
				longName := "ch_long_name_padding_to_exceed_63_characters_xxxxxxxxxxxxxxxxxx64"
				truncated := longName[:63]

				_, err = listener.Exec(ctx, "LISTEN "+longName)
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, fmt.Sprintf("NOTIFY %s, 'long'", longName))
				require.NoError(t, err)

				notification, err := listener.WaitForNotification(ctx)
				require.NoError(t, err)
				assert.Equal(t, truncated, notification.Channel)
				assert.Equal(t, "long", notification.Payload)
			})

			t.Run("rapid_notifications", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listener, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "LISTEN rapid")
				require.NoError(t, err)

				count := 100
				for i := range count {
					_, err = notifier.Exec(ctx, fmt.Sprintf("NOTIFY rapid, '%d'", i))
					require.NoError(t, err)
				}

				received := 0
				for received < count {
					_, err := listener.WaitForNotification(ctx)
					require.NoError(t, err)
					received++
				}
				assert.Equal(t, count, received)
			})

			t.Run("extended_protocol_listen_notify", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				// Force extended query protocol by using QueryExecModeDescribeExec.
				// This ensures LISTEN goes through Parse/Bind/Execute, not simple query.
				listenerCfg, err := pgx.ParseConfig(connStr)
				require.NoError(t, err)
				listenerCfg.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec

				listener, err := pgx.ConnectConfig(ctx, listenerCfg)
				require.NoError(t, err)
				defer listener.Close(ctx)

				// Notifier can use simple protocol — we're testing the listener path.
				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "LISTEN test_extended")
				require.NoError(t, err)

				_, err = notifier.Exec(ctx, "NOTIFY test_extended, 'extended_payload'")
				require.NoError(t, err)

				notification, err := listener.WaitForNotification(ctx)
				require.NoError(t, err)
				assert.Equal(t, "test_extended", notification.Channel)
				assert.Equal(t, "extended_payload", notification.Payload)
			})

			// listen_with_concurrent_queries exercises the contention
			// shape the bufMu race fix targets. The race is on the
			// SERVER side: the listener conn's synchronous query
			// handler and its async notification pusher both write
			// into the same bufferedWriter. Before startPacket /
			// writePacket held bufMu across body encoding, the two
			// could interleave byte-by-byte and produce torn packets
			// on the listener's socket.
			//
			// On the client, pgx's pgconn buffers NotificationResponse
			// messages it sees during normal reads, so a single tight
			// query loop on the listener conn already exercises the
			// race: each query reply runs the synchronous write path,
			// concurrent NOTIFYs from a sibling connection drive the
			// server-side async pusher, and pgx will surface a decode
			// error on Query if the bytes on the wire are torn. We do
			// not run a separate WaitForNotification goroutine because
			// pgx forbids concurrent use of a single Conn ("conn
			// busy") — the race is server-side, so client-side
			// concurrency adds no signal.
			t.Run("listen_with_concurrent_queries", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				listenerCfg, err := pgx.ParseConfig(connStr)
				require.NoError(t, err)
				// Force extended protocol so the listener's reply
				// path goes through Parse/Bind/Describe/Execute —
				// the helpers that share bufferedWriter with the
				// async notification pusher.
				listenerCfg.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec

				listener, err := pgx.ConnectConfig(ctx, listenerCfg)
				require.NoError(t, err)
				defer listener.Close(ctx)

				notifier, err := pgx.Connect(ctx, connStr)
				require.NoError(t, err)
				defer notifier.Close(ctx)

				_, err = listener.Exec(ctx, "LISTEN race_channel")
				require.NoError(t, err)

				// Soak window long enough for a torn packet to
				// surface against unsynchronized writers, but short
				// enough to stay under the test deadline.
				soakCtx, soakCancel := context.WithTimeout(ctx, 5*time.Second)
				defer soakCancel()

				var (
					wg          sync.WaitGroup
					queryCount  atomic.Int64
					notifyCount atomic.Int64
					queryErr    atomic.Value
					notifyErr   atomic.Value
				)

				wg.Go(func() {
					for soakCtx.Err() == nil {
						var n int
						if err := listener.QueryRow(soakCtx, "SELECT $1::int4", 1).Scan(&n); err != nil {
							if soakCtx.Err() != nil {
								return
							}
							queryErr.Store(err)
							return
						}
						if n != 1 {
							queryErr.Store(fmt.Errorf("unexpected scan: got %d, want 1", n))
							return
						}
						queryCount.Add(1)
					}
				})

				wg.Go(func() {
					for soakCtx.Err() == nil {
						if _, err := notifier.Exec(soakCtx, "NOTIFY race_channel, 'p'"); err != nil {
							if soakCtx.Err() != nil {
								return
							}
							notifyErr.Store(err)
							return
						}
						notifyCount.Add(1)
					}
				})

				wg.Wait()

				if v := queryErr.Load(); v != nil {
					t.Fatalf("query loop failed (torn packet on listener reply path?): %v", v.(error))
				}
				if v := notifyErr.Load(); v != nil {
					t.Fatalf("notifier loop failed: %v", v.(error))
				}

				assert.Greater(t, queryCount.Load(), int64(0), "query loop made no progress")
				assert.Greater(t, notifyCount.Load(), int64(0), "notifier loop made no progress")

				// Drain a few notifications after the soak: pgx
				// buffered them during the query loop, and pulling
				// them confirms the server's async pusher path also
				// produced well-formed packets.
				drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
				defer drainCancel()
				notif, err := listener.WaitForNotification(drainCtx)
				require.NoError(t, err, "drain WaitForNotification (torn NotificationResponse?)")
				assert.Equal(t, "race_channel", notif.Channel)
				assert.Equal(t, "p", notif.Payload)
			})
		})
	}
}
