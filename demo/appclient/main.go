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

// Command appclient is a stand-in application for demoing the Multigres Migrator
// table-migration flow. It has three subcommands over a shared "ledger"
// workload (an accounts table whose balances only move between rows, so the SUM
// of balances is invariant — a live no-data-loss signal):
//
//	appclient setup --source-dsn '<dsn>' [--accounts N --initial-balance B]
//	    Create and seed the ledger on a database, then exit.
//
//	appclient write --source-dsn '<dsn>' [--target-dsn '<gateway>']
//	    Continuously run random transfers/splits(insert)/merges(delete).
//
//	appclient watch --source-dsn '<dsn>' [--target-dsn '<gateway>']
//	    Render a live terminal summary (accounts, total balance, invariant).
//
// Both write and watch fail over from --source-dsn to --target-dsn on SIGUSR1
// (kill -USR1 <pid>), so a demo can cut the application over to the Multigres
// gateway after the migration reaches steady state and show the invariant still
// holds on the target. Each process uses a single connection and handles the
// failover inline in its own loop, so there is no concurrent connection access.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2) //nolint:forbidigo // main() is allowed to call os.Exit
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "setup":
		err = runSetup(args)
	case "write":
		err = runWrite(args)
	case "watch":
		err = runWatch(args)
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", cmd)
		usage()
		os.Exit(2) //nolint:forbidigo // main() is allowed to call os.Exit
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "appclient %s: %v\n", cmd, err)
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `appclient — ledger workload for the Multigres Migrator migration demo

Usage:
  appclient setup  --source-dsn <dsn> [--accounts N] [--initial-balance B] [--table schema.table]
  appclient write  --source-dsn <dsn> [--target-dsn <gateway>] [--table schema.table] [--interval D] [--transfers-only]
  appclient watch  --source-dsn <dsn> [--target-dsn <gateway>] [--table schema.table] [--interval D] [--limit N]

Send SIGUSR1 to a running write/watch process to fail it over to --target-dsn.
`)
}

// ----- setup -----

func runSetup(args []string) error {
	fs := flag.NewFlagSet("setup", flag.ExitOnError)
	dsn := fs.String("source-dsn", "", "libpq conninfo of the database to seed — required")
	table := fs.String("table", "public.accounts", "ledger table (schema.table)")
	accounts := fs.Int("accounts", 100, "number of accounts to seed")
	initial := fs.Int64("initial-balance", 1000, "starting balance per account")
	_ = fs.Parse(args)
	if *dsn == "" {
		return errors.New("--source-dsn is required")
	}
	tbl, err := quoteTable(*table)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM) //nolint:gocritic // CLI entry point
	defer stop()
	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	return setupLedger(ctx, conn, tbl, *accounts, *initial)
}

// ----- write -----

func runWrite(args []string) error {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	sourceDSN := fs.String("source-dsn", "", "libpq conninfo of the current (source) database — required")
	targetDSN := fs.String("target-dsn", "", "libpq conninfo of the Multigres gateway to fail over to on SIGUSR1")
	table := fs.String("table", "public.accounts", "ledger table (schema.table)")
	accounts := fs.Int("accounts", 100, "seed size hint, used to bound the row count")
	interval := fs.Duration("interval", 50*time.Millisecond, "delay between operations")
	verifyEvery := fs.Duration("verify-interval", 5*time.Second, "how often to verify the balance-sum invariant")
	duration := fs.Duration("duration", 0, "run for this long then exit (0 = until Ctrl-C)")
	transfersOnly := fs.Bool("transfers-only", false, "only move money between existing accounts (never INSERT/DELETE), so the row count stays fixed")
	_ = fs.Parse(args)
	if *sourceDSN == "" {
		return errors.New("--source-dsn is required")
	}
	tbl, err := quoteTable(*table)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM) //nolint:gocritic // CLI entry point
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	ep, err := newEndpoint(ctx, *sourceDSN, *targetDSN)
	if err != nil {
		return err
	}
	defer ep.close()

	wantTotal, count, err := sumAndCount(ctx, ep.conn, tbl)
	if err != nil {
		return fmt.Errorf("read initial ledger state (run setup first?): %w", err)
	}
	fmt.Printf("[%s] write: %s has %d accounts, total %d (invariant)\n", ts(), ep.label, count, wantTotal)

	w := &writer{ep: ep, tbl: tbl, seed: *accounts, wantTotal: wantTotal, transfersOnly: *transfersOnly}
	w.count.Store(count)
	w.loop(ctx, *interval, *verifyEvery)

	// The run ctx is cancelled once the loop returns (--duration or Ctrl-C), so
	// the final verify needs a fresh context.
	fctx, fcancel := context.WithTimeout(context.Background(), 5*time.Second) //nolint:gocritic // final verify after the run ctx is cancelled
	defer fcancel()
	total, cnt, err := sumAndCount(fctx, ep.conn, tbl)
	if err != nil {
		return err
	}
	ok := total == wantTotal
	fmt.Printf("[%s] final on %s: %d accounts, total %d, want %d => %s\n", ts(), ep.label, cnt, total, wantTotal, verdict(ok))
	if !ok {
		return errors.New("balance-sum invariant violated: data lost or duplicated")
	}
	return nil
}

// ----- watch -----

func runWatch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	sourceDSN := fs.String("source-dsn", "", "libpq conninfo of the database to watch — required")
	targetDSN := fs.String("target-dsn", "", "libpq conninfo of the Multigres gateway to fail over to on SIGUSR1")
	table := fs.String("table", "public.accounts", "ledger table (schema.table)")
	interval := fs.Duration("interval", 1*time.Second, "refresh interval")
	duration := fs.Duration("duration", 0, "run for this long then exit (0 = until Ctrl-C)")
	limit := fs.Int("limit", 0, "max rows to show (0 = all accounts; the header reveals column add/remove)")
	_ = fs.Parse(args)
	if *sourceDSN == "" {
		return errors.New("--source-dsn is required")
	}
	tbl, err := quoteTable(*table)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM) //nolint:gocritic // CLI entry point
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	ep, err := newEndpoint(ctx, *sourceDSN, *targetDSN)
	if err != nil {
		return err
	}
	defer ep.close()

	wantTotal, _, err := sumAndCount(ctx, ep.conn, tbl)
	if err != nil {
		return fmt.Errorf("read initial ledger state (run setup first?): %w", err)
	}

	usr1 := make(chan os.Signal, 1)
	signal.Notify(usr1, syscall.SIGUSR1)
	tick := time.NewTicker(*interval)
	defer tick.Stop()

	render := func() {
		ep.maybeReconnect(ctx)     // gateway pane self-heals across a target failover
		fmt.Print("\033[H\033[2J") // clear screen
		if ep.conn.IsClosed() {
			fmt.Printf("appclient watch — endpoint: %s — %s\n", ep.label, ts())
			fmt.Printf("  reconnecting to %s… (waiting for the primary to come back)\n", ep.label)
			return
		}
		fmt.Printf("appclient watch — endpoint: %s — %s\n", ep.label, ts())
		// The header row is the live table shape: an ALTER TABLE ADD/DROP COLUMN
		// on the source shows up here once it replicates. The TOTAL row rolls up
		// the balance column so the no-data-loss invariant is visible at a glance.
		cols, rows, terr := sampleTable(ctx, ep.conn, tbl, *limit)
		total, count, serr := sumAndCount(ctx, ep.conn, tbl)
		var footer []string
		if serr == nil {
			footer = rollupRow(cols, total)
		}
		body, w := "", 48
		if terr != nil {
			body = fmt.Sprintf("  table error: %v\n", terr)
		} else {
			body, w = formatTable(cols, rows, footer)
		}
		bar := strings.Repeat("─", maxInt(w, 48))
		fmt.Println(bar)
		fmt.Print(body)
		fmt.Println(bar)
		if serr != nil {
			fmt.Printf("  balance summary unavailable: %v\n", serr)
		} else {
			fmt.Printf("  accounts: %d · total %d (want %d) · invariant: %s\n",
				count, total, wantTotal, verdict(total == wantTotal))
		}
		fmt.Println(bar)
		// Apply-replication health. Always rendered (so the source and target
		// panes stay symmetric); it shows an error only when the subscription is
		// actually stalling.
		fmt.Printf("  %s\n", applyStatus(ctx, ep.conn))
		fmt.Println(bar)
		fmt.Println("  SIGUSR1 → fail over to target · Ctrl-C → quit")
	}
	render()
	for {
		select {
		case <-ctx.Done():
			fmt.Println()
			return nil
		case <-usr1:
			ep.failover(ctx, tbl, wantTotal)
		case <-tick.C:
			render()
		}
	}
}

// ----- endpoint (swappable active connection + one-way failover) -----
//
// The connection is touched only by the owning loop goroutine; failover is
// handled inline in that same loop (on the SIGUSR1 channel), so no locking is
// needed.
type endpoint struct {
	targetDSN string
	activeDSN string // DSN the current conn dials — reconnect target
	conn      *pgx.Conn
	label     string // "source" or "target"
	failedTo  bool
}

func newEndpoint(ctx context.Context, sourceDSN, targetDSN string) (*endpoint, error) {
	c, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		return nil, fmt.Errorf("connect source: %w", err)
	}
	return &endpoint{targetDSN: targetDSN, activeDSN: sourceDSN, conn: c, label: "source"}, nil
}

// maybeReconnect redials the active DSN when the connection has dropped. The
// gateway pane watches through the Multigres gateway (localhost:15432): when the
// target primary fails over, the gateway tears down the pinned backend session,
// so this single conn closes. A raw pgx.Conn does not self-heal, and the watch
// loop otherwise swallows every query error without exiting — so without this the
// pane would sit forever on a dead connection ("did not connect to the correct
// server") instead of the gateway re-routing it to the newly elected primary.
// Redialing the same DSN goes back through the gateway and lands on the new
// primary. Best-effort: on a failed redial it stays closed and retries next tick.
func (e *endpoint) maybeReconnect(ctx context.Context) {
	if !e.conn.IsClosed() {
		return
	}
	c, err := pgx.Connect(ctx, e.activeDSN)
	if err != nil {
		return
	}
	e.conn = c
}

func (e *endpoint) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second) //nolint:gocritic // connection close, run ctx may be cancelled
	defer cancel()
	_ = e.conn.Close(ctx)
}

// failover swaps the active connection to the target (Multigres gateway) once,
// then verifies the invariant on the target — the no-data-loss check.
func (e *endpoint) failover(ctx context.Context, tbl string, wantTotal int64) {
	if e.failedTo {
		fmt.Printf("[%s] failover ignored: already on target\n", ts())
		return
	}
	if e.targetDSN == "" {
		fmt.Printf("[%s] failover requested but --target-dsn is empty; ignoring\n", ts())
		return
	}
	fmt.Printf("[%s] failover requested -> connecting target (retrying until the gateway serves)\n", ts())
	// The gateway rejects connections to a shard that is still importing (not yet
	// serving) with a retryable error — SQLSTATE 57P03, "database is temporarily
	// unavailable; please retry". A real client retries, and so does this one: it
	// blocks here, pausing writes, until the migration is activated and the
	// gateway starts serving, then connects and resumes. Ctrl-C (ctx) aborts.
	var c *pgx.Conn
	for attempt := 1; ; attempt++ {
		var err error
		c, err = pgx.Connect(ctx, e.targetDSN)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			fmt.Printf("[%s] failover aborted before the gateway became available: %v\n", ts(), err)
			return
		}
		if attempt == 1 || attempt%10 == 0 {
			fmt.Printf("[%s] gateway not serving yet — buffering writes, retrying (attempt %d): %v\n", ts(), attempt, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	_ = e.conn.Close(ctx)
	e.conn, e.activeDSN, e.label, e.failedTo = c, e.targetDSN, "target", true

	sum, count, err := sumAndCount(ctx, e.conn, tbl)
	if err != nil {
		fmt.Printf("[%s] FAILED OVER to target; verify error: %v\n", ts(), err)
		return
	}
	fmt.Printf("[%s] FAILED OVER to target: %d accounts, total %d, want %d => %s\n",
		ts(), count, sum, wantTotal, verdict(sum == wantTotal))
}

// ----- writer workload -----

type writer struct {
	ep            *endpoint
	tbl           string
	seed          int
	wantTotal     int64
	transfersOnly bool         // if set, only opTransfer runs (row count stays fixed)
	count         atomic.Int64 // approximate row count, steers op choice
}

func (w *writer) loop(ctx context.Context, interval, verifyEvery time.Duration) {
	usr1 := make(chan os.Signal, 1)
	signal.Notify(usr1, syscall.SIGUSR1)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	verify := time.NewTicker(verifyEvery)
	defer verify.Stop()
	var ops, errs int
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[%s] stopping: %d ops, %d errors\n", ts(), ops, errs)
			return
		case <-usr1:
			w.ep.failover(ctx, w.tbl, w.wantTotal)
			if _, cnt, err := sumAndCount(ctx, w.ep.conn, w.tbl); err == nil {
				w.count.Store(cnt)
			}
		case <-verify.C:
			if total, cnt, err := sumAndCount(ctx, w.ep.conn, w.tbl); err == nil {
				w.count.Store(cnt)
				fmt.Printf("[%s] verify on %s: total %d, want %d => %s (%d ops)\n",
					ts(), w.ep.label, total, w.wantTotal, verdict(total == w.wantTotal), ops)
			}
		case <-tick.C:
			if err := w.doOp(ctx); err != nil {
				errs++
				fmt.Printf("[%s] op error on %s: %v\n", ts(), w.ep.label, err)
			} else {
				ops++
			}
		}
	}
}

type account struct {
	id      int64
	balance int64
}

// doOp runs one balance-neutral operation in a single transaction: transfer
// (UPDATE/UPDATE), split (INSERT + UPDATE), or merge (UPDATE + DELETE).
func (w *writer) doOp(ctx context.Context) error {
	tx, err := w.ep.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	switch w.chooseOp() {
	case opSplit:
		accts, err := pickAccounts(ctx, tx, w.tbl, 1)
		if err != nil || len(accts) < 1 || accts[0].balance < 2 {
			return err
		}
		src := accts[0]
		amt := src.balance / 2
		var newID int64
		if err := tx.QueryRow(ctx, "SELECT coalesce(max(id),0)+1 FROM "+w.tbl).Scan(&newID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO "+w.tbl+" (id, balance) VALUES ($1,$2)", newID, amt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE "+w.tbl+" SET balance = balance - $1 WHERE id = $2", amt, src.id); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		w.count.Add(1)
		return nil

	case opMerge:
		accts, err := pickAccounts(ctx, tx, w.tbl, 2)
		if err != nil || len(accts) < 2 {
			return err
		}
		from, into := accts[0], accts[1]
		if _, err := tx.Exec(ctx, "UPDATE "+w.tbl+" SET balance = balance + $1 WHERE id = $2", from.balance, into.id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+w.tbl+" WHERE id = $1", from.id); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		w.count.Add(-1)
		return nil

	default: // opTransfer
		accts, err := pickAccounts(ctx, tx, w.tbl, 2)
		if err != nil || len(accts) < 2 || accts[0].balance < 1 {
			return err
		}
		from, to := accts[0], accts[1]
		amt := rand.Int64N(from.balance) + 1
		if _, err := tx.Exec(ctx, "UPDATE "+w.tbl+" SET balance = balance - $1 WHERE id = $2", amt, from.id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE "+w.tbl+" SET balance = balance + $1 WHERE id = $2", amt, to.id); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
}

type opKind int

const (
	opTransfer opKind = iota
	opSplit
	opMerge
)

// chooseOp keeps the row count in [minAccounts, 2*seed] so transfers always find
// two rows and the ledger never empties. With transfersOnly set it never picks
// split/merge, so the account set (and the watch display) stays fixed.
func (w *writer) chooseOp() opKind {
	if w.transfersOnly {
		return opTransfer
	}
	const minAccounts = 4
	cnt := w.count.Load()
	maxAccounts := int64(2 * maxInt(w.seed, minAccounts))
	switch {
	case cnt <= minAccounts:
		if rand.IntN(2) == 0 {
			return opSplit
		}
		return opTransfer
	case cnt >= maxAccounts:
		if rand.IntN(2) == 0 {
			return opMerge
		}
		return opTransfer
	default:
		switch rand.IntN(4) {
		case 0:
			return opSplit
		case 1:
			return opMerge
		default:
			return opTransfer
		}
	}
}

// ----- shared SQL helpers -----

func pickAccounts(ctx context.Context, tx pgx.Tx, tbl string, k int) ([]account, error) {
	rows, err := tx.Query(ctx, "SELECT id, balance FROM "+tbl+" ORDER BY random() LIMIT $1 FOR UPDATE", k)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []account
	for rows.Next() {
		var a account
		if err := rows.Scan(&a.id, &a.balance); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func sumAndCount(ctx context.Context, conn *pgx.Conn, tbl string) (sum, count int64, err error) {
	err = conn.QueryRow(ctx, "SELECT coalesce(sum(balance),0), count(*) FROM "+tbl).Scan(&sum, &count)
	return sum, count, err
}

// ANSI colors for the apply-status line.
const (
	ansiRed   = "\033[31m"
	ansiReset = "\033[0m"
)

// applyStatus reports logical-replication apply health on the connected endpoint,
// so a stalled subscription — which otherwise silently freezes the target — is
// visible at the bottom of the accounts screen. On a publisher (the source) there
// is no subscription, so it reports "n/a"; the line is always rendered, which
// keeps the source and target panes symmetric. It reads apply_error_count, which
// is primary-local, so it is a best-effort signal: if the endpoint happens to
// serve a replica the count reads 0.
func applyStatus(ctx context.Context, conn *pgx.Conn) string {
	const q = `SELECT s.subname, ss.apply_error_count
		FROM pg_subscription s
		JOIN pg_stat_subscription_stats ss ON ss.subid = s.oid
		ORDER BY s.subname
		LIMIT 1`
	var subname string
	var errCount int64
	switch err := conn.QueryRow(ctx, q).Scan(&subname, &errCount); {
	case errors.Is(err, pgx.ErrNoRows):
		return "apply: n/a (no subscription)"
	case err != nil:
		return "apply: unavailable"
	case errCount > 0:
		return fmt.Sprintf("%s⚠ apply: %s has %d error(s) — target not advancing (see pooler log)%s",
			ansiRed, subname, errCount, ansiReset)
	default:
		return fmt.Sprintf("apply: %s ok", subname)
	}
}

// sampleTable reads the whole table (or the first `limit` rows if limit > 0)
// with SELECT *, so the returned column names reflect the live schema — an added
// or dropped column appears/disappears here as soon as DDL replicates.
//
// It runs under the simple query protocol (no cached prepared statement) on
// purpose: SELECT * changes result shape when a column is added/dropped, and a
// cached plan would fail the next tick with "cached plan must not change result
// type" (a distracting error flash in the watch view right when the DDL demo
// lands). Simple protocol re-describes every call, so the column change renders
// cleanly. The query takes no parameters, so simple protocol is a safe fit.
func sampleTable(ctx context.Context, conn *pgx.Conn, tbl string, limit int) (cols []string, out [][]string, err error) {
	q := "SELECT * FROM " + tbl + " ORDER BY 1"
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := conn.Query(ctx, q, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for _, fd := range rows.FieldDescriptions() {
		cols = append(cols, fd.Name)
	}
	for rows.Next() {
		vals, verr := rows.Values()
		if verr != nil {
			return nil, nil, verr
		}
		cells := make([]string, len(vals))
		for i, v := range vals {
			cells[i] = fmtVal(v)
		}
		out = append(out, cells)
	}
	return cols, out, rows.Err()
}

// rollupRow builds a TOTAL row: "TOTAL" under the first column and the summed
// balance under the "balance" column, blanks elsewhere.
func rollupRow(cols []string, total int64) []string {
	row := make([]string, len(cols))
	if len(row) > 0 {
		row[0] = "TOTAL"
	}
	for i, c := range cols {
		if c == "balance" {
			row[i] = strconv.FormatInt(total, 10)
		}
	}
	return row
}

func fmtVal(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(x)
	case time.Time:
		return x.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", x)
	}
}

// formatTable renders cols as a header, then rows, then (if non-nil) a rule and
// a footer (rollup) row, all left-aligned, 2-space indented. Returns the block
// and its printed width so the caller can size surrounding rules.
func formatTable(cols []string, rows [][]string, footer []string) (body string, width int) {
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	grow := func(cells []string) {
		for i := 0; i < len(cells) && i < len(widths); i++ {
			if len(cells[i]) > widths[i] {
				widths[i] = len(cells[i])
			}
		}
	}
	for _, r := range rows {
		grow(r)
	}
	grow(footer)

	total := 2 // left indent
	for i, w := range widths {
		if i > 0 {
			total += 2
		}
		total += w
	}
	line := func(cells []string) string {
		var b strings.Builder
		b.WriteString("  ")
		for i := range widths {
			if i > 0 {
				b.WriteString("  ")
			}
			cell := ""
			if i < len(cells) {
				cell = cells[i]
			}
			fmt.Fprintf(&b, "%-*s", widths[i], cell)
		}
		return strings.TrimRight(b.String(), " ")
	}
	rule := "  " + strings.Repeat("─", maxInt(total-2, 0))

	var b strings.Builder
	b.WriteString(line(cols) + "\n")
	b.WriteString(rule + "\n")
	if len(rows) == 0 {
		b.WriteString("  (no rows)\n")
	}
	for _, r := range rows {
		b.WriteString(line(r) + "\n")
	}
	if footer != nil {
		b.WriteString(rule + "\n")
		b.WriteString(line(footer) + "\n")
	}
	return b.String(), total
}

func setupLedger(ctx context.Context, conn *pgx.Conn, tbl string, n int, initial int64) error {
	ddl := "CREATE TABLE IF NOT EXISTS " + tbl + " (id bigint PRIMARY KEY, balance bigint NOT NULL CHECK (balance >= 0))"
	if _, err := conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	if _, err := conn.Exec(ctx, "TRUNCATE "+tbl); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	if _, err := conn.Exec(ctx,
		"INSERT INTO "+tbl+" (id, balance) SELECT g, $1 FROM generate_series(1,$2) AS g", initial, n); err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	fmt.Printf("[%s] setup complete: %s seeded with %d accounts, total balance %d\n", ts(), tbl, n, initial*int64(n))
	return nil
}

func ts() string { return time.Now().Format("15:04:05.000") }

func verdict(ok bool) string {
	if ok {
		return "OK"
	}
	return "MISMATCH"
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func quoteTable(qualified string) (string, error) {
	schema, table, ok := strings.Cut(qualified, ".")
	if !ok || schema == "" || table == "" {
		return "", fmt.Errorf("invalid --table %q: expected schema.table", qualified)
	}
	return pgx.Identifier{schema, table}.Sanitize(), nil
}
