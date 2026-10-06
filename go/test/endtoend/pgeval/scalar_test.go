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

package pgeval

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	_ "github.com/multigres/multigres/go/common/pgeval/funcs"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
	"github.com/multigres/multigres/go/tools/executil"
)

// TestScalarsAgainstPostgres tests direct local fmgr execution, not gateway
// passthrough. Opt in with RUN_PGEVAL_DIFFERENTIAL=1: every invocation builds
// PostgreSQL from source, so this must not run in ordinary PR integration jobs.
// Build the exact catalog/port version, and fail on any mismatch.
func TestScalarsAgainstPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and starts PostgreSQL; run without -short")
	}
	if os.Getenv("RUN_PGEVAL_DIFFERENTIAL") != "1" {
		t.Skip("set RUN_PGEVAL_DIFFERENTIAL=1 to run the PostgreSQL differential suite")
	}
	if err := pgbuilder.CheckBuildDependencies(t); err != nil {
		t.Skipf("Build dependencies not available: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	builder := pgbuilder.New(t)
	t.Cleanup(builder.Cleanup)
	require.NoError(t, builder.EnsureSource(t, ctx))
	require.NoError(t, builder.Build(t, ctx))
	server, err := pgbuilder.StartStandalone(t, ctx, builder, "pgeval")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Stop()) })
	config, err := pgx.ParseConfig("")
	require.NoError(t, err)
	config.Host = "127.0.0.1"
	config.Port = uint16(server.Port)
	config.User = server.User
	config.Password = server.Password
	config.Database = server.Database
	config.TLSConfig = nil
	config.Fallbacks = nil
	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
	conn.TypeMap().RegisterType(&pgtype.Type{Name: "cstring", OID: uint32(pgoid.CSTRINGOID), Codec: pgtype.TextCodec{}})

	var version string
	require.NoError(t, conn.QueryRow(ctx, "SHOW server_version").Scan(&version))
	require.Equal(t, "17.6", version)

	rng := rand.New(rand.NewPCG(17, 6))
	comparisons, functions := 0, 0
	seen := map[string]bool{}
	for _, proc := range pgcatalog.Procs {
		if !fmgr.IsBuiltinRegistered(proc.Src) {
			continue
		}
		functions++
		seen[proc.Src] = true
		t.Run(fmt.Sprintf("%s_%d", proc.Src, proc.Oid), func(t *testing.T) {
			info, err := fmgr.FmgrInfoFor(proc.Oid)
			require.NoError(t, err)
			placeholders := make([]string, len(proc.ArgTypes))
			types := make([]string, len(proc.ArgTypes))
			for i, typ := range proc.ArgTypes {
				types[i] = (pgx.Identifier{"pg_catalog", pgcatalog.TypeByOid(typ).Name}).Sanitize()
				placeholders[i] = "$" + strconv.Itoa(i+1) + "::" + types[i]
			}
			name := (pgx.Identifier{"pg_catalog", proc.Name}).Sanitize()
			// Confirm that SQL overload resolution calls the same OID we
			// resolve locally, including casts and alternate abs/mod names.
			var oid uint32
			require.NoError(t, conn.QueryRow(ctx, "SELECT $1::regprocedure::oid",
				name+"("+strings.Join(types, ",")+")").Scan(&oid))
			require.Equal(t, uint32(proc.Oid), oid)
			if proc.ArgTypes[0] == pgoid.INTERNALOID {
				comparisons += compareBinaryReceive(t, ctx, conn, &proc, info, rng)
				return
			}
			query := "SELECT " + name + "(" + strings.Join(placeholders, ", ") + ")"
			check := func(args ...any) {
				t.Helper()
				comparisons++
				compareScalar(t, ctx, conn, query, &proc, info, args)
			}
			// Cartesian products cover every NULL position and flag pair.
			// Range functions use compact boundary sets for their five args.
			sets := make([][]any, len(proc.ArgTypes))
			for i, typ := range proc.ArgTypes {
				sets[i] = scalarSamples(typ, len(proc.ArgTypes) == 5)
				if typ == pgoid.CSTRINGOID {
					sets[i] = inputSamples(proc.Src)
				}
			}
			args := make([]any, len(sets))
			var visit func(int)
			visit = func(i int) {
				if i == len(sets) {
					check(args...)
					return
				}
				for _, value := range sets[i] {
					args[i] = value
					visit(i + 1)
				}
			}
			visit(0)
			for range 100 {
				args := make([]any, len(proc.ArgTypes))
				for i, typ := range proc.ArgTypes {
					switch typ {
					case pgoid.CHAROID:
						args[i] = byte(rng.Uint32())
					case pgoid.INT2OID:
						args[i] = int16(rng.Uint32())
					case pgoid.INT4OID:
						args[i] = int32(rng.Uint32())
					case pgoid.INT8OID:
						args[i] = int64(rng.Uint64())
					case pgoid.BOOLOID:
						args[i] = rng.IntN(2) == 1
					case pgoid.CSTRINGOID:
						// Deterministic malformed inputs probe scanner error
						// precedence, not just acceptance of valid numbers.
						alphabet := "0123456789abcdefABCDEFxXoObB_+- \t\n"
						b := make([]byte, rng.IntN(35))
						for j := range b {
							b[j] = alphabet[rng.IntN(len(alphabet))]
						}
						args[i] = string(b)
					default:
						t.Fatalf("no test generator for type %d", typ)
					}
				}
				check(args...)
			}
		})
	}
	for _, src := range fmgr.RegisteredBuiltins() {
		assert.True(t, seen[src], "no catalog entry exercised for %s", src)
	}
	require.NotZero(t, functions)
	t.Logf("Compared %d cases across %d catalog OIDs / %d implementations with PostgreSQL %s",
		comparisons, functions, len(seen), version)
}

// Exercise the real entry point without -short in a child test binary, even
// when the parent unit run uses -short. Neither case may clone or build PG.
func TestScalarsPrerequisites(t *testing.T) {
	t.Parallel()
	executable, err := os.Executable()
	require.NoError(t, err)
	for _, tc := range []struct {
		name, envValue, message string
	}{
		{"opt_in_required", "", "set RUN_PGEVAL_DIFFERENTIAL=1"},
		{"missing_build_dependencies", "1", "Build dependencies not available"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := filepath.Join(t.TempDir(), "postgres")
			cmd := executil.Command(t.Context(), executable, "-test.run=^TestScalarsAgainstPostgres$", "-test.v")
			cmd.AddEnv("RUN_PGEVAL_DIFFERENTIAL="+tc.envValue, "PATH=", "MULTIGRES_PG_CACHE_DIR="+cache)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
			require.Contains(t, string(output), "--- SKIP: TestScalarsAgainstPostgres")
			require.Contains(t, string(output), tc.message)
			require.NoDirExists(t, cache)
		})
	}
}

func scalarSamples(typ pgoid.Oid, compact bool) []any {
	if typ == pgoid.CHAROID {
		values := []any{nil}
		for n := 0; n <= 255; n++ {
			values = append(values, byte(n))
		}
		return values
	}
	if typ == pgoid.BOOLOID {
		return []any{nil, false, true}
	}
	lo, hi := int64(math.MinInt64), int64(math.MaxInt64)
	switch typ {
	case pgoid.INT2OID:
		lo, hi = math.MinInt16, math.MaxInt16
	case pgoid.INT4OID:
		lo, hi = math.MinInt32, math.MaxInt32
	}
	values := []any{nil}
	numbers := []int64{
		lo, lo + 1, -3037000500, -2147483649, -46341, -32769, -65, -64, -33, -32,
		-17, -16, -2, -1, 0, 1, 2, 15, 16, 17, 31, 32, 33, 63, 64, 65,
		181, 182, 32767, 32768, 46340, 46341, 2147483647, 2147483648, 3037000499, 3037000500,
		hi - 1, hi,
	}
	if compact {
		numbers = []int64{lo, lo + 1, -1, 0, 1, 2, hi - 1, hi}
	}
	for _, n := range numbers {
		if n < lo || n > hi {
			continue
		}
		switch typ {
		case pgoid.INT2OID:
			values = append(values, int16(n))
		case pgoid.INT4OID:
			values = append(values, int32(n))
		case pgoid.INT8OID:
			values = append(values, n)
		}
	}
	return values
}

func inputSamples(src string) []any {
	values := []any{nil}
	if src == "boolin" {
		for _, word := range []string{"true", "false", "yes", "no", "on", "off", "1", "0"} {
			for i := 1; i <= len(word); i++ {
				values = append(values, word[:i], strings.ToUpper(word[:i]), " \t\n\r\v\f"+word[:i]+" \t\n\r\v\f")
			}
		}
		return append(values, "", " ", "truee", "t r", "10", "01", "2", "-1", "\u00a0true", "false\u2003", "falſe", "1\"2", "1\\2")
	}
	for _, s := range []string{
		"", " ", "+", "-", "--1", "+-1", "0", "-0", "00", "010", "08", "1_2_3", "\t123\r\n", "\v\f123\v\f",
		"_1", "1_", "1__0", "1_\t0", "0x", "0x_", "0x__1", "0X_FF", "0xG", "0x1_", "0b", "0b2", "0b_11", "0b1_",
		"0o", "0o_77", "0o8", "0o1_", "0o__1", "0b__1", "00x1", "0d12", "1 2", "1.0", "1e2", "１２", "١٢",
		"\u00a01", "1\u2003", "0x-1", "1\"2", "1\\2", strings.Repeat("9", 100),
	} {
		values = append(values, s)
	}
	// Test every width's boundaries for every input function. Include bases,
	// signs, whitespace, separators and malformed tails to check error ordering.
	for _, n := range []int64{math.MinInt16, math.MaxInt16, math.MinInt32, math.MaxInt32, math.MinInt64, math.MaxInt64} {
		for _, delta := range []int64{-1, 0, 1} {
			number := new(big.Int).Add(big.NewInt(n), big.NewInt(delta))
			for _, base := range []int{2, 8, 10, 16} {
				s := new(big.Int).Abs(number).Text(base)
				prefix := map[int]string{2: "0b", 8: "0o", 10: "", 16: "0x"}[base]
				sign := "+"
				if number.Sign() < 0 {
					sign = "-"
				}
				for _, digits := range []string{s, s[:1] + "_" + s[1:]} {
					text := sign + prefix + digits
					values = append(values, text, " \t"+text+"\r\n", text+"z", text+"0z", text+"_", text+"__0")
				}
			}
		}
	}
	return values
}

func compareScalar(t *testing.T, ctx context.Context, conn *pgx.Conn, query string, proc *pgcatalog.Proc, info *fmgr.FmgrInfo, args []any) {
	t.Helper()
	fcinfo := fmgr.NewFunctionCallInfo(info, len(args), pgoid.InvalidOid)
	for i, arg := range args {
		switch value := arg.(type) {
		case nil:
			fcinfo.Args[i].IsNull = true
		case byte:
			fcinfo.Args[i].Value = datum.CharGetDatum(int8(value))
		case int16:
			fcinfo.Args[i].Value = datum.Int16GetDatum(value)
		case int32:
			fcinfo.Args[i].Value = datum.Int32GetDatum(value)
		case int64:
			fcinfo.Args[i].Value = datum.Int64GetDatum(value)
		case bool:
			fcinfo.Args[i].Value = datum.BoolGetDatum(value)
		case string:
			fcinfo.Args[i].Value = datum.BytesGetDatum([]byte(value))
		default:
			t.Fatalf("unsupported argument %T", arg)
		}
	}
	var local datum.Datum
	localErr := pgerror.Recover(func() { local = fmgr.CallFunction(fcinfo) })

	var reference any
	var resultType uint32
	rows, referenceErr := conn.Query(ctx, query, args...)
	if referenceErr == nil {
		fields := rows.FieldDescriptions()
		reference, referenceErr = pgx.CollectExactlyOneRow(rows, pgx.RowTo[any])
		if referenceErr == nil {
			require.Len(t, fields, 1)
			resultType = fields[0].DataTypeOID
		}
	}
	if referenceErr != nil {
		var pgErr *pgconn.PgError
		require.ErrorAs(t, referenceErr, &pgErr, "args: %v", args)
		var localDiag *mterrors.PgDiagnostic
		require.ErrorAs(t, localErr, &localDiag, "args: %v", args)
		require.Equal(t, pgErr.Code, localDiag.Code, "args: %v", args)
		require.Equal(t, pgErr.Message, localDiag.Message, "args: %v", args)
		require.Equal(t, pgErr.Severity, localDiag.Severity, "args: %v", args)
		return
	}
	require.NoError(t, localErr, "args: %v", args)
	require.Equal(t, uint32(proc.RetType), resultType)
	require.Equal(t, reference == nil, fcinfo.IsNull, "args: %v", args)
	if reference == nil {
		return
	}
	require.Equal(t, reference, scalarResult(t, proc.RetType, local), "args: %v", args)
}

func scalarResult(t *testing.T, typ pgoid.Oid, value datum.Datum) any {
	t.Helper()
	switch typ {
	case pgoid.INT2OID:
		return datum.DatumGetInt16(value)
	case pgoid.INT4OID:
		return datum.DatumGetInt32(value)
	case pgoid.INT8OID:
		return datum.DatumGetInt64(value)
	case pgoid.BOOLOID:
		return datum.DatumGetBool(value)
	case pgoid.CSTRINGOID, pgoid.TEXTOID:
		return string(datum.DatumGetBytes(value))
	case pgoid.BYTEAOID:
		return datum.DatumGetBytes(value)
	default:
		t.Fatalf("unexpected result type %d", typ)
		return nil
	}
}
