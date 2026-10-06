# Integer and boolean primitives and type support

This package registers PostgreSQL `REL_17_6` scalar implementations with
`fmgr.RegisterBuiltin` during initialization. Consumers import it for its side
effects, resolve an OID through `fmgr.FmgrInfoFor`, and invoke the resulting
function with `fmgr.CallFunction` inside a `pgerror.Recover` boundary.

The implementation currently covers **191 `prosrc` names / 197 catalog OIDs**.
The extra OIDs are the SQL `abs` and `mod` aliases sharing implementations.

## Supported operations

- `int2`, `int4`, `int8`: arithmetic, comparisons, unary signs, absolute value,
  remainder, min/max helpers, bitwise operators and shifts, text and binary I/O.
- Every ordered pair of integer widths: arithmetic and comparisons, with
  PostgreSQL's result width and overflow behavior.
- All six casts between the integer widths; narrowing raises an error rather
  than truncating.
- `int4inc`, `int8inc`, `int8dec`, and integer `gcd`/`lcm` where PG provides them.
- Boolean comparisons, text/binary I/O, boolean-to-text and int4/boolean casts.
- Three-way integer/boolean comparators, including mixed integer widths.
  `btint2cmp` returns the actual widened difference, not just its sign.
- PostgreSQL-compatible integer hashes and seeded hashes, plus the shared
  `hashchar` / `hashcharextended` routines used for boolean hashing.
- All seven integer `in_range` window-frame helpers. Negative offsets raise
  `22013`; overflowing bounds still produce mathematical comparisons.
- The strict scalar `booland_statefunc` / `boolor_statefunc` helpers. This does
  **not** implement aggregate execution or expression-level SQL `AND` / `OR`.

## Porting details

Source: `src/backend/utils/adt/{int.c,int8.c,bool.c,numutils.c}`,
`src/backend/access/{nbtree/nbtcompare.c,hash/hashfunc.c}`,
`src/backend/libpq/pqformat.c`, `src/common/hashfn.c`, and
`src/include/common/int.h` in PostgreSQL `REL_17_6`.

The repeated integer bodies use Go type parameters. Registrations fix each
argument's width and the result width; no runtime catalog lookup or type
switch is needed to decide an arithmetic operation. Integer Datum constructors
all use the same sign-extended value slot, so returning a checked signed value
through `Int64GetDatum` preserves the narrower types' representation too.

Overflow checks use Go's defined signed wraparound semantics. They are checked
against an independent `math/big` oracle and against PostgreSQL, including
minimum-integer negation, multiplication, division and remainder.

PG 17's C shifts have platform-dependent behavior for negative or oversized
counts. The implementation matches native amd64/arm64 shift masking; int2
operands are promoted to int32 before shifting. Another architecture requires
validating its reference PostgreSQL behavior first.

Text and cstring results use `datum.BytesGetDatum`, without a required trailing
NUL or varlena header. Input ends at an embedded NUL, matching C strings;
frontend decoders must reject embedded NUL in SQL text before constructing
these internal values. Numeric input preserves PG's radix prefixes, decimal
leading-zero behavior, underscore rules, ASCII whitespace and error precedence.
Boolean input reuses `sqltypes.ParseBool` with the same whitespace rules.
The shared parser now rejects non-ASCII whitespace (such as non-breaking and
em spaces) around boolean words. This PostgreSQL-compatibility correction also
affects existing startup/GUC, Bind, SET, and planner callers, not just pgeval;
ASCII whitespace and accepted boolean words/prefixes are unchanged.

Hashing uses PG's Jenkins mix/final operations, including seed handling and
int8's sign-aware folding, so equal integers hash identically across widths.
PG's shared `hashchar` routine promotes a plain C `char`: on the supported
linux/darwin × amd64/arm64 matrix, Linux ARM64 defaults to unsigned char and
the others to signed char. Registrations select the matching width/signedness;
boolean inputs 0/1 hash identically everywhere. Additional architectures or
non-default C char-signedness flags require checking this ABI assumption.

### Binary receive arguments

`recv` takes an `internal` pointer to a `BinaryInput`, not a bytea Datum:

```go
input := funcs.NewBinaryInput(payload)
fcinfo.Args[0].Value = input.Datum()
```

The buffer borrows its bytes and maintains an execution-local cursor. Datum's
pointer keeps both the buffer and its backing slice visible to the GC. Reads
check available bytes before advancing; short input raises PG's `08P01` error.
An empty buffer is not SQL NULL; nullness still travels in `NullableDatum`.
Boolean receive accepts every nonzero byte as true; send emits only 0 or 1.

The caller must enforce framing: `recv` consumes one value and may leave bytes
for a containing decoder. A Bind decoder must reject `input.Remaining() != 0`
after a complete parameter, as PG does. This package does not add gateway Bind
handling or a general-purpose growable `StringInfo` implementation.

## Boundaries

Integer vectors, float/numeric casts, sort-support callback structures,
set-returning functions, aggregate/window execution and non-throwing
`ErrorSaveContext` handling are not implemented here. SQL boolean
short-circuiting and three-valued logic belong to the later expression
evaluator. This package is not yet wired into gateway query execution.

## Validation

The PostgreSQL differential suite is opt-in. Set
`RUN_PGEVAL_DIFFERENTIAL=1` in the environment before running the integration
command below. Without that opt-in (or with `-short`), it skips before cloning
or building PostgreSQL. Missing `make`/`gcc` also skips locally; source/build
failures and semantic mismatches still fail.

```text
/mt-dev unit ./go/common/pgeval/... -race -shuffle=on -count=10
/mt-dev unit ./go/common/sqltypes -race
/mt-dev integration pgeval TestScalarsAgainstPostgres -race -timeout=20m
```

The `Expression Engine Differential Tests` workflow
(`.github/workflows/test-pgeval-differential.yml`) runs it weekly, on manual
dispatch, and on pull requests labeled `Run Extended Query Serving Tests` or
`Run all Query Serving Tests`, as its own check next to the PostgreSQL
compatibility suites. It opts in explicitly and checks build prerequisites
first, so missing tools fail CI rather than silently skip coverage. Ordinary PR
integration jobs do not opt in. The source checkout is cached, but each
invocation rebuilds and installs PostgreSQL into an isolated directory.

The differential test builds pinned PostgreSQL 17.6 using `pgbuilder` and starts
one standalone server. It compares direct local fmgr calls, not gateway
passthrough, with the reference's values, NULLs, type OIDs, SQLSTATEs, error
messages and severities. Inputs include boundary cross-products, casts, radix
and separator syntax, malformed input, and deterministic random values.
Binary receivers are tested through raw binary Bind parameters, after verifying
PG's `typreceive` OID. Tests distinguish receiver errors from Bind's trailing-byte
rejection and compare the decoded prefix too. All 256 boolean bytes and internal
char hash inputs are exercised. Range helpers also have an independent
unbounded-integer oracle; binary decoding has cursor/lifetime tests and fuzzing.
Missing registrations are independently checked by the unit tests. Every
mismatch fails the test; none are accepted as output patches.
