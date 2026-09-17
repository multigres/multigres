// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2026, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

// Package pgcataloggen generates the Go tables in go/common/pgcatalog from
// PostgreSQL's catalog bootstrap data files (pg_type.dat, pg_proc.dat,
// pg_operator.dat, pg_cast.dat, pg_collation.dat).
//
// It reimplements, for the column subset multigres carries, what PostgreSQL's
// build does with this data:
//
//   - Catalog.pm ParseData(): the .dat files are lists of Perl hash literals;
//     entries are accumulated across lines until braces balance, and values
//     follow Perl single-quote escaping rules (postgres
//     src/backend/catalog/Catalog.pm:308-390).
//   - Column defaults (Catalog.pm:394, AddDefaultValues): a .dat row is a
//     diff against per-column defaults — any column whose value equals its
//     default is simply omitted. The defaults themselves are not in the .dat
//     files; they live as BKI_DEFAULT annotations on the struct fields in
//     the catalog headers (e.g. an omitted proisstrict means true,
//     pg_proc.h:68), with a separate BKI_ARRAY_DEFAULT baseline for
//     synthesized array-type rows. This generator hand-transcribes the
//     defaults for the columns it carries: each one appears as the fallback
//     in an e.Get(column, default) call in the resolve functions below, with
//     a comment citing the header line it came from so a version rebase can
//     re-verify them mechanically. Columns with no default go through
//     Require(), which errors if absent — mirroring upstream's fatal path.
//   - genbki.pl symbolic-reference resolution: type names, regproc references
//     (bare name, or "name(argtype,...)" when the name is overloaded), and
//     operator references of the form "name(left,right)".
//   - Catalog.pm GenerateArrayTypes(): every element type with an
//     array_type_oid implies a synthesized "_name" array type row.
//
// Upstream version: PostgreSQL REL_17_6
// (commit 7885b94dd81b98bbab9ed878680d156df7bf857f).
package pgcataloggen

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// UpstreamVersion identifies the PostgreSQL release the vendored .dat files
// were copied from. It is stamped into generated file headers.
const UpstreamVersion = "REL_17_6 (commit 7885b94dd81b98bbab9ed878680d156df7bf857f)"

// multipleSentinel marks a lookup key shared by several rows, mirroring
// genbki.pl's 'MULTIPLE' convention -
// postgres/src/backend/catalog/genbki.pl:306-335: such a key cannot be used
// as a bare reference and callers must use the signature form instead.
const multipleSentinel = ^uint32(0)

// Entry is one parsed .dat row: its fields plus the source line for errors.
type Entry struct {
	Fields map[string]string
	Line   int
}

// Get returns a field value, or the default if the field is absent.
func (e Entry) Get(key, def string) string {
	if v, ok := e.Fields[key]; ok {
		return v
	}
	return def
}

// Require returns a field value, erroring if absent (a column with no
// BKI_DEFAULT must be present in every row).
func (e Entry) Require(file, key string) (string, error) {
	v, ok := e.Fields[key]
	if !ok {
		return "", fmt.Errorf("%s line %d: missing required field %q", file, e.Line, key)
	}
	return v, nil
}

// ParseDat parses one .dat file into entries, following ParseData -
// postgres/src/backend/catalog/Catalog.pm:308: a line containing '{' starts
// an entry, lines are accumulated until '{' and '}' counts balance
// (brace-balanced values like '{i,o}' work because they balance), and lines
// without braces (comments, the enclosing [ ]) are skipped.
func ParseDat(file string, data []byte) ([]Entry, error) {
	var entries []Entry
	lines := strings.Split(string(data), "\n")
	for i := 0; i < len(lines); i++ {
		if !strings.Contains(lines[i], "{") {
			continue
		}
		startLine := i + 1
		entryLines := []string{lines[i]}
		depth := strings.Count(lines[i], "{") - strings.Count(lines[i], "}")
		for depth != 0 {
			i++
			if i >= len(lines) {
				return nil, fmt.Errorf("%s line %d: unbalanced braces at end of file", file, startLine)
			}
			entryLines = append(entryLines, lines[i])
			depth += strings.Count(lines[i], "{") - strings.Count(lines[i], "}")
		}
		entry, err := parseEntry(file, strings.Join(entryLines, "\n"), startLine)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// parseEntry parses the "key => 'value', ..." pairs of one accumulated
// { ... } entry.
func parseEntry(file, s string, line int) (Entry, error) {
	open := strings.Index(s, "{")
	closing := strings.LastIndex(s, "}")
	if open < 0 || closing < open {
		return Entry{}, fmt.Errorf("%s line %d: malformed entry", file, line)
	}
	e := Entry{Fields: make(map[string]string), Line: line}
	p := &pairParser{s: s[open+1 : closing]}
	for {
		p.skipSpace()
		if p.eof() {
			return e, nil
		}
		key := p.readWord()
		if key == "" {
			return Entry{}, fmt.Errorf("%s line %d: expected field name at %q", file, line, p.rest())
		}
		p.skipSpace()
		if !p.consume("=>") {
			return Entry{}, fmt.Errorf("%s line %d: expected '=>' after %q", file, line, key)
		}
		p.skipSpace()
		val, err := p.readValue()
		if err != nil {
			return Entry{}, fmt.Errorf("%s line %d, field %q: %w", file, line, key, err)
		}
		e.Fields[key] = val
		p.skipSpace()
		if !p.eof() && !p.consume(",") {
			return Entry{}, fmt.Errorf("%s line %d: expected ',' after field %q", file, line, key)
		}
	}
}

type pairParser struct {
	s   string
	pos int
}

func (p *pairParser) eof() bool { return p.pos >= len(p.s) }

func (p *pairParser) rest() string {
	r := p.s[p.pos:]
	if len(r) > 40 {
		r = r[:40]
	}
	return r
}

func (p *pairParser) skipSpace() {
	for !p.eof() {
		switch p.s[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *pairParser) consume(tok string) bool {
	if strings.HasPrefix(p.s[p.pos:], tok) {
		p.pos += len(tok)
		return true
	}
	return false
}

func (p *pairParser) readWord() string {
	start := p.pos
	for !p.eof() {
		c := p.s[p.pos]
		if c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			p.pos++
		} else {
			break
		}
	}
	return p.s[start:p.pos]
}

// readValue reads a single-quoted string or a bareword. Upstream evaluates
// each entry as Perl source (Catalog.pm:344), so the escaping rules are
// Perl's single-quote semantics: \' is a quote, \\ is a backslash, any other
// backslash is literal.
func (p *pairParser) readValue() (string, error) {
	if p.eof() {
		return "", errors.New("expected value, got end of entry")
	}
	if p.s[p.pos] != '\'' {
		start := p.pos
		for !p.eof() {
			c := p.s[p.pos]
			if c == ',' || c == ' ' || c == '\t' || c == '\n' || c == '\r' {
				break
			}
			p.pos++
		}
		if p.pos == start {
			return "", fmt.Errorf("expected value at %q", p.rest())
		}
		return p.s[start:p.pos], nil
	}
	p.pos++
	var b strings.Builder
	for !p.eof() {
		c := p.s[p.pos]
		switch c {
		case '\'':
			p.pos++
			return b.String(), nil
		case '\\':
			if p.pos+1 < len(p.s) && (p.s[p.pos+1] == '\'' || p.s[p.pos+1] == '\\') {
				b.WriteByte(p.s[p.pos+1])
				p.pos += 2
			} else {
				// Perl leaves any other backslash literal.
				b.WriteByte('\\')
				p.pos++
			}
		default:
			b.WriteByte(c)
			p.pos++
		}
	}
	return "", errors.New("unterminated quoted value")
}

// TypeRow is the resolved pg_type subset (see pgcatalog.Type).
type TypeRow struct {
	Oid       uint32
	Name      string
	Len       int16
	ByVal     bool
	Category  byte
	Preferred bool
	Input     uint32
	Output    uint32
	Receive   uint32
	Send      uint32
	Elem      uint32
	Array     uint32
	Collation uint32
}

// ProcRow is the resolved pg_proc subset (see pgcatalog.Proc).
type ProcRow struct {
	Oid      uint32
	Name     string
	ArgTypes []uint32
	RetType  uint32
	Strict   bool
	Volatile byte
	RetSet   bool
	Kind     byte
	Src      string
}

// OperatorRow is the resolved pg_operator subset (see pgcatalog.Operator).
type OperatorRow struct {
	Oid        uint32
	Name       string
	Kind       byte
	Left       uint32
	Right      uint32
	Result     uint32
	Commutator uint32
	Negator    uint32
	Code       uint32
	CanMerge   bool
	CanHash    bool
}

// CastRow is the resolved pg_cast subset (see pgcatalog.Cast).
type CastRow struct {
	Source  uint32
	Target  uint32
	Func    uint32
	Context byte
	Method  byte
}

// Catalog holds all resolved rows, ready for rendering.
type Catalog struct {
	Types     []TypeRow
	Procs     []ProcRow
	Operators []OperatorRow
	Casts     []CastRow
}

// Load parses the five .dat files under dataDir, applies defaults,
// synthesizes array types, resolves all symbolic references, and returns the
// resolved catalog.
func Load(dataDir string) (*Catalog, error) {
	read := func(name string) ([]Entry, error) {
		data, err := os.ReadFile(filepath.Join(dataDir, name))
		if err != nil {
			return nil, err
		}
		return ParseDat(name, data)
	}
	typeEntries, err := read("pg_type.dat")
	if err != nil {
		return nil, err
	}
	procEntries, err := read("pg_proc.dat")
	if err != nil {
		return nil, err
	}
	operEntries, err := read("pg_operator.dat")
	if err != nil {
		return nil, err
	}
	castEntries, err := read("pg_cast.dat")
	if err != nil {
		return nil, err
	}
	collEntries, err := read("pg_collation.dat")
	if err != nil {
		return nil, err
	}
	return resolve(typeEntries, procEntries, operEntries, castEntries, collEntries)
}

func resolve(typeEntries, procEntries, operEntries, castEntries, collEntries []Entry) (*Catalog, error) {
	r := &resolver{
		typeOids: make(map[string]uint32),
		procKeys: make(map[string]uint32),
		operKeys: make(map[string]uint32),
		collOids: make(map[string]uint32),
	}

	// Collations: only name -> OID is needed (typcollation references).
	for _, e := range collEntries {
		oid, err := parseOid(e.Fields["oid"])
		if err != nil {
			return nil, fmt.Errorf("pg_collation.dat line %d: %w", e.Line, err)
		}
		name, err := e.Require("pg_collation.dat", "collname")
		if err != nil {
			return nil, err
		}
		r.collOids[name] = oid
	}

	// Array type synthesis must happen before any name lookups so that
	// "_int4"-style references resolve (Catalog.pm:445).
	typeEntries, err := synthesizeArrayTypes(typeEntries)
	if err != nil {
		return nil, err
	}
	for _, e := range typeEntries {
		oid, err := parseOid(e.Fields["oid"])
		if err != nil {
			return nil, fmt.Errorf("pg_type.dat line %d: %w", e.Line, err)
		}
		name, err := e.Require("pg_type.dat", "typname")
		if err != nil {
			return nil, err
		}
		r.typeOids[name] = oid
	}

	// Proc lookup keys - genbki.pl:306-335: both the bare name and the
	// "name(argtype,...)" signature form, with keys claimed by more than one
	// row poisoned as ambiguous.
	for _, e := range procEntries {
		oid, err := parseOid(e.Fields["oid"])
		if err != nil {
			return nil, fmt.Errorf("pg_proc.dat line %d: %w", e.Line, err)
		}
		name, err := e.Require("pg_proc.dat", "proname")
		if err != nil {
			return nil, err
		}
		argTypes, err := e.Require("pg_proc.dat", "proargtypes")
		if err != nil {
			return nil, err
		}
		sig := name + "(" + strings.Join(strings.Fields(argTypes), ",") + ")"
		addKey(r.procKeys, name, oid)
		addKey(r.procKeys, sig, oid)
	}

	// Operator lookup keys - genbki.pl:285-294: always "name(left,right)",
	// with "0" for the missing side of prefix operators.
	for _, e := range operEntries {
		oid, err := parseOid(e.Fields["oid"])
		if err != nil {
			return nil, fmt.Errorf("pg_operator.dat line %d: %w", e.Line, err)
		}
		name, err := e.Require("pg_operator.dat", "oprname")
		if err != nil {
			return nil, err
		}
		left, err := e.Require("pg_operator.dat", "oprleft")
		if err != nil {
			return nil, err
		}
		right, err := e.Require("pg_operator.dat", "oprright")
		if err != nil {
			return nil, err
		}
		addKey(r.operKeys, name+"("+left+","+right+")", oid)
	}

	cat := &Catalog{}
	if cat.Types, err = r.resolveTypes(typeEntries); err != nil {
		return nil, err
	}
	if cat.Procs, err = r.resolveProcs(procEntries); err != nil {
		return nil, err
	}
	if cat.Operators, err = r.resolveOperators(operEntries); err != nil {
		return nil, err
	}
	if cat.Casts, err = r.resolveCasts(castEntries); err != nil {
		return nil, err
	}

	sort.Slice(cat.Types, func(i, j int) bool { return cat.Types[i].Oid < cat.Types[j].Oid })
	sort.Slice(cat.Procs, func(i, j int) bool { return cat.Procs[i].Oid < cat.Procs[j].Oid })
	sort.Slice(cat.Operators, func(i, j int) bool { return cat.Operators[i].Oid < cat.Operators[j].Oid })
	sort.Slice(cat.Casts, func(i, j int) bool {
		if cat.Casts[i].Source != cat.Casts[j].Source {
			return cat.Casts[i].Source < cat.Casts[j].Source
		}
		return cat.Casts[i].Target < cat.Casts[j].Target
	})
	return cat, nil
}

type resolver struct {
	typeOids map[string]uint32
	procKeys map[string]uint32
	operKeys map[string]uint32
	collOids map[string]uint32
}

func addKey(m map[string]uint32, key string, oid uint32) {
	if _, exists := m[key]; exists {
		m[key] = multipleSentinel
	} else {
		m[key] = oid
	}
}

// lookup resolves one symbolic reference, following lookup_oids -
// postgres/src/backend/catalog/genbki.pl:1071. Optional references
// (BKI_LOOKUP_OPT columns) may be '0' or '-', meaning "none".
func lookup(m map[string]uint32, ref, what string, optional bool) (uint32, error) {
	if optional && (ref == "0" || ref == "-") {
		return 0, nil
	}
	oid, ok := m[ref]
	if !ok {
		return 0, fmt.Errorf("unresolved %s reference %q", what, ref)
	}
	if oid == multipleSentinel {
		return 0, fmt.Errorf("ambiguous %s reference %q (overloaded name; a signature form is required)", what, ref)
	}
	return oid, nil
}

// synthesizeArrayTypes appends an array type row for every element row
// carrying array_type_oid, following GenerateArrayTypes -
// postgres/src/backend/catalog/Catalog.pm:445: fixed values come from the
// BKI_ARRAY_DEFAULT annotations in pg_type.h, everything else is copied from
// the element row. Only the columns this generator carries are synthesized.
func synthesizeArrayTypes(typeEntries []Entry) ([]Entry, error) {
	out := make([]Entry, 0, 2*len(typeEntries))
	for _, e := range typeEntries {
		arrayOid, hasArray := e.Fields["array_type_oid"]
		if !hasArray {
			out = append(out, e)
			continue
		}
		elemName, err := e.Require("pg_type.dat", "typname")
		if err != nil {
			return nil, err
		}
		arr := Entry{Line: e.Line, Fields: map[string]string{
			"oid":     arrayOid,
			"typname": "_" + elemName,
			"typelem": elemName,
			// BKI_ARRAY_DEFAULT values (pg_type.h): typlen -1 (line 56),
			// typbyval f (66), typcategory A (85), typispreferred f (88),
			// typinput/typoutput/typreceive/typsend array_in/out/recv/send
			// (133-138), typarray 0 (126).
			"typlen":         "-1",
			"typbyval":       "f",
			"typcategory":    "A",
			"typispreferred": "f",
			"typinput":       "array_in",
			"typoutput":      "array_out",
			"typreceive":     "array_recv",
			"typsend":        "array_send",
			"typarray":       "0",
			// Copied from the element row (no array default): typcollation.
			"typcollation": e.Get("typcollation", "0"),
		}}
		// Back-link the element's typarray to the synthesized row by name,
		// as Catalog.pm:492 does.
		elem := Entry{Line: e.Line, Fields: make(map[string]string, len(e.Fields)+1)}
		maps.Copy(elem.Fields, e.Fields)
		elem.Fields["typarray"] = "_" + elemName
		out = append(out, elem, arr)
	}
	return out, nil
}

// datMacros substitutes the C build-configuration macros that appear as
// values in pg_type.dat. Upstream defers these to cluster-creation time:
// genbki.pl passes the macro names through untouched (genbki.pl:1051) and
// initdb substitutes the running binary's compiled-in values
// (postgres/src/bin/initdb/initdb.c:1556-1566). They are conditional
// upstream only because C's Datum is word-sized (uintptr): on 32-bit builds
// FLOAT8PASSBYVAL flips int8/float8/timestamp/... to pass-by-reference. Our
// Datum's value slot is uint64 on every architecture (merely slower on
// 32-bit), so 8-byte by-value is unconditional and these constants are
// fixed, not platform-dependent. SIZEOF_POINTER is the typlen of the
// internal/pg_ddl_command pseudo-types, whose values never exist as datums
// in this engine; NAMEDATALEN=64 assumes stock shard builds.
var datMacros = map[string]string{
	"NAMEDATALEN":     "64",
	"SIZEOF_POINTER":  "8",
	"FLOAT8PASSBYVAL": "t",
}

func substituteMacros(v string) string {
	if sub, ok := datMacros[v]; ok {
		return sub
	}
	return v
}

// Column defaults below transcribe the BKI_DEFAULT annotations from the
// catalog headers; each is cited so a REL upgrade can re-verify them.

func (r *resolver) resolveTypes(entries []Entry) ([]TypeRow, error) {
	rows := make([]TypeRow, 0, len(entries))
	for _, e := range entries {
		row := TypeRow{}
		var err error
		fail := func(what string, ferr error) error {
			return fmt.Errorf("pg_type.dat line %d (%s): %w", e.Line, what, ferr)
		}
		if row.Oid, err = parseOid(e.Fields["oid"]); err != nil {
			return nil, fail("oid", err)
		}
		if row.Name, err = e.Require("pg_type.dat", "typname"); err != nil {
			return nil, err
		}
		lenStr, err := e.Require("pg_type.dat", "typlen")
		if err != nil {
			return nil, err
		}
		l, err := strconv.ParseInt(substituteMacros(lenStr), 10, 16)
		if err != nil {
			return nil, fail("typlen", err)
		}
		row.Len = int16(l)
		byval, err := e.Require("pg_type.dat", "typbyval")
		if err != nil {
			return nil, err
		}
		row.ByVal = substituteMacros(byval) == "t"
		cat, err := e.Require("pg_type.dat", "typcategory")
		if err != nil {
			return nil, err
		}
		row.Category = cat[0]
		row.Preferred = e.Get("typispreferred", "f") == "t" // pg_type.h:88
		in, err := e.Require("pg_type.dat", "typinput")
		if err != nil {
			return nil, err
		}
		if row.Input, err = lookup(r.procKeys, in, "pg_proc", false); err != nil {
			return nil, fail("typinput", err)
		}
		outFn, err := e.Require("pg_type.dat", "typoutput")
		if err != nil {
			return nil, err
		}
		if row.Output, err = lookup(r.procKeys, outFn, "pg_proc", false); err != nil {
			return nil, fail("typoutput", err)
		}
		recv, err := e.Require("pg_type.dat", "typreceive")
		if err != nil {
			return nil, err
		}
		if row.Receive, err = lookup(r.procKeys, recv, "pg_proc", true); err != nil {
			return nil, fail("typreceive", err)
		}
		send, err := e.Require("pg_type.dat", "typsend")
		if err != nil {
			return nil, err
		}
		if row.Send, err = lookup(r.procKeys, send, "pg_proc", true); err != nil {
			return nil, fail("typsend", err)
		}
		// typelem 0 (pg_type.h:120), typarray 0 (pg_type.h:126),
		// typcollation 0 (pg_type.h:228).
		if row.Elem, err = lookup(r.typeOids, e.Get("typelem", "0"), "pg_type", true); err != nil {
			return nil, fail("typelem", err)
		}
		if row.Array, err = lookup(r.typeOids, e.Get("typarray", "0"), "pg_type", true); err != nil {
			return nil, fail("typarray", err)
		}
		if row.Collation, err = lookup(r.collOids, e.Get("typcollation", "0"), "pg_collation", true); err != nil {
			return nil, fail("typcollation", err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (r *resolver) resolveProcs(entries []Entry) ([]ProcRow, error) {
	rows := make([]ProcRow, 0, len(entries))
	for _, e := range entries {
		row := ProcRow{}
		var err error
		fail := func(what string, ferr error) error {
			return fmt.Errorf("pg_proc.dat line %d (%s): %w", e.Line, what, ferr)
		}
		if row.Oid, err = parseOid(e.Fields["oid"]); err != nil {
			return nil, fail("oid", err)
		}
		if row.Name, err = e.Require("pg_proc.dat", "proname"); err != nil {
			return nil, err
		}
		ret, err := e.Require("pg_proc.dat", "prorettype")
		if err != nil {
			return nil, err
		}
		if row.RetType, err = lookup(r.typeOids, ret, "pg_type", false); err != nil {
			return nil, fail("prorettype", err)
		}
		args, err := e.Require("pg_proc.dat", "proargtypes")
		if err != nil {
			return nil, err
		}
		for argName := range strings.FieldsSeq(args) {
			argOid, err := lookup(r.typeOids, argName, "pg_type", false)
			if err != nil {
				return nil, fail("proargtypes", err)
			}
			row.ArgTypes = append(row.ArgTypes, argOid)
		}
		row.Strict = e.Get("proisstrict", "t") == "t" // pg_proc.h:68
		row.Volatile = e.Get("provolatile", "i")[0]   // pg_proc.h:74
		row.RetSet = e.Get("proretset", "f") == "t"   // pg_proc.h:71
		row.Kind = e.Get("prokind", "f")[0]           // pg_proc.h:59
		if row.Src, err = e.Require("pg_proc.dat", "prosrc"); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (r *resolver) resolveOperators(entries []Entry) ([]OperatorRow, error) {
	rows := make([]OperatorRow, 0, len(entries))
	for _, e := range entries {
		row := OperatorRow{}
		var err error
		fail := func(what string, ferr error) error {
			return fmt.Errorf("pg_operator.dat line %d (%s): %w", e.Line, what, ferr)
		}
		if row.Oid, err = parseOid(e.Fields["oid"]); err != nil {
			return nil, fail("oid", err)
		}
		if row.Name, err = e.Require("pg_operator.dat", "oprname"); err != nil {
			return nil, err
		}
		row.Kind = e.Get("oprkind", "b")[0]             // pg_operator.h:45
		row.CanMerge = e.Get("oprcanmerge", "f") == "t" // pg_operator.h:48
		row.CanHash = e.Get("oprcanhash", "f") == "t"   // pg_operator.h:51
		left, err := e.Require("pg_operator.dat", "oprleft")
		if err != nil {
			return nil, err
		}
		if row.Left, err = lookup(r.typeOids, left, "pg_type", true); err != nil {
			return nil, fail("oprleft", err)
		}
		right, err := e.Require("pg_operator.dat", "oprright")
		if err != nil {
			return nil, err
		}
		if row.Right, err = lookup(r.typeOids, right, "pg_type", false); err != nil {
			return nil, fail("oprright", err)
		}
		result, err := e.Require("pg_operator.dat", "oprresult")
		if err != nil {
			return nil, err
		}
		if row.Result, err = lookup(r.typeOids, result, "pg_type", true); err != nil {
			return nil, fail("oprresult", err)
		}
		// oprcom 0, oprnegate 0 (pg_operator.h:63,66).
		if row.Commutator, err = lookup(r.operKeys, e.Get("oprcom", "0"), "pg_operator", true); err != nil {
			return nil, fail("oprcom", err)
		}
		if row.Negator, err = lookup(r.operKeys, e.Get("oprnegate", "0"), "pg_operator", true); err != nil {
			return nil, fail("oprnegate", err)
		}
		code, err := e.Require("pg_operator.dat", "oprcode")
		if err != nil {
			return nil, err
		}
		if row.Code, err = lookup(r.procKeys, code, "pg_proc", true); err != nil {
			return nil, fail("oprcode", err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (r *resolver) resolveCasts(entries []Entry) ([]CastRow, error) {
	rows := make([]CastRow, 0, len(entries))
	for _, e := range entries {
		row := CastRow{}
		var err error
		fail := func(what string, ferr error) error {
			return fmt.Errorf("pg_cast.dat line %d (%s): %w", e.Line, what, ferr)
		}
		src, err := e.Require("pg_cast.dat", "castsource")
		if err != nil {
			return nil, err
		}
		if row.Source, err = lookup(r.typeOids, src, "pg_type", false); err != nil {
			return nil, fail("castsource", err)
		}
		tgt, err := e.Require("pg_cast.dat", "casttarget")
		if err != nil {
			return nil, err
		}
		if row.Target, err = lookup(r.typeOids, tgt, "pg_type", false); err != nil {
			return nil, fail("casttarget", err)
		}
		fn, err := e.Require("pg_cast.dat", "castfunc")
		if err != nil {
			return nil, err
		}
		if row.Func, err = lookup(r.procKeys, fn, "pg_proc", true); err != nil {
			return nil, fail("castfunc", err)
		}
		ctx, err := e.Require("pg_cast.dat", "castcontext")
		if err != nil {
			return nil, err
		}
		row.Context = ctx[0]
		method, err := e.Require("pg_cast.dat", "castmethod")
		if err != nil {
			return nil, err
		}
		row.Method = method[0]
		rows = append(rows, row)
	}
	return rows, nil
}

func parseOid(s string) (uint32, error) {
	if s == "" {
		return 0, errors.New("missing oid")
	}
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid oid %q: %w", s, err)
	}
	return uint32(v), nil
}
