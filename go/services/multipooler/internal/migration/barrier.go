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
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// slotPollInterval is how often the drain barrier checks slot confirmation.
const slotPollInterval = 500 * time.Millisecond

// ownedSequencesSQL lists (column, owned-sequence-or-null) for every live column
// of a table, given the (optionally schema-qualified) table name as $1.
const ownedSequencesSQL = `SELECT a.attname, pg_get_serial_sequence($1, a.attname)
FROM pg_attribute a
WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped`

// setvalSQL builds a setval that advances a sequence past its column's current
// max plus margin (never below 1, since setval(seq, 0) is out of range).
func setvalSQL(seq, col, table string, margin int64) string {
	return fmt.Sprintf(
		"SELECT setval(%s, GREATEST((SELECT coalesce(max(%s), 0) FROM %s) + %d, 1), true)",
		ast.QuoteStringLiteral(seq), ast.QuoteIdentifier(col), quoteQualifiedName(table), margin)
}

// pollSlotConfirmed polls check until it returns true or ctx is done.
func pollSlotConfirmed(ctx context.Context, check func(context.Context) (bool, error)) error {
	ticker := time.NewTicker(slotPollInterval)
	defer ticker.Stop()
	for {
		ok, err := check(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
