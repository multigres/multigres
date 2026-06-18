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

package mterrors

import "testing"

func TestNewParseError(t *testing.T) {
	d := NewParseError(`syntax error at or near "WHERE"`)
	if d.Code != PgSSSyntaxError {
		t.Errorf("Code = %q, want %q (42601 syntax_error)", d.Code, PgSSSyntaxError)
	}
	if d.Severity != "ERROR" {
		t.Errorf("Severity = %q, want ERROR", d.Severity)
	}
	if !d.IsError() {
		t.Errorf("expected an ErrorResponse diagnostic, MessageType = %q", d.MessageType)
	}
	if d.Message != `syntax error at or near "WHERE"` {
		t.Errorf("Message = %q, want the supplied parser message", d.Message)
	}
	if ExtractSQLSTATE(d) != PgSSSyntaxError {
		t.Errorf("ExtractSQLSTATE = %q, want %q", ExtractSQLSTATE(d), PgSSSyntaxError)
	}
}
