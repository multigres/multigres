// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sessionstate provides session state tracking for multigateway.
// It analyzes SQL statements to detect SET/RESET commands and maintains
// connection-level session settings that are propagated to multipooler.
package sessionstate

import (
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/parser/ast"
)

// SetCommandKind represents the type of SET/RESET command.
type SetCommandKind int

const (
	// SetValue represents SET variable = value
	SetValue SetCommandKind = iota
	// ResetVariable represents RESET variable
	ResetVariable
	// ResetAll represents RESET ALL
	ResetAll
	// SetLocal represents SET LOCAL variable = value (transaction-scoped)
	SetLocal
)

// SetCommand represents parsed SET/RESET command information.
type SetCommand struct {
	Kind     SetCommandKind
	Variable string // Variable name (empty for RESET ALL)
	Value    string // Value (empty for RESET)
}

// AnalyzeStatement checks if stmt is a SET/RESET command and extracts information.
// Returns (command, true) if it's a SET/RESET command, (nil, false) otherwise.
//
// Handles:
//   - SET variable = value
//   - SET variable TO value
//   - RESET variable
//   - RESET ALL
//   - SET LOCAL variable = value (detected but marked as SetLocal)
//
// Does not intercept:
//   - SET variable TO DEFAULT (VAR_SET_DEFAULT)
//   - SET variable FROM CURRENT (VAR_SET_CURRENT)
//
// These are passed through to PostgreSQL without tracking.
func AnalyzeStatement(stmt ast.Stmt) (*SetCommand, bool) {
	setStmt, ok := stmt.(*ast.VariableSetStmt)
	if !ok {
		return nil, false
	}

	// Handle SET LOCAL - detect but mark as SetLocal for special handling
	// Phase 1: These are passed through without tracking
	// Phase 2: Will require transaction-scoped settings tracking
	if setStmt.IsLocal {
		return &SetCommand{
			Kind:     SetLocal,
			Variable: setStmt.Name,
		}, true
	}

	switch setStmt.Kind {
	case ast.VAR_SET_VALUE:
		// SET variable = value
		value := extractValue(setStmt.Args)
		return &SetCommand{
			Kind:     SetValue,
			Variable: setStmt.Name,
			Value:    value,
		}, true

	case ast.VAR_RESET:
		// RESET variable
		return &SetCommand{
			Kind:     ResetVariable,
			Variable: setStmt.Name,
		}, true

	case ast.VAR_RESET_ALL:
		// RESET ALL
		return &SetCommand{
			Kind: ResetAll,
		}, true

	default:
		// VAR_SET_DEFAULT, VAR_SET_CURRENT, VAR_SET_MULTI - pass through to PostgreSQL
		return nil, false
	}
}

// extractValue converts ast.NodeList arguments to a string value.
// Handles:
//   - Single values: SET search_path = 'public'
//   - Multiple values: SET search_path = 'schema1', 'schema2'
//   - Integer values: SET work_mem = 1024
//   - A_Const wrapped values: SET search_path = a (unquoted)
//   - Complex expressions: Falls back to SqlString()
func extractValue(args *ast.NodeList) string {
	if args == nil || args.Len() == 0 {
		return ""
	}

	// Handle multiple args (e.g., search_path = 'schema1', 'schema2')
	var values []string
	for _, arg := range args.Items {
		switch v := arg.(type) {
		case *ast.A_Const:
			// A_Const wraps the actual value - unwrap it
			values = append(values, extractConstValue(v))
		case *ast.String:
			// Direct String literal - SVal is already unquoted
			values = append(values, v.SVal)
		case *ast.Integer:
			// Direct Integer literal
			values = append(values, strconv.Itoa(v.IVal))
		default:
			// For complex types (expressions, etc.), use SqlString() as fallback
			values = append(values, arg.SqlString())
		}
	}

	// Join multiple values with ", " (PostgreSQL format for multi-value settings)
	return strings.Join(values, ", ")
}

// extractConstValue extracts the string value from an A_Const node.
// A_Const wraps various value types (String, Integer, Float, etc.).
func extractConstValue(aConst *ast.A_Const) string {
	if aConst == nil || aConst.Val == nil {
		return ""
	}

	switch val := aConst.Val.(type) {
	case *ast.String:
		// String value - use SVal directly (already unquoted by parser)
		return val.SVal
	case *ast.Integer:
		// Integer value
		return strconv.Itoa(val.IVal)
	case *ast.Float:
		// Float value
		return val.FVal
	default:
		// For other types, use SqlString() as fallback
		return aConst.SqlString()
	}
}
