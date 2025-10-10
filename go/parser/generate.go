// Copyright 2025 Supabase, Inc.
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

package parser

// Generate the PostgreSQL parser using goyacc
// Ported from vitess/go/vt/sqlparser/generate.go:21
// This will process postgres.y grammar file and generate postgres.go parser

//go:generate goyacc -o postgres.go postgres.y
//go:generate go tool goimports -w postgres.go

// TODO: In Phase 3, add additional generation directives for:
// - AST helper generation
// - Code formatting
// - Error handling customization
//
// Following Vitess patterns:
// //go:generate go run ../../tools/asthelpergen/main --in . --iface ...
// //go:generate go run ../../tools/astfmtgen ...
//
// These will be implemented once we have the basic parser structure in place.
