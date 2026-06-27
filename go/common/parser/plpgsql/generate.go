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

package plpgsql

// Generate the PL/pgSQL parser using goyacc.
// The -p plpgsql prefix keeps generated identifiers (plpgsqlParser,
// plpgsqlSymType, etc.) from colliding with the main SQL parser in
// go/common/parser/postgres.y.

//go:generate go run ../goyacc -f -p plpgsql -o plpgsql.go plpgsql.y
//go:generate go tool goimports -w plpgsql.go
//go:generate go tool gofumpt -w plpgsql.go
