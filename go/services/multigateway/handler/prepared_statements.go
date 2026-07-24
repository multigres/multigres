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

package handler

import "github.com/multigres/multigres/go/common/preparedstatement"

// MarkSQLPreparedStatementAlias includes a validated SQL PREPARE name in
// backend alias reconciliation. Protocol-level names remain gateway-local.
func (h *MultigatewayHandler) MarkSQLPreparedStatementAlias(connID uint32, name string) {
	h.psc.MarkSQLAlias(connID, name)
}

// LogicalPreparedStatements returns the aliases that must exist on a backend
// before server-side dynamic SQL can resolve this client's prepared names.
func (h *MultigatewayHandler) LogicalPreparedStatements(connID uint32) []*preparedstatement.LogicalPreparedStatement {
	return h.psc.LogicalPreparedStatements(connID)
}
