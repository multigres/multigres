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

// Package protoutil provides helper functions for creating proto messages.
package protoutil

import (
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// NewPreparedStatement creates a new PreparedStatement proto message.
func NewPreparedStatement(name, queryStr string, paramTypes []uint32) *query.PreparedStatement {
	return &query.PreparedStatement{
		Name:       name,
		Query:      queryStr,
		ParamTypes: paramTypes,
	}
}

// NewPortal creates a new Portal proto message.
// paramFormats and resultFormats use int16 for compatibility with PostgreSQL wire protocol,
// but are converted to int32 for proto serialization.
// params uses Vitess-style encoding: nil = NULL, []byte{} = empty string.
func NewPortal(name, preparedStatementName string, params [][]byte, paramFormats, resultFormats []int16) *query.Portal {
	// Convert int16 slices to int32 for proto type.
	paramFormats32 := make([]int32, len(paramFormats))
	for i, f := range paramFormats {
		paramFormats32[i] = int32(f)
	}
	resultFormats32 := make([]int32, len(resultFormats))
	for i, f := range resultFormats {
		resultFormats32[i] = int32(f)
	}

	// Encode params using Vitess-style encoding (lengths + concatenated values).
	paramLengths, paramValues := sqltypes.ParamsToProto(params)

	return &query.Portal{
		Name:                  name,
		PreparedStatementName: preparedStatementName,
		ParamLengths:          paramLengths,
		ParamValues:           paramValues,
		ParamFormats:          paramFormats32,
		ResultFormats:         resultFormats32,
	}
}

// TargetEquals checks that the two specified targets are equal or not.
func TargetEquals(t1, t2 *query.Target) bool {
	if t1 == nil && t2 == nil {
		return true
	}
	if t1 == nil || t2 == nil {
		return false
	}
	if t1.GetPoolerType() != t2.GetPoolerType() {
		return false
	}
	if t1.GetShard() != t2.GetShard() {
		return false
	}
	if t1.GetTableGroup() != t2.GetTableGroup() {
		return false
	}
	return true
}
