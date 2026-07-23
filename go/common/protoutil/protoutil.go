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
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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

// NewTarget builds a *query.Target with the new {ShardKey, Mode} shape.
// Use mode=MODE_UNSPECIFIED if the caller is unspecified (tests that only
// care about table group / shard plumbing).
func NewTarget(database, tableGroup, shard string, mode query.Mode) *query.Target {
	return &query.Target{
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   database,
			TableGroup: tableGroup,
			Shard:      shard,
		},
		Mode: mode,
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
	if t1.GetMode() != t2.GetMode() {
		return false
	}
	return proto.Equal(t1.GetShardKey(), t2.GetShardKey())
}
