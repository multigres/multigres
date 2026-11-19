// Copyright 2026 Supabase, Inc.
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

package shardindex_test

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/shardindex"
)

// This file is in package shardindex_test on purpose: it can only use what
// the package exports, so it proves that an implementation living in another
// package, as every user-provided one does, can be written, registered and
// used.

// identityIndex is a user-provided implementation as a third party would
// write one: the routing identifier of an int8 is the value itself, offset so
// that byte order is numeric order, optionally under a fixed prefix byte.
type identityIndex struct {
	name   string
	prefix []byte
}

const identityType = "test_identity"

func init() {
	shardindex.Register(identityType, func(name string, params map[string]string) (shardindex.ShardIndex, error) {
		if err := shardindex.CheckParams(identityType, name, params, "prefix"); err != nil {
			return nil, err
		}
		return &identityIndex{name: name, prefix: []byte(params["prefix"])}, nil
	})
}

func (x *identityIndex) Name() string   { return x.name }
func (x *identityIndex) Type() string   { return identityType }
func (x *identityIndex) Cost() int      { return 0 }
func (x *identityIndex) IsUnique() bool { return true }

func (x *identityIndex) Map(_ context.Context, _ shardindex.Executor, values []shardindex.Value) ([]shardindex.Destination, error) {
	out := make([]shardindex.Destination, len(values))
	for i, v := range values {
		if v.IsNull {
			// A user-provided index may choose where NULL goes.
			out[i] = shardindex.DestinationNone()
			continue
		}
		n := uint64(datum.DatumGetInt64(v.Datum)) ^ (1 << 63)
		out[i] = shardindex.DestinationRoutingID(binary.BigEndian.AppendUint64(append([]byte(nil), x.prefix...), n))
	}
	return out, nil
}

func (x *identityIndex) Verify(ctx context.Context, exec shardindex.Executor, values []shardindex.Value, routingIDs [][]byte) ([]bool, error) {
	destinations, err := x.Map(ctx, exec, values)
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(values))
	for i, d := range destinations {
		out[i] = d.Kind() == shardindex.KindRoutingID && string(d.RoutingID()) == string(routingIDs[i])
	}
	return out, nil
}

func TestNew_UserProvidedImplementation(t *testing.T) {
	t.Parallel()
	index, err := shardindex.New(identityType, "by_tenant", map[string]string{"prefix": "\x40"})
	require.NoError(t, err)
	assert.Equal(t, "by_tenant", index.Name())
	assert.Equal(t, identityType, index.Type())

	// Capabilities are found by type assertion: this one maps single
	// columns but neither hashes nor restricts types.
	single, ok := index.(shardindex.SingleColumn)
	require.True(t, ok)
	_, ok = index.(shardindex.Hashing)
	assert.False(t, ok)
	_, ok = index.(shardindex.TypeChecking)
	assert.False(t, ok)

	values := []shardindex.Value{
		{Type: pgoid.INT8OID, Datum: datum.Int64GetDatum(-1)},
		{Type: pgoid.INT8OID, Datum: datum.Int64GetDatum(1)},
		{Type: pgoid.INT8OID, IsNull: true},
	}
	destinations, err := single.Map(context.Background(), nil, values)
	require.NoError(t, err)
	require.Len(t, destinations, 3)
	assert.Equal(t, "RoutingID(407fffffffffffffff)", destinations[0].String())
	assert.Equal(t, "RoutingID(408000000000000001)", destinations[1].String())
	assert.Equal(t, shardindex.KindNone, destinations[2].Kind())

	verified, err := single.Verify(context.Background(), nil, values[:2],
		[][]byte{destinations[0].RoutingID(), destinations[0].RoutingID()})
	require.NoError(t, err)
	assert.Equal(t, []bool{true, false}, verified)

	// Its factory validates its own parameters.
	_, err = shardindex.New(identityType, "by_tenant", map[string]string{"prefix": "a", "suffix": "b"})
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, mterrors.PgSSInvalidParameterValue, diag.Code)
}
