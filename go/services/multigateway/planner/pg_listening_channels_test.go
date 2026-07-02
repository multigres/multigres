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

package planner

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

func TestPlanPgListeningChannels(t *testing.T) {
	p := NewPlanner("default", slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), nil)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	for _, sql := range []string{
		"SELECT pg_listening_channels()",
		"SELECT pg_catalog.pg_listening_channels()",
		"SELECT * FROM pg_listening_channels()",
	} {
		t.Run(sql, func(t *testing.T) {
			plan, err := p.Plan(sql, parseOne(t, sql), conn, PlanOptions{})
			require.NoError(t, err)
			require.IsType(t, &engine.PgListeningChannels{}, plan.Primitive)
			require.Equal(t, engine.PlanTypePgListeningChannels, plan.Type)
		})
	}
}

func TestPlanPgListeningChannelsRejectsComplexForms(t *testing.T) {
	p := NewPlanner("default", slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), nil)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	for _, sql := range []string{
		"SELECT pg_listening_channels() WHERE true",
		"SELECT channel FROM pg_listening_channels() AS channel",
		"SELECT count(*) FROM pg_listening_channels()",
		"SELECT pg_listening_channels(), 1",
	} {
		t.Run(sql, func(t *testing.T) {
			plan, err := p.Plan(sql, parseOne(t, sql), conn, PlanOptions{})
			require.Nil(t, plan)
			require.Error(t, err)
			require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSFeatureNotSupported), "err=%v", err)
		})
	}
}
