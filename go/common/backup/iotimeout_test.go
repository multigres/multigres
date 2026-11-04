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

package backup

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIOTimeoutArgs(t *testing.T) {
	withTimeout := func(d time.Duration) context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		t.Cleanup(cancel)
		return ctx
	}

	tests := []struct {
		name string
		ctx  context.Context
		want []string
	}{
		{name: "no deadline leaves pgbackrest's default", ctx: context.Background()},
		{name: "30s context gets two thirds", ctx: withTimeout(30 * time.Second), want: []string{"--io-timeout=19"}},
		{name: "5s context gets a few seconds", ctx: withTimeout(5 * time.Second), want: []string{"--io-timeout=3"}},
		{name: "sub-second context is raised to one second", ctx: withTimeout(500 * time.Millisecond), want: []string{"--io-timeout=1"}},
		{name: "expired context is raised to one second", ctx: withTimeout(-time.Second), want: []string{"--io-timeout=1"}},
		{name: "context longer than the default needs no override", ctx: withTimeout(2 * time.Minute)},
		{name: "context just under the default after scaling", ctx: withTimeout(89 * time.Second), want: []string{"--io-timeout=59"}},
		{name: "context at 90s needs no override", ctx: withTimeout(91 * time.Second)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IOTimeoutArgs(tt.ctx))
		})
	}
}
