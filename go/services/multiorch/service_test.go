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

package multiorch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
)

func TestDisableEnableRecovery(t *testing.T) {
	mo := &MultiOrch{}
	// Note: recoveryEngine is nil - should handle gracefully

	server := NewMultiOrchServer(mo)

	// Test DisableRecovery with nil engine
	disableResp, err := server.DisableRecovery(context.Background(), &multiorchpb.DisableRecoveryRequest{})
	require.NoError(t, err)
	require.False(t, disableResp.Success)
	require.Contains(t, disableResp.Message, "not initialized")

	// Test EnableRecovery with nil engine
	enableResp, err := server.EnableRecovery(context.Background(), &multiorchpb.EnableRecoveryRequest{})
	require.NoError(t, err)
	require.False(t, enableResp.Success)
	require.Contains(t, enableResp.Message, "not initialized")

	// Test GetRecoveryStatus with nil engine
	statusResp, err := server.GetRecoveryStatus(context.Background(), &multiorchpb.GetRecoveryStatusRequest{})
	require.NoError(t, err)
	require.False(t, statusResp.Enabled)
}
