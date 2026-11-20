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

package pgbackrestd

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestWorkerMarkerRejectsUnscopedRecords(t *testing.T) {
	for _, data := range []string{"unsettled\n", "{}", `{"version":2,"owner":{"pid":1}}`, `{"version":1,"owner":{"pid":1,"token":"not-an-id"}}`} {
		_, err := decodeWorkerMarker([]byte(data))
		require.Error(t, err)
	}
}

func TestWorkerMarkerFreshProcessIsNotProof(t *testing.T) {
	s := newTestServer(t, nil)
	owner := s.workerOwner
	// The old owner can have another token/PID/start time without the namespace
	// ending: an orphaned worker still shares this domain. Never retire it.
	owner.Token = uuid.NewString()
	owner.PID = 424242
	owner.StartTicks++
	data, err := json.Marshal(workerMarker{Version: 1, Owner: owner})
	require.NoError(t, err)
	path := s.cfg.stateSub("worker-orphan.pending")
	require.NoError(t, privateWrite(path, data))
	require.NoError(t, s.Close())
	_, err = newForTest(s.cfg)
	require.ErrorContains(t, err, "prior process domain is not proven ended")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, after)
}

func TestWorkerMarkerRejectsMissingKernelIdentity(t *testing.T) {
	base := workerOwner{Platform: "linux", Token: uuid.NewString(), PID: 1, StartTicks: 100, BootID: uuid.NewString(), NamespaceDevice: 4, NamespaceInode: 100, Hostname: "slot"}
	for _, tc := range []struct {
		name   string
		change func(*workerOwner)
	}{
		{"missing device", func(o *workerOwner) { o.NamespaceDevice = 0 }},
		{"missing inode", func(o *workerOwner) { o.NamespaceInode = 0 }},
		{"missing start", func(o *workerOwner) { o.StartTicks = 0 }},
		{"nil boot", func(o *workerOwner) { o.BootID = uuid.Nil.String() }},
		{"nil token", func(o *workerOwner) { o.Token = uuid.Nil.String() }},
		{"missing platform", func(o *workerOwner) { o.Platform = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := base
			tc.change(&owner)
			data, err := json.Marshal(workerMarker{Version: 1, Owner: owner})
			require.NoError(t, err)
			_, err = decodeWorkerMarker(data)
			require.Error(t, err)
		})
	}
}
