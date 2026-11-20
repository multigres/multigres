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

//go:build linux

package sandbox

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/landlock-lsm/go-landlock/landlock"
	ll "github.com/landlock-lsm/go-landlock/landlock/syscall"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPolicyEncode_SamePathDifferentAccess(t *testing.T) {
	policy := Policy{Rules: []Rule{{Path: "/data", Access: Write}, {Path: "/data", Access: Read}}}
	encoded := policy.encode()
	decoded, err := decodePolicy(encoded)
	require.NoError(t, err)
	assert.Equal(t, Policy{Rules: []Rule{{Path: "/data", Access: Read}, {Path: "/data", Access: Write}}}, decoded)
	assert.Equal(t, Write, policy.Rules[0].Access, "encoding must not mutate the input")
	assert.Equal(t, encoded, decoded.encode(), "canonical encoding must survive a round trip")
	assert.Equal(t, (Policy{}).encode(), (Policy{Rules: []Rule{}}).encode())
}

func TestLandlockRule_RightsByFileTypeAndABI(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	writeFile(t, file)
	for _, tt := range []struct {
		name   string
		path   string
		access Access
		abi    int
		want   uint64
	}{
		{"directory-read", dir, Read, 3, accessRead},
		{"directory-write", dir, Write, 8, accessWrite},
		{"directory-exec", dir, Exec, 9, accessExec},
		{"directory-resolve-unix", dir, Read | Write, 9, accessRead | accessWrite | ll.AccessFSResolveUnix},
		{"file-read", file, Read, 3, ll.AccessFSReadFile},
		{"file-write-exec", file, Write | Exec, 8, ll.AccessFSWriteFile | ll.AccessFSTruncate | ll.AccessFSExecute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (Rule{Path: tt.path, Access: tt.access}).landlockRule(tt.abi)
			require.NoError(t, err)
			assert.Equal(t, landlock.PathAccess(landlock.AccessFSSet(tt.want), tt.path), got)
		})
	}
}

func TestRestrictAt_MissingRuleFailsBeforeConfinement(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	err := restrictAt(Policy{Rules: []Rule{{Path: missing, Access: Read}}}, minABI)
	require.ErrorIs(t, err, os.ErrNotExist)
	assert.ErrorContains(t, err, "sandbox: rule path")
}

func TestOpenAnchor_MissingAndRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	for _, mode := range []AnchorMode{AnchorDir, AnchorMount} {
		f, err := OpenAnchor(path, mode)
		require.ErrorIs(t, err, os.ErrNotExist)
		assert.Nil(t, f)
	}
	writeFile(t, path)
	for _, mode := range []AnchorMode{AnchorDir, AnchorMount} {
		f, err := OpenAnchor(path, mode)
		require.ErrorContains(t, err, "not a directory")
		assert.Nil(t, f)
	}
}
