// Copyright 2019 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package servenv

import (
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseProfileFlag(t *testing.T) {
	tests := []struct {
		arg     string
		want    *profile
		wantErr bool
	}{
		{"", nil, false},
		{"mem", &profile{mode: profileMemHeap, rate: 4096}, false},
		{"mem,rate=1234", &profile{mode: profileMemHeap, rate: 1234}, false},
		{"mem,rate", nil, true},
		{"mem,rate=foobar", nil, true},
		{"mem=allocs", &profile{mode: profileMemAllocs, rate: 4096}, false},
		{"mem=allocs,rate=420", &profile{mode: profileMemAllocs, rate: 420}, false},
		{"block", &profile{mode: profileBlock, rate: 1}, false},
		{"block,rate=4", &profile{mode: profileBlock, rate: 4}, false},
		{"cpu", &profile{mode: profileCPU}, false},
		{"cpu,quiet", &profile{mode: profileCPU, quiet: true}, false},
		{"cpu,quiet=true", &profile{mode: profileCPU, quiet: true}, false},
		{"cpu,quiet=false", &profile{mode: profileCPU, quiet: false}, false},
		{"cpu,quiet=foobar", nil, true},
		{"cpu,path=", &profile{mode: profileCPU, path: ""}, false},
		{"cpu,path", nil, true},
		{"cpu,path=a", &profile{mode: profileCPU, path: "a"}, false},
		{"cpu,path=a/b/c/d", &profile{mode: profileCPU, path: "a/b/c/d"}, false},
		{"cpu,waitSig", &profile{mode: profileCPU, waitSig: true}, false},
		{"cpu,path=a/b,waitSig", &profile{mode: profileCPU, waitSig: true, path: "a/b"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.arg, func(t *testing.T) {
			var profileFlag []string
			if tt.arg != "" {
				profileFlag = strings.Split(tt.arg, ",")
			}
			// Create a ServEnv instance to call the method
			sv := NewServEnv()
			got, err := sv.parseProfileFlag(profileFlag)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseProfileFlag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseProfileFlag() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// with waitSig, we should start with profiling off and toggle on-off-on-off
func TestPProfInitWithWaitSig(t *testing.T) {
	signal.Reset(syscall.SIGUSR1)

	// Create a ServEnv instance and set pprofFlag
	sv := NewServEnv()
	sv.pprofFlag.Set(strings.Split("cpu,waitSig", ","))

	sv.pprofInit()
	require.Eventually(t, func() bool {
		return !isProfileStarted()
	}, 2*time.Second, 50*time.Millisecond)

	err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return isProfileStarted()
	}, 2*time.Second, 50*time.Millisecond)

	err = syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return !isProfileStarted()
	}, 2*time.Second, 50*time.Millisecond)

	err = syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return isProfileStarted()
	}, 2*time.Second, 50*time.Millisecond)

	err = syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return !isProfileStarted()
	}, 2*time.Second, 50*time.Millisecond)
}

// without waitSig, we should start with profiling on and toggle off-on-off
func TestPProfInitWithoutWaitSig(t *testing.T) {
	signal.Reset(syscall.SIGUSR1)

	// Create a ServEnv instance and set pprofFlag
	sv := NewServEnv()
	sv.pprofFlag.Set(strings.Split("cpu", ","))

	sv.pprofInit()
	require.Eventually(t, func() bool {
		return isProfileStarted()
	}, 2*time.Second, 50*time.Millisecond)

	err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return !isProfileStarted()
	}, 2*time.Second, 50*time.Millisecond)

	err = syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return isProfileStarted()
	}, 2*time.Second, 50*time.Millisecond)

	err = syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return !isProfileStarted()
	}, 2*time.Second, 50*time.Millisecond)
}
