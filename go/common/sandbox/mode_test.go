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

package sandbox

import (
	"errors"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMode(t *testing.T) {
	for _, m := range []Mode{ModeRequired, ModeAuto, ModeOff} {
		got, err := ParseMode(string(m))
		require.NoError(t, err)
		assert.Equal(t, m, got)
	}
	for _, bad := range []string{"", "Required", "on", "none", "auto "} {
		_, err := ParseMode(bad)
		assert.Error(t, err, "%q", bad)
	}
}

// The errno table decides whether a misconfigured seccomp profile turns into
// unconfined children, so every row is pinned.
func TestProbeWithErrnoTable(t *testing.T) {
	tests := []struct {
		name        string
		abi         int
		err         error
		supported   bool
		wantABI     int
		unsupported string // substring of the reason
		fatal       string // substring of the error
	}{
		{name: "floor", abi: minABI, supported: true, wantABI: minABI},
		{name: "newest", abi: 11, supported: true, wantABI: 11},
		{name: "below floor", abi: minABI - 1, unsupported: "below the minimum"},
		{name: "zero", abi: 0, unsupported: "below the minimum"},
		{name: "ENOSYS", abi: -1, err: syscall.ENOSYS, unsupported: "ENOSYS"},
		{name: "EOPNOTSUPP", abi: -1, err: syscall.EOPNOTSUPP, unsupported: "EOPNOTSUPP"},
		{name: "wrapped ENOSYS", abi: -1, err: errors.Join(errors.New("x"), syscall.ENOSYS), unsupported: "ENOSYS"},
		{name: "EPERM", abi: -1, err: syscall.EPERM, fatal: "seccomp"},
		{name: "EACCES", abi: -1, err: syscall.EACCES, fatal: "not an unsupported kernel"},
		{name: "EINVAL", abi: -1, err: syscall.EINVAL, fatal: "invalid argument"},
		{name: "EFAULT", abi: -1, err: syscall.EFAULT, fatal: "not an unsupported kernel"},
		{name: "unexpected error", abi: -1, err: errors.New("boom"), fatal: "boom"},
		// An error wins over whatever the call returned alongside it.
		{name: "error with a good ABI", abi: 7, err: syscall.EPERM, fatal: "EPERM"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := ProbeWith(func() (int, error) { return tt.abi, tt.err })
			if tt.fatal != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.fatal)
				if errors.Is(tt.err, syscall.EPERM) {
					assert.ErrorContains(t, err, "EPERM")
					assert.ErrorContains(t, err, "seccomp")
				}
				assert.False(t, c.Supported)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.supported, c.Supported)
			assert.Equal(t, tt.wantABI, c.ABI)
			if !tt.supported {
				assert.Contains(t, c.Reason, tt.unsupported)
			}
		})
	}
}

func fixedProbe(c Capability, err error, calls *int) ProbeFunc {
	return func() (Capability, error) {
		*calls++
		return c, err
	}
}

func TestDecide(t *testing.T) {
	supported := Capability{Supported: true, ABI: 7}
	unsupported := Capability{Reason: "the kernel has no Landlock (ENOSYS)"}
	fatal := errors.New("probe: EPERM, seccomp may be blocking landlock_*")

	tests := []struct {
		name       string
		mode       Mode
		cap        Capability
		probeErr   error
		wantProbes int
		effective  Effective
		wantErr    string
	}{
		{name: "required supported", mode: ModeRequired, cap: supported, wantProbes: 1, effective: EffectiveEnforced},
		{name: "required unsupported fails closed", mode: ModeRequired, cap: unsupported, wantProbes: 1, wantErr: "--sandbox=required"},
		{name: "required probe error", mode: ModeRequired, probeErr: fatal, wantProbes: 1, wantErr: "seccomp"},
		{name: "auto supported behaves as required", mode: ModeAuto, cap: supported, wantProbes: 1, effective: EffectiveEnforced},
		{name: "auto unsupported degrades", mode: ModeAuto, cap: unsupported, wantProbes: 1, effective: EffectiveUnconfined},
		{name: "auto probe error does not degrade", mode: ModeAuto, probeErr: fatal, wantProbes: 1, wantErr: "seccomp"},
		{name: "off never probes", mode: ModeOff, cap: supported, wantProbes: 0, effective: EffectiveUnconfined},
		{name: "unknown mode", mode: Mode("on"), wantErr: "unknown mode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls int
			d, err := Decide(tt.mode, fixedProbe(tt.cap, tt.probeErr, &calls))
			assert.Equal(t, tt.wantProbes, calls)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.effective, d.Effective)
			assert.Equal(t, tt.mode, d.Requested)
			assert.Equal(t, tt.effective == EffectiveEnforced, d.Enforced())
		})
	}
}

func TestDecideRequiredUnsupportedWrapsErrUnsupported(t *testing.T) {
	var calls int
	_, err := Decide(ModeRequired, fixedProbe(Capability{Reason: "x"}, nil, &calls))
	assert.ErrorIs(t, err, ErrUnsupported)
	assert.ErrorContains(t, err, "MULTIGRES_PGBACKREST_SANDBOX")
}

func TestCapabilityText(t *testing.T) {
	assert.Equal(t, "Landlock ABI 7", Decision{Probed: true, Capability: Capability{Supported: true, ABI: 7}}.CapabilityText())
	assert.Equal(t, "unsupported: why", Decision{Probed: true, Capability: Capability{Reason: "why"}}.CapabilityText())
	assert.Contains(t, Decision{}.CapabilityText(), "not probed")
}
