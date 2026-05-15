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

package shardsetup

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWithVpidStamping verifies that the WithVpidStamping setup option
// flips SetupConfig.EnableVpidStamping to true. The wire-in from
// EnableVpidStamping to inst.Multipooler.VpidStampEnabled lives inside
// New() and is only exercised when a real cluster is spun up (i.e. by the
// pgregress isolation suite); covering it here keeps the option's intent
// asserted without requiring the gated integration path.
func TestWithVpidStamping(t *testing.T) {
	var cfg SetupConfig
	assert.False(t, cfg.EnableVpidStamping, "default should be false")

	WithVpidStamping()(&cfg)
	assert.True(t, cfg.EnableVpidStamping, "WithVpidStamping should set EnableVpidStamping=true")
}

// TestMultipoolerArgs_VpidStampEnabled checks that multipoolerArgs appends
// --vpid-stamp-enabled=true exactly when VpidStampEnabled is set on the
// ProcessInstance. The real startMultipooler then passes these args to
// executil.Command; testing the builder directly avoids spawning the
// multipooler binary.
func TestMultipoolerArgs_VpidStampEnabled(t *testing.T) {
	t.Run("disabled omits the flag", func(t *testing.T) {
		p := &ProcessInstance{VpidStampEnabled: false}
		args := p.multipoolerArgs()
		assert.False(t, slices.Contains(args, "--vpid-stamp-enabled=true"),
			"expected --vpid-stamp-enabled=true to be absent when VpidStampEnabled is false")
	})

	t.Run("enabled appends the flag", func(t *testing.T) {
		p := &ProcessInstance{VpidStampEnabled: true}
		args := p.multipoolerArgs()
		assert.True(t, slices.Contains(args, "--vpid-stamp-enabled=true"),
			"expected --vpid-stamp-enabled=true to be present when VpidStampEnabled is true")
	})
}
