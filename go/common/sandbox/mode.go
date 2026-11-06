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
	"fmt"
	"syscall"

	"github.com/multigres/multigres/go/common/constants"
)

// minABI is the lowest Landlock ABI that handles the full filesystem right
// set the policy relies on (truncate arrived in ABI 3). Below it a ruleset
// would leave rights unhandled, and therefore unrestricted.
const minABI = 3

// Mode is what a deployment asks for: whether pgBackRest children are
// confined. See [Decide] for how it becomes an [Effective] outcome.
type Mode string

const (
	// ModeRequired fails closed: no Landlock, no start.
	ModeRequired Mode = constants.SandboxRequired
	// ModeAuto confines when the host can and runs unconfined when it
	// reports Landlock as unsupported.
	ModeAuto Mode = constants.SandboxAuto
	// ModeOff never uses Landlock.
	ModeOff Mode = constants.SandboxOff
)

// ParseMode validates s. The empty string is not a mode; callers default it.
func ParseMode(s string) (Mode, error) {
	if err := constants.ValidateSandboxMode(s); err != nil {
		return "", err
	}
	return Mode(s), nil
}

// Effective is what [Decide] settled on, once, for the life of the process.
type Effective string

const (
	// EffectiveEnforced means every child runs under Landlock.
	EffectiveEnforced Effective = "enforced"
	// EffectiveUnconfined means no child is confined.
	EffectiveUnconfined Effective = "unconfined"
)

// Capability is what the host offers.
type Capability struct {
	// Supported is true when Landlock works at ABI >= the floor.
	Supported bool
	// ABI is the kernel's Landlock ABI version when Supported.
	ABI int
	// Reason says why Landlock is unsupported; empty when Supported.
	Reason string
}

func (c Capability) String() string {
	if c.Supported {
		return fmt.Sprintf("Landlock ABI %d", c.ABI)
	}
	return "unsupported: " + c.Reason
}

// ProbeFunc reports what the host offers. An error means the probe could not
// tell, which is never treated as "unsupported".
type ProbeFunc func() (Capability, error)

// classifyProbe turns the result of landlock_create_ruleset(NULL, 0, VERSION)
// into a [Capability] or a fatal error. Only ENOSYS (no syscall: old kernel or
// Landlock not built), EOPNOTSUPP (Landlock disabled at boot) and a version
// below the floor mean unsupported. Any other errno, EPERM above all, is what
// a seccomp profile that blocks landlock_* typically returns: degrading on it
// would turn a misconfigured profile into silently unconfined children.
func classifyProbe(abi int, err error) (Capability, error) {
	switch {
	case err == nil:
		if abi < minABI {
			return Capability{Reason: fmt.Sprintf("kernel ABI %d is below the minimum %d", abi, minABI)}, nil
		}
		return Capability{Supported: true, ABI: abi}, nil
	case errors.Is(err, syscall.ENOSYS):
		return Capability{Reason: "the kernel has no Landlock (ENOSYS)"}, nil
	case errors.Is(err, syscall.EOPNOTSUPP):
		return Capability{Reason: "Landlock is disabled at boot (EOPNOTSUPP)"}, nil
	default:
		return Capability{}, fmt.Errorf("probing Landlock failed with %w; this is not an unsupported kernel, "+
			"and a seccomp profile that blocks the landlock_* syscalls typically returns EPERM: allow landlock_create_ruleset, "+
			"landlock_add_rule and landlock_restrict_self", err)
	}
}

// ProbeWith classifies the result of fn, the landlock_create_ruleset version
// query, so the errno table is testable on any host.
func ProbeWith(fn func() (int, error)) (Capability, error) {
	abi, err := fn()
	return classifyProbe(abi, err)
}

// Decision is the outcome of [Decide].
type Decision struct {
	Requested Mode
	// Probed is false in ModeOff, which never asks the kernel.
	Probed     bool
	Capability Capability
	Effective  Effective
}

// Enforced reports whether children run under Landlock.
func (d Decision) Enforced() bool { return d.Effective == EffectiveEnforced }

// CapabilityText is the capability as logged.
func (d Decision) CapabilityText() string {
	if !d.Probed {
		return "not probed (sandbox is off)"
	}
	return d.Capability.String()
}

// Decide settles, once, whether children are confined. ModeOff does not probe.
// ModeRequired fails when the host reports Landlock unsupported; ModeAuto runs
// unconfined then. A probe that cannot tell fails in every mode.
func Decide(mode Mode, probe ProbeFunc) (Decision, error) {
	d := Decision{Requested: mode}
	switch mode {
	case ModeOff:
		d.Effective = EffectiveUnconfined
		return d, nil
	case ModeRequired, ModeAuto:
	default:
		return d, fmt.Errorf("sandbox: unknown mode %q", mode)
	}
	c, err := probe()
	if err != nil {
		return d, err
	}
	d.Probed, d.Capability = true, c
	switch {
	case c.Supported:
		d.Effective = EffectiveEnforced
	case mode == ModeAuto:
		d.Effective = EffectiveUnconfined
	default:
		return d, fmt.Errorf("%w: %s: %s", ErrUnsupported, RequirementMessage, c.Reason)
	}
	return d, nil
}
