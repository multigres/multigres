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
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"

	"github.com/multigres/multigres/go/common/constants"
)

// AnchorMode selects what [OpenAnchor] demands of the root it opens.
type AnchorMode int

const (
	// AnchorMount requires the data-volume root to be a mount point.
	AnchorMount AnchorMode = iota
	// AnchorDir accepts any directory; tests and the local provisioner only.
	AnchorDir
)

// Access is a bit set of what a rule grants.
type Access uint8

const (
	Read Access = 1 << iota
	Write
	Exec
)

// Rule grants Access to Path and everything beneath it.
//
// Path is resolved by name, following symlinks, in the process that applies
// the policy. Every component of it must therefore lie outside any directory
// an attacker can write to. For the data volume that means the anchored root
// only (the mount point cannot be renamed or replaced); never a subpath such
// as PGDATA or the socket directory, which postgres could redirect.
type Rule struct {
	Path   string
	Access Access
}

// Policy is the complete set of grants; anything absent is denied.
type Policy struct {
	Rules []Rule
}

// ErrUnsupported means Landlock is unavailable on this kernel or OS.
var ErrUnsupported = errors.New("sandbox: Landlock unavailable")

// RequirementMessage is the one explanation shown wherever a binary or the
// local provisioner refuses to start in --sandbox=required without Landlock.
const RequirementMessage = constants.SidecarHostRequirement

// SubcommandName is the argv[1] the host binary must route to [ExecMain].
const SubcommandName = "sandbox-exec"

// encode serializes the policy deterministically: rules are sorted so that
// identical policies yield identical argv, and the input is not mutated.
func (p Policy) encode() string {
	rules := slices.Clone(p.Rules)
	if rules == nil {
		rules = []Rule{}
	}
	slices.SortFunc(rules, func(a, b Rule) int {
		if c := strings.Compare(a.Path, b.Path); c != 0 {
			return c
		}
		return int(a.Access) - int(b.Access)
	})
	// A struct of strings and integers cannot fail to marshal.
	raw, _ := json.Marshal(Policy{Rules: rules})
	return base64.StdEncoding.EncodeToString(raw)
}

func decodePolicy(s string) (Policy, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return Policy{}, fmt.Errorf("sandbox: decoding policy: %w", err)
	}
	var p Policy
	if err := json.Unmarshal(raw, &p); err != nil {
		return Policy{}, fmt.Errorf("sandbox: parsing policy: %w", err)
	}
	return p, nil
}

// Command returns an *exec.Cmd that runs argv under policy by re-executing
// self ("<self> sandbox-exec --policy <base64 JSON> -- argv...").
func Command(ctx context.Context, self string, policy Policy, argv []string) *exec.Cmd {
	args := append([]string{SubcommandName, "--policy", policy.encode(), "--"}, argv...)
	// The brief's contract is a plain *exec.Cmd; executil.Cmd only adds
	// graceful termination when its own Start/Wait are used, which a bare
	// embedded *exec.Cmd would bypass.
	//nolint:gocritic // see above
	return exec.CommandContext(ctx, self, args...)
}

// restrictSelf is a variable so staticcheck does not conclude that the exec in
// ExecMain is dead code on hosts where RestrictSelf always fails.
var restrictSelf = RestrictSelf

// ExecMain is called by the binary's "sandbox-exec" subcommand with the
// arguments after the subcommand name: it decodes the policy, applies it with
// [RestrictSelf], then replaces the process with argv[0].
//
// There is deliberately no mode parameter and no unconfined path: a caller
// that decided not to confine does not wrap at all, so a wrong default can
// only fail, never run a child unconfined. Every failure, including an absent
// or malformed policy or a kernel without Landlock, returns before anything
// is executed.
func ExecMain(args []string) error {
	fs := flag.NewFlagSet(SubcommandName, flag.ContinueOnError)
	// The wrapped error carries the message; the package does not write to
	// stderr itself.
	fs.SetOutput(io.Discard)
	policyArg := fs.String("policy", "", "base64 JSON policy")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("sandbox: %w", err)
	}
	argv := fs.Args()
	if *policyArg == "" || len(argv) == 0 {
		return errors.New("sandbox: usage: sandbox-exec --policy <base64 JSON> -- argv...")
	}
	policy, err := decodePolicy(*policyArg)
	if err != nil {
		return err
	}
	// The lookup happens before confinement, so PATH resolution is not
	// subject to the policy.
	path, err := exec.LookPath(argv[0])
	if err != nil {
		return fmt.Errorf("sandbox: resolving %q: %w", argv[0], err)
	}
	if err := restrictSelf(policy); err != nil {
		return err
	}
	if err := syscall.Exec(path, argv, os.Environ()); err != nil {
		return fmt.Errorf("sandbox: exec %q: %w", path, err)
	}
	return nil
}
