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

// Package removedflags keeps the names of command-line flags that no longer
// configure anything registered, so that a process given one refuses to start
// with an explanation. It imports nothing of multigres: every binary that had
// such a flag can use it without taking on another package's dependencies.
package removedflags

import (
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/pflag"
)

// These flags configured the pgBackRest that multipooler and pgctld ran
// themselves. Both flag sets were removed: every pgBackRest command runs in the
// pgbackrestd sidecar. A multigres-operator that does not deploy the sidecar
// still passes them, and a pod started that way has no sidecar to archive WAL
// or take backups, so the flags stay registered for one purpose: a process
// given one refuses to start and says why. They are never read as settings.
var (
	// Multipooler are the removed flags of multipooler (and of
	// minigres, which takes the multipooler's flags).
	Multipooler = []string{
		"pgbackrest-cert-file",
		"pgbackrest-key-file",
		"pgbackrest-ca-file",
		"pgbackrest-port",
		"pgbackrest-cipher-key-file",
	}
	// PgctldServer are the removed flags of `pgctld server`.
	PgctldServer = []string{
		"pgbackrest-port",
		"pgbackrest-cert-dir",
	}
)

// Register registers each name as a hidden flag that takes any
// value, so that a command line carrying it parses and Check can
// refuse it with an explanation instead of pflag's "unknown flag".
func Register(fs *pflag.FlagSet, names []string) {
	for _, name := range names {
		fs.String(name, "", "removed: pgBackRest runs in the pgbackrestd sidecar")
		// MarkHidden fails only for a flag that is not registered.
		_ = fs.MarkHidden(name)
	}
}

// Check returns an error when the command line set any of the
// removed flags. Call it before anything else at startup: binary is the
// program's name for the message.
func Check(fs *pflag.FlagSet, binary string, names []string) error {
	var set []string
	for _, name := range names {
		if f := fs.Lookup(name); f != nil && f.Changed {
			set = append(set, "--"+name)
		}
	}
	if len(set) == 0 {
		return nil
	}
	verb := "flag %s was removed"
	if len(set) > 1 {
		verb = "flags %s were removed"
	}
	return fmt.Errorf(verb+": pgBackRest runs in the pgbackrestd sidecar, and this %s needs a multigres-operator that deploys that sidecar and does not pass the flag",
		strings.Join(set, ", "), binary)
}

// CipherKeyFileEnv named the cipher key file for multipooler (and minigres)
// when --pgbackrest-cipher-key-file was not given. The cipher keys belong to
// the sidecar (pgbackrestd --cipher-key-file). A process that still has the
// variable was deployed to encrypt its repository this way, and starting it
// would leave a new repository unencrypted without a word.
const CipherKeyFileEnv = "MULTIGRES_PGBACKREST_CIPHER_KEY_FILE"

// CheckEnv returns an error when the environment sets CipherKeyFileEnv, to any
// value, the empty string included. lookup is os.LookupEnv. Call it with
// Check, before anything else at startup.
func CheckEnv(lookup func(string) (string, bool), binary string) error {
	if _, ok := lookup(CipherKeyFileEnv); !ok {
		return nil
	}
	return fmt.Errorf("environment variable %s was removed: pgBackRest runs in the pgbackrestd sidecar, which holds the cipher keys, and this %s needs a multigres-operator that deploys that sidecar and does not set the variable",
		CipherKeyFileEnv, binary)
}

// CheckConfig returns an error when a config file sets any of the removed
// settings: the flags were also config file keys of the same name. settings is
// the file's own content, keyed in dotted form; a key counts under its bare
// name and under any dotted prefix (minigres keeps the multipooler's settings
// under "multipooler.").
func CheckConfig(settings map[string]any, binary string, names []string) error {
	var set []string
	for key := range settings {
		name := key
		if i := strings.LastIndex(key, "."); i >= 0 {
			name = key[i+1:]
		}
		if slices.Contains(names, name) {
			set = append(set, key)
		}
	}
	if len(set) == 0 {
		return nil
	}
	slices.Sort(set)
	verb := "config file setting %s was removed"
	if len(set) > 1 {
		verb = "config file settings %s were removed"
	}
	return fmt.Errorf(verb+": pgBackRest runs in the pgbackrestd sidecar, and this %s needs a multigres-operator that deploys that sidecar and does not set it",
		strings.Join(set, ", "), binary)
}
