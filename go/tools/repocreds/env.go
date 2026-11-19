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

// Package repocreds names the environment variables that give a process a
// credential for a backup repository, so that every process other than the
// one that talks to the repository can be started, and checked, without them.
package repocreds

import (
	"regexp"
	"slices"
	"strings"
)

// envNames are the environment variables that hold, or point at, a
// credential for the backup repository. Only pgbackrestd and the pgBackRest
// commands it starts may have one: postgres, pgctld and multipooler run code
// (extensions, archive and restore commands) that must not be able to reach
// the repository on its own.
var envNames = []string{
	// Static keys, as exported by `aws sso login` and similar.
	"AWS_ACCESS_KEY_ID",
	"AWS_SECRET_ACCESS_KEY",
	"AWS_SESSION_TOKEN",
	// The same keys under the names the AWS SDKs also read.
	"AWS_ACCESS_KEY",
	"AWS_SECRET_KEY",
	// Files and profiles the AWS credential chain reads keys from.
	"AWS_SHARED_CREDENTIALS_FILE",
	"AWS_CONFIG_FILE",
	"AWS_PROFILE",
	"AWS_DEFAULT_PROFILE",
	// Identity sources that mint keys on request.
	"AWS_WEB_IDENTITY_TOKEN_FILE",
	"AWS_ROLE_ARN",
	"AWS_CONTAINER_CREDENTIALS_FULL_URI",
	"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
	"AWS_CONTAINER_AUTHORIZATION_TOKEN",
	"AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
}

// pgBackRestSecretEnv matches pgBackRest's own option variables that carry a
// repository secret: the S3 key, key secret and token, and the cipher
// passphrase, for any repository number.
var pgBackRestSecretEnv = regexp.MustCompile(`^PGBACKREST_REPO[0-9]*_(S3_KEY|S3_KEY_SECRET|S3_TOKEN|CIPHER_PASS)$`)

// IsEnvVar reports whether an environment variable of this name
// would give its process a credential for the backup repository.
func IsEnvVar(name string) bool {
	return slices.Contains(envNames, name) || pgBackRestSecretEnv.MatchString(name)
}

// EnvVars returns the names in environ ("NAME=value" entries, as
// os.Environ returns them) that IsEnvVar accepts, sorted and without
// duplicates. A variable set to the empty string counts: it is still a
// misconfiguration, and some readers treat empty and unset differently.
func EnvVars(environ []string) []string {
	var found []string
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if IsEnvVar(name) && !slices.Contains(found, name) {
			found = append(found, name)
		}
	}
	slices.Sort(found)
	return found
}

// WithoutEnv returns environ without the entries IsEnvVar
// accepts. Use it for the environment of every child that is not pgbackrestd.
func WithoutEnv(environ []string) []string {
	kept := make([]string, 0, len(environ))
	for _, kv := range environ {
		if name, _, _ := strings.Cut(kv, "="); !IsEnvVar(name) {
			kept = append(kept, kv)
		}
	}
	return kept
}
