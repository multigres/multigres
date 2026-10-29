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
	"fmt"
	"slices"
)

// touchesRepository reports whether a pgBackRest command reads or writes the
// repository, and so may need credentials. stop and start only manage the stop
// file in lock/, and version prints a string. Anything not listed here is
// assumed to touch it, so a new command defaults to the safe-for-correctness
// side (it works) and shows in the env test, which pins this list.
func touchesRepository(command string) bool {
	switch command {
	case cmdStop, cmdStart, cmdVersion:
		return false
	}
	return true
}

// envKind picks the environment class of a child: an S3-command child that
// never touches the repository gets neither the credentials token nor any
// cipher passphrase or static key.
func envKind(kind Kind, command string) Kind {
	if kind == KindS3Command && !touchesRepository(command) {
		return KindLocalCommand
	}
	return kind
}

// envFor builds the complete environment of a child. It never reads
// os.Environ(): PID 1 holds the real credential source (AWS_ROLE_ARN, token
// paths), and nothing from it may reach a child.
func envFor(kind Kind, cfg Config, secrets Secrets) []string {
	env := []string{
		"PATH=/usr/bin:/bin",
		"HOME=" + cfg.StateDir,
		"LANG=C.UTF-8",
		"PGPASSFILE=" + cfg.stateSub(filePgpass),
	}

	switch kind {
	case KindServer:
		// The server's remote children would otherwise read
		// /etc/pgbackrest.conf. It carries no token and no repo secret, so
		// nothing it forks, including a client-configured remote, can obtain
		// credentials.
		env = append(env, "PGBACKREST_CONFIG="+cfg.stateSub(dirConf+"/"+serverConfName))
	case KindS3Command:
		env = append(env, "PGBACKRESTD_CREDS_TOKEN="+secrets.CredsToken)
		for n, gen := range secrets.Repos {
			if pass := secrets.CipherPass[gen]; pass != "" {
				env = append(env, fmt.Sprintf("PGBACKREST_REPO%d_CIPHER_PASS=%s", n, pass))
			}
		}
		for n, key := range secrets.StaticKeys {
			env = append(env,
				fmt.Sprintf("PGBACKREST_REPO%d_S3_KEY=%s", n, key.ID),
				fmt.Sprintf("PGBACKREST_REPO%d_S3_KEY_SECRET=%s", n, key.Secret),
			)
		}
	}

	slices.Sort(env)
	return env
}
