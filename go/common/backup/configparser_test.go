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

package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAndUpdateCredentials(t *testing.T) {
	input := `[global]
log-path=/var/log/pgbackrest
repo1-s3-key=OLDKEY123
repo1-s3-key-secret=OLDSECRET456
repo1-s3-key-type=shared
repo1-s3-token=OLDTOKEN789

[multigres]
repo1-type=s3
repo1-s3-bucket=test-bucket
`

	newCreds := map[string]string{
		"repo1-s3-key":        "NEWKEY999",
		"repo1-s3-key-secret": "NEWSECRET888",
		"repo1-s3-token":      "NEWTOKEN777",
		"repo1-s3-key-type":   "shared",
	}

	output, err := UpdateCredentialsInConfig(input, newCreds)
	require.NoError(t, err)

	// Verify old values are replaced
	require.NotContains(t, output, "OLDKEY123")
	require.NotContains(t, output, "OLDSECRET456")
	require.NotContains(t, output, "OLDTOKEN789")

	// Verify new values are present
	require.Contains(t, output, "repo1-s3-key=NEWKEY999")
	require.Contains(t, output, "repo1-s3-key-secret=NEWSECRET888")
	require.Contains(t, output, "repo1-s3-token=NEWTOKEN777")

	// Verify other config is preserved
	require.Contains(t, output, "log-path=/var/log/pgbackrest")
	require.Contains(t, output, "repo1-s3-bucket=test-bucket")
}

func TestUpdateCredentialsPreservesFormatting(t *testing.T) {
	input := `[global]
log-path=/var/log/pgbackrest

# Comment about credentials
repo1-s3-key=OLDKEY
repo1-s3-key-secret=OLDSECRET

[multigres]
repo1-type=s3
`

	newCreds := map[string]string{
		"repo1-s3-key":        "NEWKEY",
		"repo1-s3-key-secret": "NEWSECRET",
	}

	output, err := UpdateCredentialsInConfig(input, newCreds)
	require.NoError(t, err)

	// Verify blank lines preserved
	require.Contains(t, output, "\n\n")

	// Verify comments preserved
	require.Contains(t, output, "# Comment about credentials")
}

func TestUpdateCredentialsWithoutToken(t *testing.T) {
	input := `[global]
repo1-s3-key=OLDKEY
repo1-s3-key-secret=OLDSECRET
repo1-s3-key-type=shared
`

	newCreds := map[string]string{
		"repo1-s3-key":        "NEWKEY",
		"repo1-s3-key-secret": "NEWSECRET",
		"repo1-s3-key-type":   "shared",
	}

	output, err := UpdateCredentialsInConfig(input, newCreds)
	require.NoError(t, err)

	// Should not contain old token or add new one
	require.NotContains(t, output, "repo1-s3-token")
	require.Contains(t, output, "repo1-s3-key=NEWKEY")
}
