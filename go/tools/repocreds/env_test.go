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

package repocreds

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnv(t *testing.T) {
	environ := []string{
		"PATH=/usr/bin",
		"AWS_SECRET_ACCESS_KEY=s",
		"AWS_ACCESS_KEY_ID=k",
		"AWS_SESSION_TOKEN=",
		"AWS_REGION=us-east-1",
		"AWS_PROFILE=dev",
		"AWS_ACCESS_KEY=k",
		"AWS_SECRET_KEY=s",
		"AWS_DEFAULT_PROFILE=dev",
		"AWS_WEB_IDENTITY_TOKEN_FILE=/var/run/token",
		"PGBACKREST_REPO1_S3_KEY=k",
		"PGBACKREST_REPO12_S3_KEY_SECRET=s",
		"PGBACKREST_REPO1_S3_TOKEN=t",
		"PGBACKREST_REPO2_CIPHER_PASS=p",
		"PGBACKREST_REPO1_S3_BUCKET=b",
		"PGBACKREST_REPO1_S3_KEY_TYPE=shared",
		"PGPASSFILE=/x",
	}
	assert.Equal(t, []string{
		"AWS_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "AWS_DEFAULT_PROFILE", "AWS_PROFILE", "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY",
		"AWS_SESSION_TOKEN", "AWS_WEB_IDENTITY_TOKEN_FILE",
		"PGBACKREST_REPO12_S3_KEY_SECRET", "PGBACKREST_REPO1_S3_KEY", "PGBACKREST_REPO1_S3_TOKEN", "PGBACKREST_REPO2_CIPHER_PASS",
	}, EnvVars(environ), "a set-but-empty variable counts, and non-secret settings do not")
	assert.Equal(t, []string{
		"PATH=/usr/bin", "AWS_REGION=us-east-1", "PGBACKREST_REPO1_S3_BUCKET=b", "PGBACKREST_REPO1_S3_KEY_TYPE=shared", "PGPASSFILE=/x",
	}, WithoutEnv(environ))
	assert.Empty(t, EnvVars([]string{"PATH=/usr/bin"}))
}
