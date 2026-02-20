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
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/ini.v1"
)

func TestWriteServerConfig(t *testing.T) {
	tmpDir := t.TempDir()

	opts := ServerConfigOpts{
		PoolerDir:     tmpDir,
		CertDir:       "/certs",
		Port:          8443,
		Pg1Port:       5432,
		Pg1SocketPath: "/tmp/socket",
		Pg1Path:       "/var/lib/postgresql/data",
	}

	configPath, err := WriteServerConfig(opts)
	require.NoError(t, err)

	assert.Equal(t, filepath.Join(tmpDir, "pgbackrest", "pgbackrest-server.conf"), configPath)

	cfg, err := ini.Load(configPath)
	require.NoError(t, err)

	global := cfg.Section("global")
	assert.Equal(t, filepath.Join(tmpDir, "pgbackrest", "log"), global.Key("log-path").String())
	assert.Equal(t, "/certs/pgbackrest.crt", global.Key("tls-server-cert-file").String())
	assert.Equal(t, "/certs/pgbackrest.key", global.Key("tls-server-key-file").String())
	assert.Equal(t, "/certs/ca.crt", global.Key("tls-server-ca-file").String())
	assert.Equal(t, "0.0.0.0", global.Key("tls-server-address").String())
	assert.Equal(t, "8443", global.Key("tls-server-port").String())
	assert.Equal(t, "pgbackrest=*", global.Key("tls-server-auth").String())

	stanza := cfg.Section("multigres")
	assert.Equal(t, "/tmp/socket", stanza.Key("pg1-socket-path").String())
	assert.Equal(t, "5432", stanza.Key("pg1-port").String())
	assert.Equal(t, "/var/lib/postgresql/data", stanza.Key("pg1-path").String())
	assert.Equal(t, "postgres", stanza.Key("pg1-user").String())
	assert.Equal(t, "postgres", stanza.Key("pg1-database").String())

	// Must NOT contain client-only settings
	for _, key := range global.Keys() {
		assert.False(t, strings.HasPrefix(key.Name(), "repo1-"), "server config should not contain repo settings")
	}
	assert.False(t, global.HasKey("spool-path"), "server config should not contain spool-path")
	assert.False(t, global.HasKey("lock-path"), "server config should not contain lock-path")
}

func TestWriteServerConfig_CreatesLogDir(t *testing.T) {
	tmpDir := t.TempDir()

	opts := ServerConfigOpts{
		PoolerDir:     tmpDir,
		CertDir:       "/certs",
		Port:          8443,
		Pg1Port:       5432,
		Pg1SocketPath: "/tmp/socket",
		Pg1Path:       "/data",
	}

	_, err := WriteServerConfig(opts)
	require.NoError(t, err)

	logDir := filepath.Join(tmpDir, "pgbackrest", "log")
	assert.DirExists(t, logDir)
}
