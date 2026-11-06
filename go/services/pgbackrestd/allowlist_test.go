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
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

func TestAllowlist(t *testing.T) {
	const (
		backupCmd  = pb.Command_COMMAND_BACKUP
		verifyCmd  = pb.Command_COMMAND_VERIFY
		infoCmd    = pb.Command_COMMAND_INFO
		checkCmd   = pb.Command_COMMAND_CHECK
		expireCmd  = pb.Command_COMMAND_EXPIRE
		stanzaCmd  = pb.Command_COMMAND_STANZA_CREATE
		restoreCmd = pb.Command_COMMAND_RESTORE
	)
	accepted := []struct {
		name string
		cmd  pb.Command
		args []string
	}{
		{"backup bare", backupCmd, nil},
		{"backup full", backupCmd, []string{"--type=full"}},
		{"backup diff", backupCmd, []string{"--type=diff"}},
		{"backup incr", backupCmd, []string{"--type=incr"}},
		{"backup annotations repeat", backupCmd, []string{"--annotation=a=b", "--annotation=c=d=e"}},
		{"backup repo bounds", backupCmd, []string{"--repo=1"}},
		{"backup repo max", backupCmd, []string{"--repo=256"}},
		{"backup pg2", backupCmd, []string{"--pg2-host=10.0.0.5", "--pg2-path=/data/pg_data", "--pg2-host-port=8432"}},
		{"backup pg2 ipv6 and name", backupCmd, []string{"--pg2-host=pod-0.svc.cluster.local"}},
		{"verify", verifyCmd, []string{"--set=20260101-120000F", "--repo=2"}},
		{"info", infoCmd, []string{"--set=20260101-120000F_20260102-120000I", "--repo=1", "--output=json", "--log-level-console=off"}},
		{"check", checkCmd, []string{"--repo=3"}},
		{"expire", expireCmd, []string{"--set=20260101-120000F", "--repo=1"}},
		{"expire retention", expireCmd, []string{
			"--repo1-retention-full=3", "--repo1-retention-full-type=time", "--repo1-retention-diff=2",
			"--repo1-retention-history=0", "--repo1-retention-archive=4", "--repo1-retention-archive-type=incr",
		}},
		{"stanza-create", stanzaCmd, nil},
		{"restore", restoreCmd, []string{"--set=latest", "--repo=1", "--type=standby"}},
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			assert.NoError(t, checkArgs(tc.cmd, tc.args))
		})
	}

	rejected := []struct {
		name string
		cmd  pb.Command
		args []string
	}{
		{"unspecified command", pb.Command_COMMAND_UNSPECIFIED, nil},
		{"unknown command", pb.Command(99), nil},
		{"--config", backupCmd, []string{"--config=/etc/x"}},
		{"--stanza", infoCmd, []string{"--stanza=other"}},
		{"--repo1-path", infoCmd, []string{"--repo1-path=/x"}},
		{"--repo1-s3-bucket", backupCmd, []string{"--repo1-s3-bucket=evil"}},
		{"--repo1-s3-key", backupCmd, []string{"--repo1-s3-key=k"}},
		{"--repo1-cipher-pass", backupCmd, []string{"--repo1-cipher-pass=x"}},
		{"--repo1-cipher-type", backupCmd, []string{"--repo1-cipher-type=none"}},
		{"--pg1-path", backupCmd, []string{"--pg1-path=/x"}},
		{"--pg1-host", backupCmd, []string{"--pg1-host=x"}},
		{"--pg2-port", backupCmd, []string{"--pg2-port=5432"}},
		{"--pg2-host-type", backupCmd, []string{"--pg2-host-type=ssh"}},
		{"--pg2-host-ca-file", backupCmd, []string{"--pg2-host-ca-file=/x"}},
		{"--pg2-host-cert-file", backupCmd, []string{"--pg2-host-cert-file=/x"}},
		{"--pg2-host-key-file", backupCmd, []string{"--pg2-host-key-file=/x"}},
		{"--recovery-option", restoreCmd, []string{"--recovery-option=restore_command=x"}},
		{"--force", restoreCmd, []string{"--force=y"}},
		{"--force bare", restoreCmd, []string{"--force"}},
		{"--delta", restoreCmd, []string{"--delta=y"}},
		{"--lock-path", backupCmd, []string{"--lock-path=/x"}},
		{"--log-level-file", backupCmd, []string{"--log-level-file=trace"}},
		{"--type=log", backupCmd, []string{"--type=log"}},
		{"--type=full on restore", restoreCmd, []string{"--type=immediate"}},
		{"--type empty", backupCmd, []string{"--type="}},
		{"--repo=0", backupCmd, []string{"--repo=0"}},
		{"--repo=257", backupCmd, []string{"--repo=257"}},
		{"--repo=+1", backupCmd, []string{"--repo=+1"}},
		{"--repo=1 1", backupCmd, []string{"--repo= 1"}},
		{"--repo=-1", checkCmd, []string{"--repo=-1"}},
		{"--repo huge", checkCmd, []string{"--repo=99999999999999999999"}},
		{"repeated --set", infoCmd, []string{"--set=a", "--set=b"}},
		{"repeated --set same value", infoCmd, []string{"--set=a", "--set=a"}},
		{"repeated --type", backupCmd, []string{"--type=full", "--type=full"}},
		{"--no-type", backupCmd, []string{"--no-type"}},
		{"--no-type=", backupCmd, []string{"--no-type=y"}},
		{"--reset-type", backupCmd, []string{"--reset-type=y"}},
		{"--typex=full", backupCmd, []string{"--typex=full"}},
		{"--ty=full prefix", backupCmd, []string{"--ty=full"}},
		{"--TYPE=full", backupCmd, []string{"--TYPE=full"}},
		{"positional", infoCmd, []string{"info"}},
		{"empty arg", infoCmd, []string{""}},
		{"single dash", infoCmd, []string{"-set=a"}},
		{"bare dashes", infoCmd, []string{"--"}},
		{"bare dashes eq", infoCmd, []string{"--=x"}},
		{"no value", infoCmd, []string{"--output"}},
		{"output text", infoCmd, []string{"--output=text"}},
		{"log level console bad", infoCmd, []string{"--log-level-console=loud"}},
		{"set with slash", infoCmd, []string{"--set=../../x"}},
		{"set empty", verifyCmd, []string{"--set="}},
		{"annotation no equals", backupCmd, []string{"--annotation=abc"}},
		{"annotation empty key", backupCmd, []string{"--annotation==v"}},
		{"annotation newline", backupCmd, []string{"--annotation=a=b\nc"}},
		{"pg2-host with slash", backupCmd, []string{"--pg2-host=a/b"}},
		{"pg2-host with space", backupCmd, []string{"--pg2-host=a b"}},
		{"pg2-path relative", backupCmd, []string{"--pg2-path=data"}},
		{"pg2-path dotdot", backupCmd, []string{"--pg2-path=/data/../etc"}},
		{"pg2-host-port 0", backupCmd, []string{"--pg2-host-port=0"}},
		{"pg2-host-port 65536", backupCmd, []string{"--pg2-host-port=65536"}},
		{"retention repo0", expireCmd, []string{"--repo0-retention-full=3"}},
		{"retention repo257", expireCmd, []string{"--repo257-retention-full=3"}},
		{"retention unknown", expireCmd, []string{"--repo1-retention-bogus=3"}},
		{"retention on backup", backupCmd, []string{"--repo1-retention-full=3"}},
		{"retention bad type", expireCmd, []string{"--repo1-retention-full-type=forever"}},
		{"retention bad archive type", expireCmd, []string{"--repo1-retention-archive-type=log"}},
		{"retention negative", expireCmd, []string{"--repo1-retention-full=-1"}},
		{"retention repeat", expireCmd, []string{"--repo1-retention-full=1", "--repo1-retention-full=2"}},
		{"stanza-create with arg", stanzaCmd, []string{"--repo=1"}},
		{"check with set", checkCmd, []string{"--set=a"}},
		{"verify with type", verifyCmd, []string{"--type=full"}},
		{"expire with type", expireCmd, []string{"--type=full"}},
		{"info with annotation", infoCmd, []string{"--annotation=a=b"}},
		{"restore with pg2-host", restoreCmd, []string{"--pg2-host=x"}},
	}
	for _, tc := range rejected {
		t.Run("reject "+tc.name, func(t *testing.T) {
			err := checkArgs(tc.cmd, tc.args)
			require.Error(t, err)
			assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		})
	}
}

// Each command's table above must not be the only thing keeping the lists
// apart: a flag allowed for one command is rejected for every other.
func TestAllowlist_OptionsAreScopedPerCommand(t *testing.T) {
	assert.NoError(t, checkArgs(pb.Command_COMMAND_BACKUP, []string{"--pg2-host=h"}))
	for _, cmd := range []pb.Command{
		pb.Command_COMMAND_VERIFY, pb.Command_COMMAND_INFO, pb.Command_COMMAND_CHECK,
		pb.Command_COMMAND_EXPIRE, pb.Command_COMMAND_STANZA_CREATE, pb.Command_COMMAND_RESTORE,
	} {
		assert.Error(t, checkArgs(cmd, []string{"--pg2-host=h"}), cmd.String())
	}
}
