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
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"connectrpc.com/connect"

	"github.com/multigres/multigres/go/common/backup"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// argRule validates the value of one allowed option.
type argRule struct {
	// repeatable options may appear more than once; every other option once.
	repeatable bool
	valid      func(value string) error
}

var (
	digitsRe     = regexp.MustCompile(`^[0-9]+$`)
	setLabelRe   = regexp.MustCompile(`^[0-9A-Za-z_-]{1,128}$`)
	hostRe       = regexp.MustCompile(`^[0-9A-Za-z._:-]{1,253}$`)
	retentionRe  = regexp.MustCompile(`^repo([0-9]{1,3})-retention-(full|full-type|diff|history|archive|archive-type)$`)
	logLevelEnum = []string{"off", "error", "warn", "info", "detail", "debug", "trace"}
)

func oneOf(values ...string) func(string) error {
	return func(v string) error {
		if slices.Contains(values, v) {
			return nil
		}
		return fmt.Errorf("must be one of %s", strings.Join(values, ", "))
	}
}

func intRange(lo, hi int) func(string) error {
	return func(v string) error {
		if !digitsRe.MatchString(v) || len(v) > 9 {
			return fmt.Errorf("must be a decimal integer from %d to %d", lo, hi)
		}
		n, _ := strconv.Atoi(v)
		if n < lo || n > hi {
			return fmt.Errorf("must be from %d to %d", lo, hi)
		}
		return nil
	}
}

func validAnnotation(v string) error {
	k, _, ok := strings.Cut(v, "=")
	if !ok || k == "" {
		return errors.New("must be key=value")
	}
	if strings.ContainsAny(v, "\x00\n\r") {
		return errors.New("must not contain control characters")
	}
	return nil
}

func validSetLabel(v string) error {
	if !setLabelRe.MatchString(v) {
		return errors.New("must be a backup label")
	}
	return nil
}

func validHost(v string) error {
	if !hostRe.MatchString(v) {
		return errors.New("must be a host name or address")
	}
	return nil
}

func validAbsPath(v string) error {
	if !filepath.IsAbs(v) || filepath.Clean(v) != v || strings.ContainsAny(v, "\x00\n\r") {
		return errors.New("must be a clean absolute path")
	}
	return nil
}

var (
	ruleRepo = argRule{valid: intRange(1, maxRepoNumber)}
	ruleSet  = argRule{valid: validSetLabel}
)

// allowed maps each command to the options a caller may pass. Everything not
// listed is rejected, including every option that selects a location, a
// credential, a config file or a stanza.
var allowed = map[pb.Command]map[string]argRule{
	pb.Command_COMMAND_BACKUP: {
		"type":          {valid: oneOf("full", "diff", "incr")},
		"annotation":    {repeatable: true, valid: validAnnotation},
		"repo":          ruleRepo,
		"pg2-host":      {valid: validHost},
		"pg2-path":      {valid: validAbsPath},
		"pg2-host-port": {valid: intRange(1, 65535)},
	},
	pb.Command_COMMAND_VERIFY: {"set": ruleSet, "repo": ruleRepo},
	pb.Command_COMMAND_INFO: {
		"set":               ruleSet,
		"repo":              ruleRepo,
		"output":            {valid: oneOf("json")},
		"log-level-console": {valid: oneOf(logLevelEnum...)},
	},
	pb.Command_COMMAND_CHECK:         {"repo": ruleRepo},
	pb.Command_COMMAND_EXPIRE:        {"set": ruleSet, "repo": ruleRepo},
	pb.Command_COMMAND_STANZA_CREATE: {},
	// repo-ls always lists one directory of this stanza's archive, which the
	// sidecar names (see repoLsPath), and the output format is required.
	pb.Command_COMMAND_REPO_LS: {
		"output":            {valid: oneOf("json")},
		"log-level-console": {valid: oneOf(logLevelEnum...)},
	},
	// These two take no options: the sidecar decides every step.
	pb.Command_COMMAND_STANZA_DELETE:       {},
	pb.Command_COMMAND_ARCHIVE_SPOOL_RESET: {},
	// --type is optional for restore: pgBackRest's default type (recover to
	// the end of the archive) is valid, and multipooler always sends standby.
	pb.Command_COMMAND_RESTORE: {
		"set":  ruleSet,
		"repo": ruleRepo,
		"type": {valid: oneOf("standby")},
	},
}

// requiredArgs lists the options a command cannot run without.
var requiredArgs = map[pb.Command][]string{
	pb.Command_COMMAND_REPO_LS: {"output"},
}

// retentionRule validates the per-repository retention options EXPIRE allows.
func retentionRule(name string) (argRule, bool) {
	m := retentionRe.FindStringSubmatch(name)
	if m == nil {
		return argRule{}, false
	}
	if n, _ := strconv.Atoi(m[1]); n < 1 || n > maxRepoNumber {
		return argRule{}, false
	}
	switch m[2] {
	case "full-type":
		return argRule{valid: oneOf("count", "time")}, true
	case "archive-type":
		return argRule{valid: oneOf("full", "diff", "incr")}, true
	default:
		return argRule{valid: intRange(0, 9999999)}, true
	}
}

// pgbackrestCommand names the pgBackRest command a request command runs.
func pgbackrestCommand(cmd pb.Command) (string, bool) {
	switch cmd {
	case pb.Command_COMMAND_BACKUP:
		return cmdBackup, true
	case pb.Command_COMMAND_VERIFY:
		return cmdVerify, true
	case pb.Command_COMMAND_INFO:
		return cmdInfo, true
	case pb.Command_COMMAND_CHECK:
		return cmdCheck, true
	case pb.Command_COMMAND_EXPIRE:
		return cmdExpire, true
	case pb.Command_COMMAND_STANZA_CREATE:
		return cmdStanzaCreate, true
	case pb.Command_COMMAND_RESTORE:
		return cmdRestore, true
	case pb.Command_COMMAND_REPO_LS:
		return cmdRepoLs, true
	}
	// COMMAND_STANZA_DELETE and COMMAND_ARCHIVE_SPOOL_RESET are sequences of
	// children (runExclusive), not one plain command.
	return "", false
}

// checkArgs accepts args for cmd only if every one is "--name=value" with a
// name on cmd's list and a value that passes its rule. Names are compared
// exactly: pgBackRest accepts unambiguous prefixes and the --no- and --reset-
// forms, and any of those would slip past a prefix or substring check.
func checkArgs(cmd pb.Command, args []string) error {
	rules, known := allowed[cmd]
	if !known {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown command %v", cmd))
	}
	seen := make(map[string]bool, len(args))
	for _, arg := range args {
		rest, ok := strings.CutPrefix(arg, "--")
		if !ok {
			return rejectArg(arg, "not an option")
		}
		name, value, ok := strings.Cut(rest, "=")
		if !ok {
			return rejectArg(arg, "options must be --name=value")
		}
		rule, ok := rules[name]
		if !ok && cmd == pb.Command_COMMAND_EXPIRE {
			rule, ok = retentionRule(name)
		}
		if !ok {
			return rejectArg(arg, "option not allowed for this command")
		}
		if seen[name] && !rule.repeatable {
			return rejectArg(arg, "option repeated")
		}
		seen[name] = true
		if err := rule.valid(value); err != nil {
			return rejectArg(arg, err.Error())
		}
	}
	for _, name := range requiredArgs[cmd] {
		if !seen[name] {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("option --%s is required for %v", name, cmd))
		}
	}
	return nil
}

// checkRequest validates a whole Run request: its options, and the structured
// archive fields that only COMMAND_REPO_LS takes. A caller never supplies a
// repository path; repoLsPath builds it from these fields.
func checkRequest(req *pb.RunRequest) error {
	if err := checkArgs(req.GetCommand(), req.GetArgs()); err != nil {
		return err
	}
	id, dir := req.GetArchiveId(), req.GetWalDir()
	if req.GetCommand() != pb.Command_COMMAND_REPO_LS {
		if id != "" || dir != "" {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("archive_id and wal_dir are only for %v", pb.Command_COMMAND_REPO_LS))
		}
		return nil
	}
	if id != "" && !backup.ValidArchiveID(id) {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("archive_id must be <pg major>-<history id>"))
	}
	if dir != "" && !backup.ValidWALDir(dir) {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("wal_dir must be 16 uppercase hexadecimal digits"))
	}
	if dir != "" && id == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("wal_dir needs archive_id"))
	}
	return nil
}

// repoLsPath is the repository directory COMMAND_REPO_LS lists, relative to the
// repository root: this stanza's archive, an archive id beneath it, or a WAL
// directory beneath that. Only validated fields reach it, so it cannot leave
// the stanza's archive (repo-ls accepts "../" and lists the repository's parent).
func repoLsPath(req *pb.RunRequest) string {
	path := "archive/" + Stanza
	if id := req.GetArchiveId(); id != "" {
		path += "/" + id
		if dir := req.GetWalDir(); dir != "" {
			path += "/" + dir
		}
	}
	return path
}

func rejectArg(arg, why string) error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("argument %q rejected: %s", arg, why))
}
