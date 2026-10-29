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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"text/template"

	"github.com/multigres/multigres/config"
	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/fileutil"
)

const (
	fileClientConf = dirConf + "/pgbackrest.conf"
	fileServerConf = dirConf + "/" + serverConfName
	dirLog         = "log"
)

// replaceOnce edits a rendered template. The target must be present exactly
// once: a template change that removes or duplicates it fails rendering
// instead of silently dropping the edit.
func replaceOnce(body, from, to string) (string, error) {
	if strings.Count(body, from) != 1 {
		return "", fmt.Errorf("config template no longer contains exactly one %q", from)
	}
	return strings.Replace(body, from, to, 1), nil
}

// location is the backup location this sidecar was started with.
func (c Config) location() *clustermetadatapb.BackupLocation {
	if c.LocationType == LocationS3 {
		return &clustermetadatapb.BackupLocation{Location: &clustermetadatapb.BackupLocation_S3{
			S3: &clustermetadatapb.S3Backup{
				Bucket:    c.S3Bucket,
				Region:    c.S3Region,
				Endpoint:  c.S3Endpoint,
				KeyPrefix: c.S3KeyPrefix,
			},
		}}
	}
	return &clustermetadatapb.BackupLocation{Location: &clustermetadatapb.BackupLocation_Filesystem{
		Filesystem: &clustermetadatapb.FilesystemBackup{Path: c.FSRepoPath},
	}}
}

// s3KeyType maps the credential source to pgBackRest's s3-key-type. Sources
// that need the credential helper go through process, so the secret reaches
// only the S3-command child that asks for it.
func (c Config) s3KeyType() string {
	switch c.S3CredSource {
	case CredStatic:
		return "shared"
	case CredWebIdentity, CredPodIdentity, CredEndpoint:
		return "process"
	default:
		return "auto"
	}
}

// renderClientConf writes conf/pgbackrest.conf for the repositories in st.
// The file holds no secret: cipher passphrases and static keys reach children
// through their environment (envFor), and the credential helper is named here
// by command only.
func (s *Server) renderClientConf(st persistedState, snap RepoSnapshot) error {
	body, err := s.clientConfBody(st, snap)
	if err != nil {
		return err
	}
	return fileutil.AtomicWriteFile(s.cfg.stateSub(fileClientConf), body, 0o600)
}

func (s *Server) clientConfBody(st persistedState, snap RepoSnapshot) ([]byte, error) {
	bc, err := backup.NewConfig(s.cfg.location())
	if err != nil {
		return nil, err
	}
	repoConfig := map[string]string{}
	retention := map[string]string{}
	for _, r := range st.Repos {
		storage, err := bc.PgBackRestConfig(int(r.RepoNumber), int64(r.Generation), Stanza)
		if err != nil {
			return nil, fmt.Errorf("repository %d: %w", r.RepoNumber, err)
		}
		maps.Copy(repoConfig, storage)
		prefix := fmt.Sprintf("repo%d-", r.RepoNumber)
		// Verification is never turned off. A custom endpoint is verified
		// against the CA the sidecar was given for it, or the system roots.
		delete(repoConfig, prefix+"s3-verify-tls")
		if s.cfg.LocationType == LocationS3 && s.cfg.S3Endpoint != "" && s.cfg.S3CredCAFile != "" {
			repoConfig[prefix+"storage-ca-file"] = s.cfg.S3CredCAFile
		}
		if s.cfg.LocationType == LocationS3 {
			repoConfig[prefix+"s3-key-type"] = s.cfg.s3KeyType()
			if s.cfg.s3KeyType() == "process" {
				repoConfig[prefix+"s3-process-cmd"] = processCmdValue(prefix+"s3-process-cmd", s.credsCommand())
			}
		}
		if snap.CipherPass[r.Generation] != "" {
			repoConfig[prefix+"cipher-type"] = backup.CipherType
		}
		retention[prefix+"retention-diff"] = backup.RetentionDifferential
		retention[prefix+"retention-full"] = backup.RetentionFull
		retention[prefix+"retention-full-type"] = backup.RetentionFullType
		retention[prefix+"retention-history"] = backup.RetentionHistory
	}

	tmpl, err := template.New("pgbackrest").Parse(config.PgBackRestConfigTmpl)
	if err != nil {
		return nil, fmt.Errorf("parse pgbackrest config template: %w", err)
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, map[string]any{
		"LogPath":         s.cfg.stateSub(dirLog),
		"SpoolPath":       s.cfg.stateSub(dirSpool),
		"LockPath":        s.cfg.stateSub(dirLock),
		"RetentionConfig": retention,
		"RepoConfig":      repoConfig,
		"RepoCredentials": map[string]string{},
		"Pg1SocketPath":   s.cfg.PGSocketDir,
		"Pg1Port":         s.cfg.PGPort,
		"Pg1Path":         s.cfg.PGData,
		"Pg1User":         s.cfg.PGUser,
		"ProcessLimits":   backup.CurrentProcessLimits(),
	})
	if err != nil {
		return nil, fmt.Errorf("render pgbackrest config template: %w", err)
	}
	// Nothing logs to a file: stderr is what the API streams back.
	out, err := replaceOnce(buf.String(), "log-level-file=detail", "log-level-file=off\nlog-level-stderr=info")
	if err != nil {
		return nil, err
	}
	// The template only mentions the queue limit in a comment.
	out, err = replaceOnce(out, "archive-async=y", "archive-async=y\narchive-get-queue-max="+archiveGetQueueMax(s.cfg.ArchiveGetQueueMax, st.WALSegmentSizeBytes))
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}

// archiveGetQueueMax is the prefetch queue limit to render: the configured
// limit, raised to two segments when that is more. pgBackRest always keeps two
// segments queued, so a smaller limit would be exceeded anyway, and the
// operator sizes spool/ from the same rule. Only the get queue is ever sized
// this way: an archive-push queue limit would make pgBackRest drop WAL.
func archiveGetQueueMax(configured string, segmentBytes uint32) string {
	if 2*uint64(segmentBytes) > queueMaxBytes(configured) {
		return strconv.FormatUint(2*uint64(segmentBytes), 10)
	}
	return configured
}

// queueMaxBytes parses a size already checked against queueMaxRe: digits and
// an optional k, m, g or t unit (1024-based, with optional i and b).
func queueMaxBytes(v string) uint64 {
	v = strings.ToLower(v)
	digits := strings.TrimRight(v, "kmgtib")
	n, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0
	}
	switch strings.TrimRight(strings.TrimPrefix(v, digits), "ib") {
	case "k":
		n <<= 10
	case "m":
		n <<= 20
	case "g":
		n <<= 30
	case "t":
		n <<= 40
	}
	return n
}

// renderServerConf writes conf/pgbackrest-server.conf. It does not depend on
// the repositories, so it exists from startup on.
func (s *Server) renderServerConf() error {
	body, err := s.serverConfBody()
	if err != nil {
		return err
	}
	return fileutil.AtomicWriteFile(s.cfg.stateSub(fileServerConf), body, 0o600)
}

func (s *Server) serverConfBody() ([]byte, error) {
	tmpl, err := template.New("pgbackrest-server").Parse(config.PgBackRestServerConfigTmpl)
	if err != nil {
		return nil, fmt.Errorf("parse pgbackrest server config template: %w", err)
	}
	cert, key, ca := constants.PgBackRestCertFiles(s.cfg.TLSCertDir)
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, map[string]any{
		"LogPath":        s.cfg.stateSub(dirLog),
		"LockPath":       s.cfg.stateSub(dirLock),
		"ServerCertFile": cert,
		"ServerKeyFile":  key,
		"ServerCAFile":   ca,
		"ServerPort":     s.cfg.ServerPort,
		"Pg1SocketPath":  s.cfg.PGSocketDir,
		"Pg1Port":        s.cfg.PGPort,
		"Pg1Path":        s.cfg.PGData,
		"Pg1User":        s.cfg.PGUser,
	})
	if err != nil {
		return nil, fmt.Errorf("render pgbackrest server config template: %w", err)
	}
	// tls-server-auth is <client certificate CN>=<stanzas>: only a client whose
	// certificate has CN "pgbackrest" may use the server, and only for stanza
	// "multigres". The template's wildcard admits every stanza.
	out, err := replaceOnce(buf.String(), "tls-server-auth=pgbackrest=*", "tls-server-auth=pgbackrest=multigres")
	if err != nil {
		return nil, err
	}
	// The state dir has no log/ directory and the sandbox grants no write on
	// one, so the default file log level only produces an "unable to open log
	// file" warning on every start. stderr is what the supervisor captures.
	out, err = replaceOnce(out, "log-path="+s.cfg.stateSub(dirLog), "log-level-file=off")
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}

// pgpassField escapes the two characters the pgpass format treats specially.
func pgpassField(v string) string {
	return strings.NewReplacer(`\`, `\\`, ":", `\:`).Replace(v)
}

// writePgpass renders <state>/pgpass from the password file. The file the
// children read always exists (a sandbox rule on a missing path is an error),
// so an absent --pg-password-file leaves it as startup created it.
func (s *Server) writePgpass() error {
	line, ok, err := s.pgpassBody()
	if err != nil || !ok {
		return err
	}
	return fileutil.AtomicWriteFile(s.cfg.stateSub(filePgpass), line, 0o600)
}

// pgpassBody is the content writePgpass would write; ok is false when there is
// no password file and so nothing to write.
func (s *Server) pgpassBody() (body []byte, ok bool, err error) {
	if s.cfg.PGPasswordFile == "" {
		return nil, false, nil
	}
	raw, err := os.ReadFile(s.cfg.PGPasswordFile)
	if err != nil {
		return nil, false, fmt.Errorf("read postgres password file: %w", err)
	}
	pw := strings.TrimRight(string(raw), "\r\n")
	if pw == "" {
		return nil, false, errors.New("postgres password file is empty")
	}
	return fmt.Appendf(nil, "*:*:*:%s:%s\n", pgpassField(s.cfg.PGUser), pgpassField(pw)), true, nil
}

// fileHolds reports whether path already contains exactly body.
func fileHolds(path string, body []byte) bool {
	cur, err := os.ReadFile(path)
	return err == nil && bytes.Equal(cur, body)
}

// locationFingerprint identifies where this sidecar's repositories live, so a
// caller can check that it agrees with the topology without the API ever
// naming the location. Struct field order fixes the JSON.
func (c Config) locationFingerprint() string {
	b, _ := json.Marshal(struct {
		LocationType string `json:"location-type"`
		S3Bucket     string `json:"s3-bucket"`
		S3Region     string `json:"s3-region"`
		S3Endpoint   string `json:"s3-endpoint"`
		S3KeyPrefix  string `json:"s3-key-prefix"`
		FSRepoPath   string `json:"fs-repo-path"`
	}{c.LocationType, c.S3Bucket, c.S3Region, c.S3Endpoint, c.S3KeyPrefix, c.FSRepoPath})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// defaultStateDir is what `pgbackrestd creds` assumes without --state-dir, and
// what the pod layout uses.
const defaultStateDir = "/var/lib/pgbackrestd"

// credsCommand is the argv of the command pgBackRest runs to obtain S3
// credentials. The helper finds the credential socket under the state dir, so a
// sidecar that was given another state dir (the local provisioner's layout)
// must pass it on; the pod layout keeps the default and renders no flag.
func (s *Server) credsCommand() []string {
	argv := []string{s.self, "creds"}
	if s.cfg.StateDir != defaultStateDir {
		argv = append(argv, "--state-dir", s.cfg.StateDir)
	}
	return argv
}

// processCmdValue renders a process command for a repository option. The
// option is a list: pgBackRest runs the first value as the program and each
// further occurrence is one argument, so "prog creds" in a single value names
// a program called "prog creds" and fails with ExecuteError. The conf template
// writes one key=value line per map entry, so the further occurrences are
// carried as extra lines inside the value.
func processCmdValue(key string, argv []string) string {
	var b strings.Builder
	for i, a := range argv {
		if i > 0 {
			b.WriteString("\n" + key + "=")
		}
		b.WriteString(a)
	}
	return b.String()
}
