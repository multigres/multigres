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

// Package pgbackrestd implements the pgBackRest sidecar: PID 1 of the
// pgBackRest container, which runs every pgBackRest invocation as a sandboxed
// child and serves the WAL and control APIs on unix sockets.
package pgbackrestd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/multigres/multigres/go/common/sandbox"
)

// Stanza is the one stanza every invocation uses. The API cannot name another.
const Stanza = "multigres"

const (
	LocationS3         = "s3"
	LocationFilesystem = "filesystem"

	CredWebIdentity = "web-identity"
	CredPodIdentity = "pod-identity"
	CredStatic      = "static"
	CredNone        = "none"
	// CredEndpoint is for tests: credentials come from a URL.
	CredEndpoint = "endpoint"

	AnchorMountFlag = "mount"
	AnchorDirFlag   = "dir"
)

// State directory layout, relative to Config.StateDir.
const (
	dirConf  = "conf"
	dirSpool = "spool"
	dirLock  = "lock"
	dirTmp   = "tmp"
	dirCreds = "creds"
	dirState = "state"

	filePgpass = "pgpass"

	// restoreSpoolRel is where restore keeps its async spool. It is a sibling
	// of spool/, not inside it: archive children can write spool/ and could
	// swap a directory there for a link, and restore's Write rule is resolved
	// by name. It also keeps the push acknowledgements out of restore's reach.
	restoreSpoolRel = "spool-restore"

	fileCommandConf = dirConf + "/" + "pgbackrest.conf"
)

// Config is the parsed command line of `pgbackrestd serve`.
type Config struct {
	WALSocket      string
	RestorePIDFile string
	ControlSocket  string
	StateDir       string
	DataVolume     string

	PGData         string
	PGSocketDir    string
	PGPort         int
	PGUser         string
	PGPasswordFile string

	LocationType   string
	S3Bucket       string
	S3Region       string
	S3Endpoint     string
	S3KeyPrefix    string
	S3CredSource   string
	S3CredEndpoint string
	S3CredCAFile   string
	S3StaticKeyDir string
	CipherKeyFile  string
	FSRepoPath     string

	TLSCertDir string
	ServerPort int

	SandboxAnchor string
	// Sandbox is the --sandbox mode; empty means required.
	Sandbox string

	MaxFetches         int
	ArchiveGetQueueMax string
}

var queueMaxRe = regexp.MustCompile(`(?i)^[0-9]+([kmgt]i?b?)?$`)

// stateSub returns an absolute path inside the state directory.
func (c Config) stateSub(rel string) string { return filepath.Join(c.StateDir, rel) }

func (c Config) anchorMode() sandbox.AnchorMode {
	if c.SandboxAnchor == AnchorDirFlag {
		return sandbox.AnchorDir
	}
	return sandbox.AnchorMount
}

func (c Config) sandboxMode() sandbox.Mode {
	if c.Sandbox == "" {
		return sandbox.ModeRequired
	}
	return sandbox.Mode(c.Sandbox)
}

// Validate rejects configurations that cannot work or that would weaken the
// sandbox.
func (c Config) Validate() error {
	required := []struct{ flag, value string }{
		{"--restore-pidfile", c.RestorePIDFile},
		{"--data-volume", c.DataVolume},
		{"--state-dir", c.StateDir},
		{"--wal-socket", c.WALSocket},
		{"--control-socket", c.ControlSocket},
		{"--pgdata", c.PGData},
		{"--pg-socket-dir", c.PGSocketDir},
	}
	for _, r := range required {
		if r.value == "" {
			return fmt.Errorf("%s is required", r.flag)
		}
		if !filepath.IsAbs(r.value) {
			return fmt.Errorf("%s must be an absolute path, got %q", r.flag, r.value)
		}
	}
	// The rules are on the data volume alone, so anything postgres-side the
	// sandboxed children must reach has to lie beneath it.
	for _, r := range required[5:] {
		if !pathWithin(c.DataVolume, r.value) {
			return fmt.Errorf("%s %q must lie within --data-volume %q", r.flag, r.value, c.DataVolume)
		}
	}

	if c.Sandbox != "" {
		if _, err := sandbox.ParseMode(c.Sandbox); err != nil {
			return fmt.Errorf("--sandbox: %w", err)
		}
	}
	switch c.SandboxAnchor {
	case AnchorMountFlag, AnchorDirFlag:
	default:
		return fmt.Errorf("--sandbox-anchor must be %q or %q, got %q", AnchorMountFlag, AnchorDirFlag, c.SandboxAnchor)
	}

	// A data volume that holds the state dir or a socket directory would hand
	// every sandboxed child read access to the credential socket, the token
	// material or the API sockets. The mount anchor check is only a second line.
	for _, p := range []struct{ flag, path string }{
		{"--state-dir", c.StateDir},
		{"--wal-socket", filepath.Dir(c.WALSocket)},
		{"--control-socket", filepath.Dir(c.ControlSocket)},
	} {
		if pathWithin(c.DataVolume, p.path) {
			return fmt.Errorf("--data-volume %q equals or contains %s %q", c.DataVolume, p.flag, p.path)
		}
	}

	switch c.LocationType {
	case LocationS3:
		if c.S3Bucket == "" || c.S3Region == "" {
			return errors.New("--location-type=s3 requires --s3-bucket and --s3-region")
		}
		switch c.S3CredSource {
		case CredWebIdentity, CredPodIdentity, CredNone:
		case CredStatic:
			if c.S3StaticKeyDir == "" {
				return errors.New("--s3-cred-source=static requires --s3-static-key-dir")
			}
		case CredEndpoint:
			if c.S3CredEndpoint == "" {
				return errors.New("--s3-cred-source=endpoint requires --s3-cred-endpoint")
			}
		default:
			return fmt.Errorf("unknown --s3-cred-source %q", c.S3CredSource)
		}
	case LocationFilesystem:
		if err := c.validateFSRepoPath(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("--location-type must be %q or %q, got %q", LocationS3, LocationFilesystem, c.LocationType)
	}

	if c.ServerPort < 1 || c.ServerPort > 65535 {
		return fmt.Errorf("--server-port %d out of range", c.ServerPort)
	}
	if c.MaxFetches < 1 {
		return fmt.Errorf("--max-fetches must be at least 1, got %d", c.MaxFetches)
	}
	if !queueMaxRe.MatchString(c.ArchiveGetQueueMax) {
		return fmt.Errorf("--archive-get-queue-max %q is not a size such as 256MiB", c.ArchiveGetQueueMax)
	}
	return nil
}

// fsRepoDenied are trees a repository path may neither be nor contain nor lie
// within: the repo gets Read or Write for every child.
var fsRepoDenied = []string{"/secrets", "/proc", "/sys", "/dev", "/etc", "/usr", "/lib", "/bin", "/run"}

func (c Config) validateFSRepoPath() error {
	p := c.FSRepoPath
	if p == "" || !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return fmt.Errorf("--fs-repo-path must be a clean absolute path, got %q", p)
	}
	related := func(a, b string) bool { return pathWithin(a, b) || pathWithin(b, a) }
	for _, other := range append([]string{c.StateDir, c.DataVolume, filepath.Dir(c.WALSocket), filepath.Dir(c.ControlSocket)}, fsRepoDenied...) {
		if related(p, other) {
			return fmt.Errorf("--fs-repo-path %q overlaps %q", p, other)
		}
	}
	return nil
}

// pathWithin reports whether target equals parent or lies beneath it.
func pathWithin(parent, target string) bool {
	rel, err := filepath.Rel(filepath.Clean(parent), filepath.Clean(target))
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

// prepareState creates the private state layout. Clearing tmp/ is a startup
// duty (runStartupDuties), not done here, so it has one owner.
func prepareState(cfg Config) error {
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	for _, d := range []string{dirConf, dirSpool, dirLock, dirTmp, dirCreds, dirState} {
		p := cfg.stateSub(d)
		if err := os.Mkdir(p, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create %s: %w", p, err)
		}
		if err := os.Chmod(p, 0o700); err != nil {
			return fmt.Errorf("chmod %s: %w", p, err)
		}
	}
	rs := cfg.stateSub(restoreSpoolRel)
	if err := os.Mkdir(rs, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create %s: %w", rs, err)
	}
	if err := writeMinimalConf(cfg); err != nil {
		return err
	}

	// A sandbox rule on a missing path is an error, so pgpass must exist
	// before the first child starts. Content is written later by rename.
	f, err := os.OpenFile(cfg.stateSub(filePgpass), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create pgpass: %w", err)
	}
	return f.Close()
}

// writeMinimalConf gives commands that run before the first Configure (stop
// and start at boot) a config to read: pgBackRest treats an explicit missing
// --config as fatal. An existing file is a rendered config from an earlier
// Configure and is kept.
func writeMinimalConf(cfg Config) error {
	path := cfg.stateSub(fileCommandConf)
	if _, err := os.Lstat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	body := "[global]\nlock-path=" + cfg.stateSub(dirLock) + "\nlog-level-file=off\n"
	tmp, err := os.CreateTemp(cfg.stateSub(dirConf), ".pgbackrest.conf.*")
	if err != nil {
		return fmt.Errorf("create minimal conf: %w", err)
	}
	if _, err := tmp.WriteString(body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return fmt.Errorf("write minimal conf: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("close minimal conf: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("install minimal conf: %w", err)
	}
	return nil
}
