// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/parser/ast"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/tools/fileutil"
)

type walSetting struct{ name, stable string }

// Match only commands that previous releases wrote for this pooler. Never
// install a missing restore_command: consensus clears it on cohort members.
var walSettings = [...]walSetting{
	{"archive_command", backup.ArchiveWALCommand},
	{"restore_command", backup.RestoreWALCommand},
}

// Extract a candidate former root, then reconstruct the entire known command.
// Do not migrate a shell pipeline, extra options, or mismatched PID/config roots.
var oldWALRoot = regexp.MustCompile(`--config='*(/[a-zA-Z0-9_./ -]+)/pgbackrest/pgbackrest\.conf`)

func (s walSetting) matches(value, dir string) bool {
	roots := []string{dir}
	if match := oldWALRoot.FindStringSubmatch(value); match != nil && match[1] != dir {
		// Old direct commands did not shell-quote paths. Do not mistake custom
		// command-line arguments for part of a relocated directory name.
		quotedRoot := strings.HasPrefix(strings.TrimPrefix(value, "'"), "pgctld restore-wrapper ")
		if quotedRoot || !strings.Contains(match[1], " ") {
			roots = append(roots, match[1])
		}
	}
	for _, root := range roots {
		archive, restore := backup.LegacyWALCommands(backup.ClientConfigPath(root), root)
		forms := []string{archive}
		if s.name == "restore_command" {
			forms = []string{restore, fmt.Sprintf(`pgbackrest --config=%s --stanza=multigres archive-get %%f "%%p"`, backup.ClientConfigPath(root))}
		}
		for _, form := range forms {
			form, err := ast.QuoteConfValue(form)
			if err != nil {
				continue
			}
			if value == form {
				return true
			}
		}
	}
	return false
}

// Force probes on persistent writes, never on ordinary health polls. Bound both
// so an unresponsive pgctld cannot stall backup health or a pending Start.
const (
	walWriteStatusTimeout = 12 * time.Second
	walReadinessTimeout   = 2 * time.Second
)

func (pm *MultipoolerManager) walCommandsEnabled() bool {
	return pm.config != nil && pm.config.WALCommandWrappers && !pm.IsUnmanaged()
}

func (pm *MultipoolerManager) supportsWALWrappers(ctx context.Context) bool {
	return pm.freshWALWrapperStatus(ctx) != nil
}

// A fresh affirmative status for the same pooler is necessary for each new
// write. An older pgctld, timeout, ambiguous answer or root mismatch cannot
// authorize a write, regardless of what an earlier request observed.
func (pm *MultipoolerManager) freshWALWrapperStatus(ctx context.Context) *pgctldpb.StatusResponse {
	dir := pm.record.PoolerDir()
	if !pm.walCommandsEnabled() || pm.pgctldClient == nil || !filepath.IsAbs(dir) {
		return nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, walWriteStatusTimeout)
	defer cancel()
	resp, err := pm.pgctldClient.Status(probeCtx, &pgctldpb.StatusRequest{RecheckWalWrappers: true})
	if err == nil {
		err = probeCtx.Err()
	}
	if err != nil {
		pm.logger.WarnContext(ctx, "cannot check WAL wrapper capability; keeping legacy WAL commands", "error", err)
		return nil
	}
	if resp.GetWalWrapperProbeUnknown() || resp.GetWalWrapperVersion() < backup.WALWrapperVersion || !sameWALPath(resp.GetPoolerDir(), dir) {
		pm.logger.WarnContext(ctx, "pgctld did not verify WAL wrappers for this pooler; keeping legacy WAL commands")
		return nil
	}
	return resp
}

func sameWALPath(reported, local string) bool {
	return filepath.IsAbs(reported) && filepath.IsAbs(local) && filepath.Clean(reported) == filepath.Clean(local)
}

// Only called on the monitor's action-locked Start path, never on attach.
// pg_isready alone cannot prove the postmaster stopped: any postmaster.pid,
// even stale, malformed or a dangling symlink, vetoes conversion.
func (pm *MultipoolerManager) migrateStoppedWALCommands(ctx context.Context) error {
	data, dir := postgresDataDir(), pm.record.PoolerDir()
	if !pm.walCommandsEnabled() || !filepath.IsAbs(data) {
		return nil
	}
	pidFile := filepath.Join(data, "postmaster.pid")
	stopped := func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		_, err := os.Lstat(pidFile)
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, err
	}
	if absent, err := stopped(); !absent {
		return err
	}
	status := pm.freshWALWrapperStatus(ctx)
	if status == nil || status.GetStatus() != pgctldpb.ServerStatus_STOPPED || status.GetReady() || status.GetPid() != 0 || !sameWALPath(status.GetDataDir(), data) {
		return nil
	}
	// Check again just before publishing, in case PostgreSQL came up while
	// we were waiting for pgctld or reading the settings.
	write := func(path string, contents []byte) error {
		if absent, err := stopped(); !absent {
			return err
		}
		return fileutil.AtomicReplaceFile(path, contents)
	}
	err := pm.editAutoConfWith(ctx, write, func(content string) (string, bool) {
		lines := strings.Split(content, "\n")
		changed := false
		for _, s := range walSettings {
			for i, line := range lines {
				if !autoConfLineSets(line, s.name) {
					continue
				}
				_, value, ok := strings.Cut(line, "=")
				if ok && s.matches(strings.TrimSpace(value), dir) {
					stable, _ := ast.QuoteConfValue(s.stable)
					lines[i] = s.name + " = " + stable
					changed = true
				}
			}
		}
		return strings.Join(lines, "\n"), changed
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Use the executable shell words from the persisted ABI, not an independent
// lookup through the backend's PATH. The two fixed commands each start with one
// quoted executable word; their remaining words are PostgreSQL placeholders.
func postmasterWALProbeCommand(dir string) string {
	archive, _, _ := strings.Cut(backup.ArchiveWALCommand, " ")
	restore, _, _ := strings.Cut(backup.RestoreWALCommand, " ")
	root := sha256.Sum256([]byte(filepath.Clean(dir)))
	var checks []string
	for _, exe := range []string{archive, restore} {
		checks = append(checks, fmt.Sprintf(`(case %[1]s in /*) test -f %[1]s && test ! -L %[1]s && test "$(%[1]s --probe pooler-root %[2]x)" = wal-wrapper-v1 ;; *) exit 1 ;; esac)`, exe, root))
	}
	return strings.Join(checks, " && ")
}

func (pm *MultipoolerManager) probePostmasterWAL(ctx context.Context, dir string) error {
	return pm.adminExec(ctx, `COPY (SELECT 1 WHERE false) TO PROGRAM `+ast.QuoteStringLiteral(postmasterWALProbeCommand(dir)))
}

// Bound the passive inventory, including for a malformed PGDATA mount. An
// unreadable or oversized configuration is unverified, not healthy.
func readWALInventory(path string) ([]byte, error) {
	const maxSize = 1 << 20
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxSize {
		return nil, errors.New("invalid auto.conf for WAL inventory")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxSize+1))
	if len(data) > maxSize {
		return nil, errors.New("auto.conf exceeds WAL inventory limit")
	}
	return data, err
}

// Read only the two persisted settings, including when postgres is stopped or
// rollout is disabled. Never install, clear or reload anything from health.
func (pm *MultipoolerManager) walCommandReadiness(ctx context.Context) string {
	data := postgresDataDir()
	dir := pm.record.PoolerDir()
	if data == "" || dir == "" {
		return ""
	}
	content, err := readWALInventory(filepath.Join(data, "postgresql.auto.conf"))
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		return backupengine.ReadyReasonWALWrapperUnverified
	}
	var values [2]string
	for line := range strings.SplitSeq(string(content), "\n") {
		key, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		for i, s := range walSettings {
			if strings.TrimSpace(key) == s.name {
				values[i] = strings.TrimSpace(v)
			}
		}
	}
	stable, unrecognized := false, false
	for i, s := range walSettings {
		switch v := values[i]; {
		case v == "" || v == "''":
		case v == "'"+s.stable+"'":
			stable = true
		case !s.matches(v, dir):
			unrecognized = true
		}
	}
	if stable {
		if pm.pgctldClient == nil {
			return backupengine.ReadyReasonWALWrapperUnverified
		}
		probeCtx, cancel := context.WithTimeout(ctx, walReadinessTimeout)
		defer cancel()
		resp, err := pm.pgctldClient.Status(probeCtx, &pgctldpb.StatusRequest{})
		if err != nil || probeCtx.Err() != nil || resp == nil || resp.GetWalWrapperProbeUnknown() {
			return backupengine.ReadyReasonWALWrapperUnverified
		}
		if resp.GetWalWrapperVersion() < backup.WALWrapperVersion {
			return backupengine.ReadyReasonWALWrapperUnavailable
		}
		if !sameWALPath(resp.GetPoolerDir(), dir) || !sameWALPath(resp.GetDataDir(), data) {
			return backupengine.ReadyReasonWALWrapperUnverified
		}
		switch resp.GetStatus() {
		case pgctldpb.ServerStatus_STOPPED:
			if resp.GetReady() || resp.GetPid() != 0 {
				return backupengine.ReadyReasonWALWrapperUnverified
			}
		case pgctldpb.ServerStatus_RUNNING:
			// This is diagnostic only. SQL connectivity and the command probe
			// are not a trustworthy gate for converting any live postmaster.
			if !resp.GetReady() || pm.probePostmasterWAL(probeCtx, dir) != nil || probeCtx.Err() != nil {
				return backupengine.ReadyReasonWALWrapperUnverified
			}
		default:
			return backupengine.ReadyReasonWALWrapperUnverified
		}
	}
	if unrecognized {
		return backupengine.ReadyReasonWALCommandUnrecognized
	}
	return ""
}
