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

package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"sync"

	"github.com/multigres/multigres/go/common/safefs"
)

// generationsCopyFile is the PVC copy of the generation list. It lets a
// restarting multipooler configure the sidecar before postgres is up to read
// the table. It is only ever a floor: a copy is replaced solely by a strictly
// higher revision, so a stale writer cannot move the sidecar back to a
// retired generation.
const generationsCopyFile = "pgbackrest-generations.json"

// RepoGeneration maps a repository generation to the pgBackRest repo number
// that holds it.
type RepoGeneration struct {
	Generation uint32 `json:"generation"`
	RepoNumber uint32 `json:"repo_number"`
}

type generationsCopyDoc struct {
	Revision int64            `json:"revision"`
	Repos    []RepoGeneration `json:"repos"`
}

var errEmptyGenerations = errors.New("refusing to store an empty pgbackrest generation list")

// generationsCopyMu makes the read-compare-write in writeGenerationsCopy
// atomic within the process. The file is per pooler dir and one multipooler
// owns it, so a process-wide lock costs nothing.
var generationsCopyMu sync.Mutex

// readGenerationsCopy returns the PVC copy. ok is false when there is no
// usable copy (a brand-new PVC, or an unreadable or invalid file); the caller
// then falls back to generation 1 at repo 1, revision 0, and the table
// supplies the real list once postgres is reachable.
func (pm *MultipoolerManager) readGenerationsCopy() (rev int64, repos []RepoGeneration, ok bool) {
	doc, err := pm.loadGenerationsCopy()
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			pm.logger.Warn("ignoring unusable pgbackrest generations copy", "error", err)
		}
		return 0, nil, false
	}
	return doc.Revision, doc.Repos, true
}

func (pm *MultipoolerManager) loadGenerationsCopy() (*generationsCopyDoc, error) {
	root, err := pm.openGenerationsRoot()
	if err != nil {
		return nil, err
	}
	defer root.Close()
	data, err := root.ReadFile(generationsCopyFile)
	if err != nil {
		return nil, err
	}
	var doc generationsCopyDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", generationsCopyFile, err)
	}
	if doc.Revision < 0 {
		return nil, fmt.Errorf("invalid %s: negative revision %d", generationsCopyFile, doc.Revision)
	}
	// The sidecar rejects an empty list, and a stored one would outrank the
	// revision-0 fallback, so an empty copy counts as absent.
	if len(doc.Repos) == 0 {
		return nil, fmt.Errorf("invalid %s: no repos", generationsCopyFile)
	}
	return &doc, nil
}

// openGenerationsRoot returns a fresh handle on the data volume root. The
// default opens poolerDir by path, which safefs does not anchor to a mount
// root; production must set pm.volumeRoot to an opener built on
// sandbox.OpenAnchor.
func (pm *MultipoolerManager) openGenerationsRoot() (*safefs.Root, error) {
	if pm.volumeRoot != nil {
		return pm.volumeRoot()
	}
	dir := pm.record.PoolerDir()
	if dir == "" {
		return nil, errors.New("pooler dir is not set")
	}
	return safefs.OpenRoot(dir)
}

// writeGenerationsCopy stores rev and repos, but only when rev is greater than
// the revision already on disk; an equal or older revision is a no-op, never
// an error. Callers write only after the table transaction that produced rev
// has committed, so the copy never runs ahead of the table.
func (pm *MultipoolerManager) writeGenerationsCopy(rev int64, repos []RepoGeneration) error {
	if len(repos) == 0 {
		return errEmptyGenerations
	}
	generationsCopyMu.Lock()
	defer generationsCopyMu.Unlock()

	if cur, _, ok := pm.readGenerationsCopy(); ok && rev <= cur {
		return nil
	}

	sorted := slices.Clone(repos)
	slices.SortFunc(sorted, func(a, b RepoGeneration) int {
		if a.Generation != b.Generation {
			return int(int64(a.Generation) - int64(b.Generation))
		}
		return int(int64(a.RepoNumber) - int64(b.RepoNumber))
	})
	data, err := json.Marshal(generationsCopyDoc{Revision: rev, Repos: sorted})
	if err != nil {
		return fmt.Errorf("marshal pgbackrest generations copy: %w", err)
	}

	root, err := pm.openGenerationsRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	return root.WriteFile(generationsCopyFile, data, 0o600)
}

// refreshGenerationsCopy brings the PVC copy up to the table's revision. It
// is the standby path too: a standby sees a promoted primary's new list only
// through replication, so it refreshes when the table is ahead of its copy.
func (pm *MultipoolerManager) refreshGenerationsCopy(ctx context.Context) error {
	rev, repos, err := pm.readGenerations(ctx)
	if err != nil {
		return err
	}
	// Between createSidecarSchema and the seed the table has a revision but no
	// repos; there is nothing worth copying yet.
	if len(repos) == 0 {
		return nil
	}
	return pm.writeGenerationsCopy(rev, repos)
}
