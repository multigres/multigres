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

	"github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
)

// VerifyBackups runs a full-stanza pgbackrest verify, validating every backup
// file and WAL segment in the repository. It delegates to the backup engine;
// see backup.Engine.Verify for the concurrency and error semantics.
func (pm *MultiPoolerManager) VerifyBackups(ctx context.Context) (*backup.VerifyResult, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	return pm.backup.Verify(ctx)
}
