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

package topoclient

import (
	"cmp"
	"context"
	"errors"
	"path"
	"slices"
	"strconv"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// Tablegroup provision claims (see TableGroupProvisionClaim in
// clustermetadata.proto). A provisioner claims a tablegroup's catalog id
// before it provisions anything for it and releases the claim after it has
// removed everything, so the catalog primary never deletes a tablegroup's
// rows while a provisioner still holds something for it: a drop completes
// only once the id has neither a claim nor a pooler record. Claims are keyed
// by catalog id, which is never reused, so a claim can never be mistaken for
// one on a later tablegroup of the same name.

func tableGroupClaimsDir(database string) string {
	return path.Join(DatabasesPath, database, TableGroupClaimsDir)
}

func tableGroupClaimPath(database string, tableGroupID int64) string {
	return path.Join(tableGroupClaimsDir(database), strconv.FormatInt(tableGroupID, 10))
}

// validateClaimArgs rejects arguments that would otherwise resolve to a path
// under a phantom database or a node that can never be a claim.
func validateClaimArgs(database string, tableGroupID int64) error {
	if database == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "a tablegroup provision claim needs a database")
	}
	if tableGroupID <= 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "a tablegroup provision claim needs a positive tablegroup id, got %d", tableGroupID)
	}
	return nil
}

// claimAttempts bounds the create-then-read loop in ClaimTableGroupProvisioning
// against a claim that keeps appearing and vanishing between the two steps.
const claimAttempts = 3

// ClaimTableGroupProvisioning creates the claim node for claim.TablegroupId
// with a create-if-not-exists write, stamping claimed_at if the caller left it
// empty. A claim already held by the same provisioner is left as it is and is
// not an error, so a provisioner that crashed after claiming can resume. A
// claim held by another provisioner is a NodeExists error: an id has one
// provisioner.
//
// A provisioner claims before it provisions anything, then reads the catalog's
// list again: a tablegroup it listed as creating may have been dropped and, as
// it was unclaimed, deleted in between. It provisions only if the tablegroup
// is still listed as creating or active, and otherwise releases the claim.
func (ts *store) ClaimTableGroupProvisioning(ctx context.Context, database string, claim *clustermetadatapb.TableGroupProvisionClaim) error {
	if err := validateClaimArgs(database, claim.GetTablegroupId()); err != nil {
		return err
	}
	if claim.GetProvisioner() == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "a tablegroup provision claim needs a provisioner")
	}
	if claim.GetClaimedAt() == nil {
		claim = proto.Clone(claim).(*clustermetadatapb.TableGroupProvisionClaim)
		claim.ClaimedAt = timestamppb.Now()
	}
	data, err := proto.Marshal(claim)
	if err != nil {
		return mterrors.Wrapf(err, "failed to marshal the provision claim for tablegroup %d", claim.GetTablegroupId())
	}
	filePath := tableGroupClaimPath(database, claim.GetTablegroupId())
	var lastErr error
	for range claimAttempts {
		_, err := ts.globalTopo.Create(ctx, filePath, data)
		if err == nil {
			return nil
		}
		if !errors.Is(err, &TopoError{Code: NodeExists}) {
			return mterrors.Wrapf(err, "failed to claim tablegroup %d", claim.GetTablegroupId())
		}
		lastErr = err
		existing, err := ts.GetTableGroupProvisionClaim(ctx, database, claim.GetTablegroupId())
		if errors.Is(err, &TopoError{Code: NoNode}) {
			continue // released between the create and the read: the id is free, claim it
		}
		if err != nil {
			return err
		}
		if existing.GetProvisioner() != claim.GetProvisioner() {
			return NewError(NodeExists, filePath)
		}
		return nil
	}
	return mterrors.Wrapf(lastErr, "failed to claim tablegroup %d after %d attempts", claim.GetTablegroupId(), claimAttempts)
}

// GetTableGroupProvisionClaim reads one claim. A missing claim is a NoNode
// error, which is how a caller learns the id is unclaimed.
func (ts *store) GetTableGroupProvisionClaim(ctx context.Context, database string, tableGroupID int64) (*clustermetadatapb.TableGroupProvisionClaim, error) {
	if err := validateClaimArgs(database, tableGroupID); err != nil {
		return nil, err
	}
	data, _, err := ts.globalTopo.Get(ctx, tableGroupClaimPath(database, tableGroupID))
	if err != nil {
		return nil, err
	}
	claim := &clustermetadatapb.TableGroupProvisionClaim{}
	if err := proto.Unmarshal(data, claim); err != nil {
		return nil, mterrors.Wrapf(err, "failed to unmarshal the provision claim for tablegroup %d", tableGroupID)
	}
	return claim, nil
}

// ListTableGroupProvisionClaims returns a database's claims in ascending id
// order (the directory lists ids as strings, so "10" would otherwise sort
// before "9"). A database with no claims, or that has never had one, returns
// an empty list. Nodes in the directory that are not claims are ignored: a
// pooler started with the directory's name as its tablegroup, which nothing
// forbids, would leave its shard's init claim there.
func (ts *store) ListTableGroupProvisionClaims(ctx context.Context, database string) ([]*clustermetadatapb.TableGroupProvisionClaim, error) {
	if database == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "a tablegroup provision claim needs a database")
	}
	entries, err := ts.globalTopo.ListDir(ctx, tableGroupClaimsDir(database), false)
	if errors.Is(err, &TopoError{Code: NoNode}) {
		return nil, nil
	}
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to list the tablegroup provision claims of database %s", database)
	}
	claims := make([]*clustermetadatapb.TableGroupProvisionClaim, 0, len(entries))
	for _, entry := range entries {
		id, err := strconv.ParseInt(entry.Name, 10, 64)
		if err != nil || id <= 0 {
			continue
		}
		claim, err := ts.GetTableGroupProvisionClaim(ctx, database, id)
		if errors.Is(err, &TopoError{Code: NoNode}) {
			continue // released between the listing and the read
		}
		if err != nil {
			return nil, err
		}
		claims = append(claims, claim)
	}
	slices.SortFunc(claims, func(a, b *clustermetadatapb.TableGroupProvisionClaim) int {
		return cmp.Compare(a.GetTablegroupId(), b.GetTablegroupId())
	})
	return claims, nil
}

// ReleaseTableGroupProvisioning deletes the claim node. A claim that is not
// held is not an error, so a crashed provisioner can repeat its removal.
func (ts *store) ReleaseTableGroupProvisioning(ctx context.Context, database string, tableGroupID int64) error {
	if err := validateClaimArgs(database, tableGroupID); err != nil {
		return err
	}
	err := ts.globalTopo.Delete(ctx, tableGroupClaimPath(database, tableGroupID), nil)
	if err == nil || errors.Is(err, &TopoError{Code: NoNode}) {
		return nil
	}
	return mterrors.Wrapf(err, "failed to release the provision claim for tablegroup %d", tableGroupID)
}
