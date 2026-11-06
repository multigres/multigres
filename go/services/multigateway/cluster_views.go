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

package multigateway

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
)

// ClusterViewRows serves the gateway's cluster views (see
// engine/cluster_view.go): multigres.multipoolers from the pooler cache and
// multigres.multigateways from the topology.
func (mg *Multigateway) ClusterViewRows(ctx context.Context, view string) ([][]any, error) {
	switch view {
	case constants.MultigresMultipoolersView:
		return mg.poolerGateway.MultipoolerRows(), nil
	case constants.MultigresMultigatewaysView:
		return mg.multigatewayRows(ctx)
	}
	return nil, mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
		fmt.Sprintf("internal error: unknown cluster view %q", view), "")
}

// multigatewayRows reads every registered gateway from the topology, one row
// per gateway in the column order of multigres.multigateways: name, cell,
// pid_prefix, is_self. A PID prefix the gateway did not claim (a gateway
// serving a single pooler claims none) is NULL. The topology is read on
// every call: the gateway does not watch its peers, and the view is for
// operators, not the query path.
func (mg *Multigateway) multigatewayRows(ctx context.Context) ([][]any, error) {
	gateways, err := listMultigateways(ctx, mg.ts, func(cell string, err error) error {
		return mterrors.NewPgError("ERROR", mterrors.PgSSCannotConnectNow,
			fmt.Sprintf("multigres.multigateways: reading cell %q", cell), err.Error())
	})
	if err != nil {
		var pgErr *mterrors.PgDiagnostic
		if errors.As(err, &pgErr) {
			return nil, err
		}
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSCannotConnectNow,
			"multigres.multigateways: topology unavailable", err.Error())
	}
	sort.Slice(gateways, func(i, j int) bool {
		a, b := gateways[i].GetId(), gateways[j].GetId()
		if a.GetCell() != b.GetCell() {
			return a.GetCell() < b.GetCell()
		}
		return a.GetName() < b.GetName()
	})
	rows := make([][]any, 0, len(gateways))
	for _, gw := range gateways {
		rows = append(rows, []any{
			gw.GetId().GetName(),
			gw.GetId().GetCell(),
			prefixOrNull(gw.GetPidPrefix()),
			topoclient.ComponentIDString(gw.GetId()) == topoclient.ComponentIDString(mg.gatewayID),
		})
	}
	return rows, nil
}

// listMultigateways reads every registered gateway across all cells. A cell
// read failure is passed to onCellErr: returning nil skips the cell, returning
// an error stops the listing with it. A cell-names failure is returned as is.
func listMultigateways(ctx context.Context, ts topoclient.Store, onCellErr func(cell string, err error) error) ([]*topoclient.MultigatewayInfo, error) {
	cells, err := ts.GetCellNames(ctx)
	if err != nil {
		return nil, err
	}
	var gateways []*topoclient.MultigatewayInfo
	for _, cell := range cells {
		inCell, err := ts.GetMultigatewaysByCell(ctx, cell)
		if err != nil {
			if err := onCellErr(cell, err); err != nil {
				return nil, err
			}
			continue
		}
		gateways = append(gateways, inCell...)
	}
	return gateways, nil
}

func prefixOrNull(prefix uint32) any {
	if prefix == 0 {
		return nil
	}
	return prefix
}
