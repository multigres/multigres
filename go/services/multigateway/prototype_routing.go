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
	"errors"
	"log/slog"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/services/multigateway/executor"
	"github.com/multigres/multigres/go/services/multigateway/poolergateway"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// PROTOTYPE: the application routing of the gateway is fixed at startup. It sends the
// application's SQL of one database to one tablegroup, both named in the gateway's
// configuration (--routing-database and --routing-tablegroup). It is how a gateway serves a
// database from an unmanaged tablegroup until routing that comes from the catalog replaces it:
// the tablegroup does not change while the gateway runs, and it is set before the gateway
// serves anything.
type staticRouting struct {
	database   viperutil.Value[string]
	tablegroup viperutil.Value[string]
}

func (r *staticRouting) RegisterFlags(fs *pflag.FlagSet, reg *viperutil.Registry) {
	r.database = viperutil.Configure(reg, "routing-database", viperutil.Options[string]{
		Default:  "",
		FlagName: "routing-database",
		Dynamic:  false,
	})
	r.tablegroup = viperutil.Configure(reg, "routing-tablegroup", viperutil.Options[string]{
		Default:  "",
		FlagName: "routing-tablegroup",
		Dynamic:  false,
	})
	fs.String("routing-database", r.database.Default(), "send the application queries of this database to --routing-tablegroup instead of the default tablegroup (prototype)")
	fs.String("routing-tablegroup", r.tablegroup.Default(), "the tablegroup that serves the application queries of --routing-database, for example one served by an unmanaged pooler (prototype)")
	viperutil.BindFlags(fs, r.database, r.tablegroup)
}

// apply sets the routing on the executor's planner and on the pooler gateway. With neither flag
// set it does nothing, and application SQL goes to the default tablegroup, as it always did.
func (r *staticRouting) apply(ex *executor.Executor, pg *poolergateway.PoolerGateway, logger *slog.Logger) error {
	if r == nil {
		return nil
	}
	db, tg := r.database.Get(), r.tablegroup.Get()
	if db == "" && tg == "" {
		return nil
	}
	if db == "" || tg == "" {
		return errors.New("--routing-database and --routing-tablegroup are set together")
	}
	ex.SetApplicationRouting(db, tg)
	pg.SetAppRouting(poolergateway.NewAppRouting(db, tg))
	logger.Info("application routing", "database", db, "tablegroup", tg)
	return nil
}
