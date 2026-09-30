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

// minigres runs a multigateway and a multipooler in one process, for a
// database served by a single pooler with no replicas.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multigateway"
	"github.com/multigres/multigres/go/services/multipooler"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// sharedComponentFlags are flags both halves define with the same meaning.
// They are exposed once, through the multigateway's definition, and copied
// into the multipooler's flag set before either half starts.
var sharedComponentFlags = []string{"cell", "service-id", "enable-slot-based-replication"}

// omittedPoolerFlags are multipooler flags minigres does not expose. The
// multigateway's pg-port is the client-facing port; the multipooler adopts the
// postgres port from pgctld when its own pg-port is not set.
var omittedPoolerFlags = []string{"pg-port"}

// minigres holds the pieces both halves share and the two halves themselves.
type minigres struct {
	senv       *servenv.ServEnv
	grpcServer *servenv.GrpcServer
	topoConfig *topoclient.TopoConfig
	ts         topoclient.Store

	gateway *multigateway.Multigateway
	pooler  *multipooler.Multipooler

	// poolerFlags is the multipooler's own flag set. The multipooler checks by
	// name whether its flags were set explicitly (for example to decide whether
	// to adopt pg-port from pgctld), so it must not see the multigateway's
	// flags of the same name.
	poolerFlags *pflag.FlagSet
}

func newMinigres() *minigres {
	reg := viperutil.NewRegistry()
	tel := telemetry.NewTelemetry()
	m := &minigres{
		senv:        servenv.NewServEnvWithConfig(reg, servenv.NewLogger(reg, tel), viperutil.NewViperConfig(reg), tel),
		grpcServer:  servenv.NewGrpcServer(reg),
		topoConfig:  topoclient.NewTopoConfig(reg),
		poolerFlags: pflag.NewFlagSet(constants.ServiceMultipooler, pflag.ContinueOnError),
	}
	// One value for both halves, so they cannot be given different servers.
	resources := servenv.ProcessResources{
		ServEnv:    m.senv,
		GrpcServer: m.grpcServer,
		TopoStore:  func() topoclient.Store { return m.ts },
	}
	m.pooler = multipooler.NewMultipooler(tel, multipooler.WithSingleProcessMode(resources))
	m.gateway = multigateway.NewMultigateway(multigateway.WithSingleProcessMode(resources))
	return m
}

// registerFlags defines the shared flags once, then each half's own flags.
func (m *minigres) registerFlags(fs *pflag.FlagSet) error {
	m.senv.RegisterFlags(fs)
	m.grpcServer.RegisterFlags(fs)
	m.topoConfig.RegisterFlags(fs)
	m.gateway.RegisterFlags(fs)
	m.pooler.RegisterFlags(m.poolerFlags)
	return mergePoolerFlags(fs, m.poolerFlags)
}

// mergePoolerFlags exposes the multipooler's flags on fs, except the shared
// and omitted ones. The flags are added by reference, so parsing fs sets them
// in the multipooler's flag set too.
func mergePoolerFlags(fs, poolerFlags *pflag.FlagSet) error {
	var err error
	poolerFlags.VisitAll(func(f *pflag.Flag) {
		if err != nil || slices.Contains(sharedComponentFlags, f.Name) || slices.Contains(omittedPoolerFlags, f.Name) {
			return
		}
		if fs.Lookup(f.Name) != nil {
			err = fmt.Errorf("flag %q is defined by both multigateway and multipooler", f.Name)
			return
		}
		fs.AddFlag(f)
	})
	return err
}

// copySharedFlags copies the shared flags that were set on fs into the
// multipooler's flag set, so both halves see the same values.
func copySharedFlags(fs, poolerFlags *pflag.FlagSet) error {
	for _, name := range sharedComponentFlags {
		f := fs.Lookup(name)
		if f == nil || !f.Changed {
			continue
		}
		if err := poolerFlags.Set(name, f.Value.String()); err != nil {
			return fmt.Errorf("copy flag %q to multipooler: %w", name, err)
		}
	}
	return nil
}

// setDefaultIfUnset gives a flag a minigres-specific value unless the operator
// set it. The multipooler requires a table group and shard, and the gateway
// only routes to the default pair.
func setDefaultIfUnset(fs *pflag.FlagSet, name, value string) error {
	if fs.Changed(name) {
		return nil
	}
	return fs.Set(name, value)
}

func (m *minigres) preRun(cmd *cobra.Command) error {
	fs := cmd.Flags()
	if err := setDefaultIfUnset(fs, "table-group", constants.DefaultTableGroup); err != nil {
		return err
	}
	if err := setDefaultIfUnset(fs, "shard", constants.DefaultShard); err != nil {
		return err
	}
	if err := copySharedFlags(fs, m.poolerFlags); err != nil {
		return err
	}
	if err := m.gateway.CobraPreRunE(cmd); err != nil {
		return err
	}
	if err := m.pooler.CobraPreRunE(cmd); err != nil {
		return err
	}
	return m.senv.CobraPreRunE(cmd)
}

func (m *minigres) run(ctx context.Context) error {
	ts, err := m.topoConfig.Open()
	if err != nil {
		return fmt.Errorf("topo open: %w", err)
	}
	m.ts = ts
	// Closed only after the serving loop returns, which is after both halves'
	// shutdown hooks have run.
	defer ts.Close()

	// servenv.Init may only run once per process, so it runs here with the
	// process identity rather than in either half.
	id := m.pooler.ServiceIdentity()
	id.ServiceName = constants.ServiceMinigres
	if id.ServiceInstanceID == "" {
		id.ServiceInstanceID = servenv.GenerateRandomServiceID()
	}
	if err := m.senv.Init(id); err != nil {
		return fmt.Errorf("servenv init: %w", err)
	}

	if err := m.pooler.Init(ctx); err != nil {
		return err
	}
	if err := m.gateway.Init(ctx); err != nil {
		return err
	}
	m.senv.HTTPHandleFunc("/", handleIndex)
	return m.senv.RunDefault(m.grpcServer)
}

// handleIndex links to both halves' status pages, which a shared servenv
// serves under their service names.
func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<html><head><title>Minigres</title></head><body>
<h1>Minigres</h1>
<ul>
<li><a href="/%s">Multigateway status</a></li>
<li><a href="/%s">Multipooler status</a></li>
<li><a href="/config">Config</a></li>
<li><a href="/live">Live</a></li>
<li><a href="/ready">Ready</a></li>
</ul>
</body></html>
`, constants.ServiceMultigateway, constants.ServiceMultipooler)
}

// CreateMinigresCommand creates the minigres command with both halves wired
// together and their flags registered.
func CreateMinigresCommand() (*cobra.Command, error) {
	m := newMinigres()
	cmd := &cobra.Command{
		Use:   constants.ServiceMinigres,
		Short: "Minigres runs a multigateway and a multipooler in one process, for a database served by a single pooler with no replicas.",
		Long:  "Minigres runs a multigateway and a multipooler in one process, for a database served by a single pooler with no replicas.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return m.preRun(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return m.run(cmd.Context())
		},
	}
	if err := m.registerFlags(cmd.Flags()); err != nil {
		return nil, err
	}
	return cmd, nil
}

func main() {
	cmd, err := CreateMinigresCommand()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
	if err := cmd.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}
