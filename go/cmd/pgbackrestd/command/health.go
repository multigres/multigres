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

package command

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
)

const healthTimeout = 2 * time.Second

// newHealthCmd is the container probe. It checks only that the control API
// answers. It does not look at S3, at 8432, at the certificates or at the
// stanza: the probe gates postgres from starting, and a repository outage or
// a server problem must never block the primary. Those states are reported by
// the pgbackrest.server.up gauge and Status.
func newHealthCmd() *cobra.Command {
	var controlSocket string
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Container health probe",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), healthTimeout)
			defer cancel()
			return probeControl(ctx, controlSocket)
		},
	}
	f := cmd.Flags()
	f.StringVar(&controlSocket, "control-socket", "/run/pgbackrest-control/control.sock", "Unix socket for the control API")
	return cmd
}

func probeControl(ctx context.Context, socket string) error {
	var d net.Dialer
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return d.DialContext(ctx, "unix", socket)
		},
		DisableKeepAlives: true,
	}}
	defer client.CloseIdleConnections()
	svc := pgbackrestdserviceconnect.NewPgBackRestdClient(client, "http://pgbackrestd")
	if _, err := svc.Status(ctx, connect.NewRequest(&pgbackrestdservice.StatusRequest{})); err != nil {
		return fmt.Errorf("control status: %w", err)
	}
	return nil
}
