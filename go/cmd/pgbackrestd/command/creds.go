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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

const credsRequestTimeout = 30 * time.Second

func newCredsCmd() *cobra.Command {
	var stateDir string
	cmd := &cobra.Command{
		Use:   "creds",
		Short: "Print S3 credentials from the sidecar for pgBackRest's process key type",
		Args:  cobra.NoArgs,
		// pgBackRest parses stdout and stderr is its error text, so failure is
		// exactly one stderr line and an exit status, not cobra's usual output.
		SilenceErrors: true,
		// The root hook logs "logging initialized" to stdout, which would
		// corrupt the JSON pgBackRest reads.
		PersistentPreRunE: func(*cobra.Command, []string) error { return nil },
		RunE: func(cmd *cobra.Command, _ []string) error {
			body, err := fetchCreds(cmd.Context(), stateDir, os.Getenv("PGBACKRESTD_CREDS_TOKEN"))
			if err != nil {
				fmt.Fprintln(os.Stderr, "pgbackrestd creds:", err)
				os.Exit(1) //nolint:forbidigo // exit status is the helper's contract with pgBackRest
			}
			_, err = cmd.OutOrStdout().Write(body)
			return err
		},
	}
	cmd.Flags().StringVar(&stateDir, "state-dir", "/var/lib/pgbackrestd", "Private state directory of the sidecar")
	return cmd
}

// fetchCreds asks the sidecar's credential socket for credentials.
func fetchCreds(ctx context.Context, stateDir, token string) ([]byte, error) {
	if token == "" {
		return nil, errors.New("PGBACKRESTD_CREDS_TOKEN is not set")
	}
	sock := filepath.Join(stateDir, "creds", "creds.sock")
	client := &http.Client{
		Timeout: credsRequestTimeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://pgbackrestd/creds", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Pgbackrestd-Token", token)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("credential socket answered %s", resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}
