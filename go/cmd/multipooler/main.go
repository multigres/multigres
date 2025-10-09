// Copyright 2025 Supabase, Inc.
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

// multipooler provides connection pooling and communicates with pgctld via gRPC
// to serve queries from multigateway instances.
package main

import (
	"log/slog"
	"os"

	"github.com/multigres/multigres/go/multipooler"

	"github.com/spf13/cobra"
)

// CreateMultiPoolerCommand creates a cobra command with a MultiPooler instance and registers its flags
func CreateMultiPoolerCommand() (*cobra.Command, *multipooler.MultiPooler) {
	mp := multipooler.NewMultiPooler()

	cmd := &cobra.Command{
		Use:   "multipooler",
		Short: "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
		Long:  "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return mp.CobraPreRunE(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args, mp)
		},
	}

	mp.RegisterFlags(cmd.Flags())

	return cmd, mp
}

func main() {
	cmd, _ := CreateMultiPoolerCommand()

	if err := cmd.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string, mp *multipooler.MultiPooler) error {
	mp.Init()
	mp.RunDefault()
	return nil
}
