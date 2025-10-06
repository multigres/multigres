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
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

var Main = &cobra.Command{
	Use:     "multipooler",
	Short:   "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
	Long:    "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
	Args:    cobra.NoArgs,
	PreRunE: servenv.CobraPreRunE,
	RunE:    run,
}

func main() {
	if err := Main.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	servenv.Init()
	multipooler.Init()
	servenv.OnClose(multipooler.Shutdown)
	servenv.RunDefault()

	return nil
}

func init() {
	servenv.OnParseFor("multipooler", multipooler.RegisterFlags)
	servenv.RegisterServiceCmd(Main)
	servenv.RegisterGRPCServerFlags()
}
