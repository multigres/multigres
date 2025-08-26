/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// pgctld manages PostgreSQL server instances within the Multigres cluster,
// providing lifecycle management and configuration control.
package main

import (
	"log/slog"
	"os"

	"github.com/multigres/multigres/go/cmd/pgctld/command"
	"github.com/multigres/multigres/go/servenv"
)

func main() {
	if err := command.Root.Execute(); err != nil {
		slog.Error("Command execution failed", "error", err)
		os.Exit(1)
	}
}

func init() {
	servenv.RegisterServiceCmd(command.Root)
	servenv.InitServiceMap("grpc", "pgctld")
}
