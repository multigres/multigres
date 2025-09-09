/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Modifications Copyright 2025 The Multigres Authors.
*/

package servenv

import (
	"log/slog"
	"net"
	"os"

	"github.com/spf13/pflag"
)

// socketFile has the flag used when calling
// RegisterDefaultSocketFileFlags.
var socketFile string

// serveSocketFile listen to the named socket and serves RPCs on it.
func serveSocketFile() {
	if socketFile == "" {
		slog.Info("Not listening on socket file")
		return
	}
	name := socketFile

	// try to delete if file exists
	if _, err := os.Stat(name); err == nil {
		err = os.Remove(name)
		if err != nil {
			slog.Info("Cannot remove socket file", "file", name, "err", err)
			os.Exit(1)
		}
	}

	l, err := net.Listen("unix", name)
	if err != nil {
		slog.Error("Error listening on socket file", "name", name, "err", err)
	}
	slog.Info("Listening on socket file for gRPC", "name", name)
	go func() {
		if err := GRPCServer.Serve(l); err != nil {
			slog.Error("grpc server failed", "err", err)
		}
	}()
}

// RegisterDefaultSocketFileFlags registers the default flags for listening
// to a socket. This needs to be called before flags are parsed.
func RegisterDefaultSocketFileFlags() {
	OnParse(func(fs *pflag.FlagSet) {
		fs.StringVar(&socketFile, "socket-file", socketFile, "Local unix socket file to listen on")
	})
}
