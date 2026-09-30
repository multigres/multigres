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

package servenv

import (
	"errors"
)

// ProcessResources holds the resources a process has only one of: its serving
// environment and its gRPC server. The process's main creates and owns them.
// Components that run together in one process (single-process mode) are
// handed these resources instead of creating their own; main builds a single
// value and hands the same value to every component, so they cannot end up
// with different servers.
//
// The topology store is not here: main only opens it after parsing flags, well
// after components are constructed, so there is nothing valid to hand out yet
// at that point. main passes the opened store directly to each component's
// Init instead, once it exists.
type ProcessResources struct {
	// ServEnv is the process's serving environment. main registers its flags,
	// loads its configuration, calls Init once and runs its serving loop.
	ServEnv *ServEnv

	// GrpcServer is the gRPC server the serving loop starts. Components register
	// their services on it; main registers its flags.
	GrpcServer *GrpcServer
}

// Validate reports whether every resource is set. A component given an
// incomplete ProcessResources would, for example, register its services on a
// gRPC server that is never started.
func (pr ProcessResources) Validate() error {
	var errs []error
	if pr.ServEnv == nil {
		errs = append(errs, errors.New("ServEnv is required"))
	}
	if pr.GrpcServer == nil {
		errs = append(errs, errors.New("GrpcServer is required"))
	}
	return errors.Join(errs...)
}
