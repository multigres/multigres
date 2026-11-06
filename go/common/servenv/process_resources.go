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
// environment and its gRPC server. The process's main creates and owns them
// and hands them to every component it runs; components never create their
// own. When several components run in one process (as in minigres), main
// hands the same value to each, so they cannot end up with different servers.
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

	// QueryGrpcServer is where the multipooler serves its query service over
	// gRPC: the same as GrpcServer. Nil in minigres, where the only caller,
	// the multigateway in the same process, calls the pooler directly, so the
	// query service is not served over gRPC at all. Optional.
	QueryGrpcServer *GrpcServer

	// SequencedShutdown is set when main runs the components' shutdown
	// itself, in order, instead of each component registering it as a
	// shutdown hook of its own. Minigres sets it so its gateway drains before
	// its pooler stops PostgreSQL: servenv runs the hooks of one phase all at
	// once.
	SequencedShutdown bool
}

// Validate reports whether every required resource is set. A component given an
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
