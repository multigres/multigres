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

package multipooler

import (
	"fmt"

	"github.com/multigres/multigres/go/common/servenv"
)

// Option configures a Multipooler at construction. With no options the
// multipooler owns everything, as a standalone process.
type Option func(*options)

type options struct {
	// single holds the process's shared resources in single-process mode; nil
	// when the multipooler runs standalone.
	single *servenv.ProcessResources
}

// WithSingleProcessMode runs the multipooler in single-process mode, in one
// process with the multigateway.
//
// The multipooler uses the given process resources instead of creating its
// own: it registers none of their flags, does not call servenv.Init, does not
// open or close the store, and serves its status page under /multipooler
// instead of /.
//
// In single-process mode the multipooler is the only pooler of its shard, so it
// is the shard's leader without consensus: the consensus service is never
// registered, even if --service-map asks for it; postgres is started and kept
// as a primary; and the routing role is PRIMARY whenever postgres is out of
// recovery.
//
// It panics if resources is incomplete, which is a programming error in the
// caller.
func WithSingleProcessMode(resources servenv.ProcessResources) Option {
	if err := resources.Validate(); err != nil {
		panic(fmt.Sprintf("multipooler: invalid ProcessResources: %v", err))
	}
	return func(o *options) {
		o.single = &resources
	}
}
