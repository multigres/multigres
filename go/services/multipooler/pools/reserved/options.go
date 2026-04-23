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

package reserved

import (
	"context"

	"github.com/multigres/multigres/go/services/multipooler/pools/regular"
)

// ReservedConnOption configures a NewConn call.
type ReservedConnOption func(*reservedConnOpts)

// reservedConnOpts holds optional behavior for NewConn.
type reservedConnOpts struct {
	// validate, if non-nil, is invoked after a regular connection is acquired
	// and before the reserved connection is registered. If it returns a
	// connection error the underlying socket is tainted and another attempt
	// is made (up to maxNewConnAttempts). Other errors are returned unchanged
	// after recycling the connection.
	validate func(context.Context, *regular.Conn) error
}

// WithValidate registers a callback that runs against the freshly acquired
// regular connection. Returning a connection error triggers a transparent
// retry against a replacement socket; returning any other error aborts
// NewConn and propagates the error to the caller.
func WithValidate(fn func(context.Context, *regular.Conn) error) ReservedConnOption {
	return func(o *reservedConnOpts) {
		o.validate = fn
	}
}
