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

package poolergateway

import (
	"errors"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// newUnavailablePgError preserves the canonical UNAVAILABLE classification
// used by gateway routing while exposing a PostgreSQL diagnostic to pgwire
// callers. The diagnostic is safe to forward only for failures known to occur
// before a role lookup.
func newUnavailablePgError(message, internalFormat string, args ...any) error {
	diagnostic := mterrors.NewPgError("ERROR", mterrors.PgSSCannotConnectNow, message, "")
	return mterrors.WithCode(
		mterrors.Wrapf(diagnostic, internalFormat, args...),
		mtrpcpb.Code_UNAVAILABLE,
	)
}

type noWritablePrimaryError struct{ error }

func (e *noWritablePrimaryError) Unwrap() error { return e.error }

func newNoWritablePrimaryError(internalFormat string, args ...any) error {
	return &noWritablePrimaryError{newUnavailablePgError(
		"no writable primary is currently available",
		internalFormat,
		args...,
	)}
}

func isNoWritablePrimaryError(err error) bool {
	var target *noWritablePrimaryError
	return errors.As(err, &target)
}

// isCredentialSourceUnavailable recognizes only errors that occur before the
// pooler can inspect pg_authid. Buffer terminal errors retain the semantics of
// the MTF01 that caused credential lookup to enter failover buffering.
func isCredentialSourceUnavailable(err error) bool {
	return mterrors.Code(err) == mtrpcpb.Code_UNAVAILABLE ||
		mterrors.IsErrorCode(err,
			mterrors.MTF01.ID,
			mterrors.MTB01.ID,
			mterrors.MTB02.ID,
			mterrors.MTB03.ID,
		)
}
