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
	"strings"

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

// translatePreExecutionUnavailable removes the internal retry marker after the
// gateway finishes buffering while retaining its client-safe diagnostic.
func translatePreExecutionUnavailable(err error) error {
	if !mterrors.IsPreExecutionUnavailable(err) {
		return err
	}
	var diagnostic *mterrors.PgDiagnostic
	if errors.As(err, &diagnostic) {
		return mterrors.WithCode(diagnostic, mtrpcpb.Code_UNAVAILABLE)
	}
	return err
}

// isReadWriteDuringRecoveryError recognizes PostgreSQL's rejection of BEGIN
// READ WRITE on a standby. Match both SQLSTATE and exact message because 0A000
// covers many unrelated feature-not-supported errors.
func isReadWriteDuringRecoveryError(err error) bool {
	var diagnostic *mterrors.PgDiagnostic
	return errors.As(err, &diagnostic) &&
		diagnostic.Code == mterrors.PgSSFeatureNotSupported &&
		diagnostic.Message == "cannot set transaction read-write mode during recovery"
}

// isCredentialSourceUnavailable recognizes only errors that occur before the
// pooler can inspect pg_authid. Buffer terminal errors retain the semantics of
// the MTF01 that caused credential lookup to enter failover buffering.
func isCredentialSourceUnavailable(err error) bool {
	return mterrors.Code(err) == mtrpcpb.Code_UNAVAILABLE ||
		mterrors.IsPreExecutionUnavailable(err) ||
		mterrors.IsErrorCode(err,
			mterrors.MTF01.ID,
			mterrors.MTB01.ID,
			mterrors.MTB02.ID,
			mterrors.MTB03.ID,
		)
}

// catalogPrimaryMovedMessages are the refusals the catalog shard's poolers
// give a catalog read (checkCatalogPrimary in the pooler's manager) while its
// primary fails over: postgres is already a standby, or the pooler is not (or
// no longer) the elected primary. They carry no structured detail beyond
// FAILED_PRECONDITION, so they are matched by message.
var catalogPrimaryMovedMessages = []string{
	"operation not allowed: the PostgreSQL instance is in standby mode",
	"operation not allowed: this pooler is not the elected primary of the catalog shard",
}

// isCatalogPrimaryMovedError recognizes a catalog read refused because the
// pooler the gateway still takes for the catalog shard's primary is not its
// elected primary. Match both code and message: FAILED_PRECONDITION also
// covers refusals that a failover never resolves, such as a read sent to a
// shard that is not the catalog shard.
func isCatalogPrimaryMovedError(err error) bool {
	if mterrors.Code(err) != mtrpcpb.Code_FAILED_PRECONDITION {
		return false
	}
	msg := err.Error()
	for _, m := range catalogPrimaryMovedMessages {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}
