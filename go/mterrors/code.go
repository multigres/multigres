// Copyright 2022 The Vitess Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Modifications Copyright 2025 Supabase, Inc.

package mterrors

import (
	"fmt"
	"strings"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// Errors added to the list of variables below must be added to the Errors slice a little below in this same file.
// This will enable the auto-documentation of error code in the website repository.

var (
	// MT13001 General Error
	MT13001 = errorWithoutState("MT13001", mtrpcpb.Code_INTERNAL, "[BUG] %s", "This error should not happen and is a bug. Please file an issue on GitHub: https://github.com/multigres/multigres/issues/new/choose.")

	// Errors is a list of errors that must match all the variables
	// defined above to enable auto-documentation of error codes.
	Errors = []func(args ...any) *MultigresError{
		MT13001,
	}

	ErrorsWithNoCode = []func(code mtrpcpb.Code, args ...any) *MultigresError{}
)

type MultigresError struct {
	Err         error
	Description string
	ID          string
	State       State
}

func (o *MultigresError) Error() string {
	return o.Err.Error()
}

func (o *MultigresError) Cause() error {
	return o.Err
}

var _ error = (*MultigresError)(nil)

// errorWithoutState is an error that does not have any state, e.g. the state will be unknown
func errorWithoutState(id string, code mtrpcpb.Code, short, long string) func(args ...any) *MultigresError {
	return func(args ...any) *MultigresError {
		s := short
		if len(args) != 0 {
			s = fmt.Sprintf(s, args...)
		}

		return &MultigresError{
			Err:         New(code, id+": "+s),
			Description: long,
			ID:          id,
		}
	}
}

func errorWithState(id string, code mtrpcpb.Code, state State, short, long string) func(args ...any) *MultigresError {
	return func(args ...any) *MultigresError {
		var err error
		if len(args) != 0 {
			err = NewErrorf(code, state, id+": "+short, args...)
		} else {
			err = NewError(code, state, id+": "+short)
		}

		return &MultigresError{
			Err:         err,
			Description: long,
			ID:          id,
			State:       state,
		}
	}
}

// ErrorWithNoCode refers to error code that do not have a predefined error code.
type ErrorWithNoCode func(code mtrpcpb.Code, args ...any) *MultigresError

// errorWithNoCode creates a MultigresError where the error code is set by the user when creating the error
// instead of having a static error code that is declared in this file.
func errorWithNoCode(id string, short, long string) func(code mtrpcpb.Code, args ...any) *MultigresError {
	return func(code mtrpcpb.Code, args ...any) *MultigresError {
		s := short
		if len(args) != 0 {
			s = fmt.Sprintf(s, args...)
		}

		return &MultigresError{
			Err:         New(code, id+": "+s),
			Description: long,
			ID:          id,
		}
	}
}

func IsError(err error, code string) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), code)
}
