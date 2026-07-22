// Copyright 2025 Supabase, Inc.
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

package mterrors

import mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"

// ErrorWithCode is an interface for errors that have an associated error code
type ErrorWithCode interface {
	error
	ErrorCode() mtrpcpb.Code
}

// codedError associates an RPC error code with an existing error while
// preserving the original error chain for errors.Is and errors.As.
type codedError struct {
	err  error
	code mtrpcpb.Code
}

func (e *codedError) Error() string           { return e.err.Error() }
func (e *codedError) Unwrap() error           { return e.err }
func (e *codedError) ErrorCode() mtrpcpb.Code { return e.code }

// WithCode associates code with err without replacing err or obscuring its
// structured causes. It returns nil when err is nil.
func WithCode(err error, code mtrpcpb.Code) error {
	if err == nil {
		return nil
	}
	return &codedError{err: err, code: code}
}
