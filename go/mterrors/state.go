// Copyright 2025 The Multigres Authors.
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

// State represents an error state that we will later use to mimic PostgreSQL errors
type State int

// All the error states
const (
	// Undefined is the default error state for errors that don't have a specific state
	Undefined State = iota
	// invalid argument
	BadFieldError
	// We can keep porting errors from Vitess here as we go.
)

// ErrorWithCode is an interface for errors that have an associated error code
type ErrorWithCode interface {
	error
	ErrorCode() mtrpcpb.Code
}

// ErrorWithState is an interface for errors that have an associated state
type ErrorWithState interface {
	error
	ErrorState() State
}
