// Copyright 2026 Supabase, Inc.
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

package local

import (
	"errors"
	"os"
	"syscall"
)

// serviceStateErrorReason returns only fixed diagnostic labels. State errors
// can embed config-derived paths or arbitrary JSON marshaler error text; never
// print their Error() strings in progress output.
func serviceStateErrorReason(err error) string {
	switch {
	case errors.Is(err, os.ErrPermission):
		return "permission denied; check state directory permissions"
	case errors.Is(err, syscall.ENOSPC):
		return "no space left; check state filesystem capacity"
	case errors.Is(err, os.ErrNotExist):
		return "missing state directory"
	default:
		return "state unavailable or invalid; check state directory and configuration"
	}
}
