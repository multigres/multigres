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

// Package openapitest loads the checked-in Multiadmin REST contract for tests.
package openapitest

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/pb33f/libopenapi"
	validator "github.com/pb33f/libopenapi-validator"
	"github.com/stretchr/testify/require"
)

// Load reads docs/api/multiadmin.openapi.yaml.
func Load(t *testing.T) libopenapi.Document {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	data, err := os.ReadFile(filepath.Join(filepath.Dir(source), "../../../../docs/api/multiadmin.openapi.yaml"))
	require.NoError(t, err)
	doc, err := libopenapi.NewDocument(data)
	require.NoError(t, err)
	return doc
}

// Validator builds a response validator from the checked-in contract.
func Validator(t *testing.T) validator.Validator {
	t.Helper()
	v, errs := validator.NewValidator(Load(t))
	require.Empty(t, errs)
	return v
}
