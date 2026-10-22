// Copyright 2025 Supabase, Inc.
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

package constants

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsCatalogShard(t *testing.T) {
	assert.True(t, IsCatalogShard(DefaultTableGroup, DefaultShard))
	assert.False(t, IsCatalogShard(DefaultTableGroup, "0-80"))
	assert.False(t, IsCatalogShard("commerce", DefaultShard))
	assert.False(t, IsCatalogShard("commerce", "0-80"))
	assert.False(t, IsCatalogShard("", ""))
}

func TestDefaultConstants(t *testing.T) {
	// Ensure the default constants have the expected values
	assert.Equal(t, "default", DefaultTableGroup)
	assert.Equal(t, "0-inf", DefaultShard)
	assert.Equal(t, "0", ShardNameMinBound)
	assert.Equal(t, "inf", ShardNameMaxBound)
}
