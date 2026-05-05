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

package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// TestUserAuthDebugRedactAnnotation pins `[debug_redact = true]` on every
// UserAuth field. The bytes are SCRAM passthrough keys — password-equivalent —
// so any future proto edit that drops the annotation must be deliberate, not
// accidental. Today google.golang.org/protobuf (v1.36) does not honor
// debug_redact in prototext / String(); upstream tracks this at
// https://github.com/golang/protobuf/issues/1655. Once that lands, the
// annotation will start producing redacted String() output for free.
func TestUserAuthDebugRedactAnnotation(t *testing.T) {
	md := (&UserAuth{}).ProtoReflect().Descriptor()
	for _, name := range []protoreflect.Name{"client_key", "server_key"} {
		fd := md.Fields().ByName(name)
		require.NotNil(t, fd, "UserAuth.%s field missing", name)
		opts, ok := fd.Options().(*descriptorpb.FieldOptions)
		require.True(t, ok, "UserAuth.%s has no FieldOptions", name)
		assert.True(t, opts.GetDebugRedact(),
			"UserAuth.%s must carry [debug_redact = true]; the bytes are password-equivalent",
			name)
	}
}
