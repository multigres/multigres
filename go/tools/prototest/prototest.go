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

// Package prototest provides test helpers for asserting equality of protobuf messages.
package prototest

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// RequireEqual asserts that two proto messages are equal using proto.Equal semantics.
func RequireEqual(t *testing.T, expected, actual proto.Message, msgAndArgs ...any) {
	t.Helper()
	if !proto.Equal(expected, actual) {
		diff := cmp.Diff(expected, actual, protocmp.Transform())
		t.Fatalf("proto messages not equal%s\ndiff (-want +got):\n%s", formatMsg(msgAndArgs), diff)
	}
}

// RequireElementsMatch asserts that two slices of proto messages contain the same elements
// regardless of order, using proto.Equal semantics for comparison.
func RequireElementsMatch[T proto.Message](t *testing.T, expected, actual []T, msgAndArgs ...any) {
	t.Helper()

	if len(expected) != len(actual) {
		t.Fatalf("proto slices have different lengths: expected %d, got %d%s\ndiff (-want +got):\n%s",
			len(expected), len(actual), formatMsg(msgAndArgs),
			cmp.Diff(expected, actual, protocmp.Transform()))
		return
	}

	used := make([]bool, len(actual))
	for _, exp := range expected {
		found := false
		for j, act := range actual {
			if !used[j] && proto.Equal(exp, act) {
				used[j] = true
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("proto slices do not match%s\ndiff (-want +got):\n%s",
				formatMsg(msgAndArgs),
				cmp.Diff(expected, actual, protocmp.Transform()))
			return
		}
	}
}

func formatMsg(msgAndArgs []any) string {
	if len(msgAndArgs) == 0 {
		return ""
	}
	if len(msgAndArgs) == 1 {
		return fmt.Sprintf(": %v", msgAndArgs[0])
	}
	return ": " + fmt.Sprintf(fmt.Sprintf("%v", msgAndArgs[0]), msgAndArgs[1:]...)
}
