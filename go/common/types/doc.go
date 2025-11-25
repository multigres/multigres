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

// Package types contains plain Go structs generated from protobuf definitions.
//
// These structs are immutable-friendly alternatives to protobuf messages,
// enabling safe concurrent access without deep copies. They mirror the
// protobuf definitions exactly but use value types instead of pointers.
//
// Generated files:
//   - clustermetadata_gen.go: structs from clustermetadata.proto
//
// To regenerate after proto changes:
//
//	make generate-types
//
// Or directly:
//
//	go generate ./go/common/types/...
package types

//go:generate go run ../../../tools/proto-to-struct -proto=clustermetadata.proto -output=. -package=types
