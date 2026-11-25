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

package clustersetup

// OTLPTraceEnv is the environment that makes a process export every trace to
// the OTLP HTTP endpoint, such as a test collector's.
func OTLPTraceEnv(endpoint string) []string {
	return []string{
		"OTEL_TRACES_EXPORTER=otlp",
		"OTEL_EXPORTER_OTLP_ENDPOINT=" + endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_TRACES_SAMPLER=always_on",
	}
}
