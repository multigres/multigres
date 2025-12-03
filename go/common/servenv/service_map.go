// Copyright 2019 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package servenv

import (
	"fmt"
	"log/slog"
)

// InitServiceMap will set the default value for a protocol/name to be served.
func (sv *ServEnv) InitServiceMap(protocol, name string) {
	sv.serviceMap[protocol+"-"+name] = true
}

// updateServiceMap takes the command line parameter, and updates the
// ServiceMap accordingly
func (sv *ServEnv) updateServiceMap() {
	for _, s := range sv.serviceMapFlag.Get() {
		if s[0] == '-' {
			delete(sv.serviceMap, s[1:])
		} else {
			sv.serviceMap[s] = true
		}
	}
}

// checkServiceMap returns if we should register a RPC service
// (and also logs how to enable / disable it)
func (sv *ServEnv) checkServiceMap(protocol, name string) bool {
	if sv.serviceMap[protocol+"-"+name] {
		slog.Info(fmt.Sprintf("Registering %v for %v, disable it with -%v-%v service_map parameter", name, protocol, protocol, name))
		return true
	}
	slog.Info(fmt.Sprintf("Not registering %v for %v, enable it with %v-%v service_map parameter", name, protocol, protocol, name))
	return false
}
