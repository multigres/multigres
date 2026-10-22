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

package main

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// halves name each half's registry view: a half's settings are written as
// multigateway.<name> or multipooler.<name> in the config file.
var halves = []string{constants.ServiceMultigateway, constants.ServiceMultipooler}

// prepareConfig checks every load and reload of the config file: it copies the
// shared settings into both halves and rejects keys nothing reads.
func prepareConfig(reg *viperutil.Registry) viperutil.ConfigPreparer {
	return func(settings map[string]any) (map[string]any, error) {
		known := make(map[string]bool)
		for _, key := range reg.Keys() {
			known[key] = true
		}
		omitted := make(map[string]bool)
		for _, name := range omittedPoolerFlags {
			omitted[reg.WithPrefix(constants.ServiceMultipooler).Key(name)] = true
		}

		extra := make(map[string]any)
		var errs []error
		for _, key := range slices.Sorted(maps.Keys(settings)) {
			val := settings[key]
			half, name, inHalf := strings.Cut(key, ".")
			inHalf = inHalf && slices.Contains(halves, half)
			switch {
			case slices.Contains(sharedComponentFlags, key):
				for _, h := range halves {
					extra[reg.WithPrefix(h).Key(key)] = val
				}
			case inHalf && slices.Contains(sharedComponentFlags, name):
				if top, ok := settings[name]; !ok || fmt.Sprint(top) != fmt.Sprint(val) {
					errs = append(errs, fmt.Errorf("%s: %s is shared by both halves; set it once as %s", key, name, name))
				}
			case omitted[key]:
				errs = append(errs, fmt.Errorf("%s: not available in minigres", key))
			case !known[key]:
				errs = append(errs, fmt.Errorf("%s: unknown setting", key))
			}
		}
		if len(errs) > 0 {
			return nil, errors.Join(errs...)
		}
		return extra, nil
	}
}

// checkEnvClashes fails if an environment variable is read by more than one
// setting, unless they are one shared setting read by both halves.
func checkEnvClashes(reg *viperutil.Registry) error {
	var errs []error
	env := reg.EnvVars()
	for _, name := range slices.Sorted(maps.Keys(env)) {
		keys := env[name]
		if len(keys) < 2 || sharedByHalves(keys) {
			continue
		}
		slices.Sort(keys)
		errs = append(errs, fmt.Errorf("environment variable %s is read by %s", name, strings.Join(keys, " and ")))
	}
	return errors.Join(errs...)
}

// sharedByHalves reports whether keys are one shared setting as each half
// names it.
func sharedByHalves(keys []string) bool {
	var shared string
	for _, key := range keys {
		half, name, ok := strings.Cut(key, ".")
		if !ok || !slices.Contains(halves, half) || !slices.Contains(sharedComponentFlags, name) || (shared != "" && name != shared) {
			return false
		}
		shared = name
	}
	return true
}
