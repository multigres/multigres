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

package shardindex

import (
	"encoding/json"
	"slices"
	"sync"

	"github.com/multigres/multigres/go/common/mterrors"
)

// Factory builds an instance of an implementation from its catalog name and
// parameters. It must reject parameters it does not know; CheckParams does
// that for implementations with a fixed set of names.
type Factory func(name string, params map[string]string) (ShardIndex, error)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]Factory)
)

// Register makes an implementation available under the type name typ. Call it
// from the implementation's init. It panics when typ is empty, f is nil or typ
// is already registered: a type name identifies one permanent mapping, so two
// implementations claiming it is a build error, not a runtime condition.
func Register(typ string, f Factory) {
	if typ == "" {
		panic("shardindex: Register with an empty type")
	}
	if f == nil {
		panic("shardindex: Register of type " + typ + " with a nil factory")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[typ]; dup {
		panic("shardindex: type " + typ + " is already registered")
	}
	registry[typ] = f
}

// New builds the shard index name of the registered type typ. It fails when
// the type is not registered in this binary or the factory rejects params.
func New(typ, name string, params map[string]string) (ShardIndex, error) {
	registryMu.RLock()
	f, ok := registry[typ]
	registryMu.RUnlock()
	if !ok {
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSUndefinedObject,
			"shard index type \""+typ+"\" does not exist",
			"Shard index \""+name+"\" needs an implementation that is not built into this binary.")
	}
	return f(name, params)
}

// Types returns the registered type names, sorted.
func Types() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	types := make([]string, 0, len(registry))
	for typ := range registry {
		types = append(types, typ)
	}
	slices.Sort(types)
	return types
}

// CheckParams rejects every parameter of the shard index name, of type typ,
// whose name is not in known. An unknown parameter is an error rather than a
// warning: a misspelt parameter silently ignored would place rows by a mapping
// the user did not ask for, permanently.
func CheckParams(typ, name string, params map[string]string, known ...string) error {
	var unknown []string
	for param := range params {
		if !slices.Contains(known, param) {
			unknown = append(unknown, param)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	// Report the same parameter whatever the map's iteration order.
	slices.Sort(unknown)
	return mterrors.NewPgError("ERROR", mterrors.PgSSInvalidParameterValue,
		"unrecognized parameter \""+unknown[0]+"\" for shard index \""+name+"\" of type \""+typ+"\"", "")
}

// ParseParams decodes the catalog's parameters (multigres.shard_index.params,
// a JSON object served as text) into the string map New takes. Every value
// must be a JSON string; an empty text is an empty object.
func ParseParams(text string) (map[string]string, error) {
	if text == "" {
		return map[string]string{}, nil
	}
	// A map of strings alone would also accept null, for the object and for
	// any value, so decode to raw messages and check each one.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(text), &raw); err != nil || raw == nil {
		return nil, mterrors.NewPgError("ERROR", mterrors.PgSSInvalidParameterValue,
			"shard index parameters must be a JSON object", "")
	}
	params := make(map[string]string, len(raw))
	for name, value := range raw {
		var s *string
		if err := json.Unmarshal(value, &s); err != nil || s == nil {
			return nil, mterrors.NewPgError("ERROR", mterrors.PgSSInvalidParameterValue,
				"shard index parameter \""+name+"\" must be a JSON string", "")
		}
		params[name] = *s
	}
	return params, nil
}
