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

// Package shardingcatalog is the read model of a database's sharding catalog:
// a validated, indexed, immutable view of a shardingcatalogpb.Snapshot, which
// the catalog shard's primary produces and every gateway caches.
//
// A Snapshot is built once by New and never changes afterward, so any number
// of goroutines may read it without locking. Everything it returns, including
// slices and protos, is shared and must not be modified.
package shardingcatalog

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/keyrange"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
)

// Snapshot is a validated sharding catalog of one database.
type Snapshot struct {
	routingVersion int64
	planEpoch      int64

	// tableGroups is every tablegroup, ordered by id.
	tableGroups       []*TableGroup
	tableGroupsByID   map[int64]*TableGroup
	tableGroupsByName map[string]*TableGroup
	tables            map[tableName]*Table
}

// tableName keys Snapshot.tables. It is unexported on purpose: callers look a
// table up with Table(schema, name), so nothing outside the package needs the
// type, and exporting it would commit the package to a second qualified-name
// type next to the one the planner gets from the parser. Export it (or use the
// parser's) once a caller needs to pass a qualified name around as one value.
type tableName struct {
	schema, name string
}

// TableGroup is a tablegroup and its shards.
type TableGroup struct {
	id    int64
	name  string
	state shardingcatalogpb.TableGroupState
	// shards is every shard of the tablegroup, in any state, ordered by id.
	shards []*Shard
	// activeShards is the active shards in range order. When there are any,
	// they cover the whole keyspace without gaps or overlaps.
	activeShards []*Shard
	// shardRules is the shard rules from the tablegroup, in target range
	// order.
	shardRules []*RoutingRule
}

// Shard is one key range of a tablegroup.
type Shard struct {
	id         int64
	tableGroup *TableGroup
	name       string
	keyRange   *clustermetadatapb.KeyRange
	state      shardingcatalogpb.ShardState
}

// Table is a table placed in a tablegroup.
type Table struct {
	id         int64
	schema     string
	name       string
	tableGroup *TableGroup
	// primaryKey is the active primary shard key, nil when there is none.
	primaryKey *ShardKey
	// rule is the table rule from the table, nil when there is none.
	rule *RoutingRule
}

// ShardIndex is a named way to compute a routing identifier.
type ShardIndex struct {
	id     int64
	name   string
	typ    string
	params string
	state  shardingcatalogpb.ShardIndexState
}

// ShardKey binds a table's columns to a shard index.
type ShardKey struct {
	id         int64
	name       string
	role       shardingcatalogpb.ShardKeyRole
	state      shardingcatalogpb.ShardKeyState
	columns    []string
	shardIndex *ShardIndex
}

// RoutingRule overrides where the catalog sends traffic while its operation
// moves data. A shard rule (scope TABLEGROUP) serves the routing identifiers of
// its source tablegroup in its target shard's range from that shard; a table
// rule (scope TABLE) serves its table as a member of the target tablegroup.
type RoutingRule struct {
	id             int64
	operationID    string
	scope          shardingcatalogpb.RoutingRuleScope
	fromTableGroup *TableGroup
	// fromTable is the table a table rule moves, nil for a shard rule.
	fromTable    *Table
	toTableGroup *TableGroup
	// toShard is the target of a shard rule, nil for a table rule.
	toShard *Shard
	// toShardKeyID is the catalog id of the shard key a table rule places
	// its table by, 0 when there is none.
	toShardKeyID   int64
	createdVersion int64
}

// New validates pb and builds a Snapshot from it. It rejects a snapshot in
// which:
//
//   - an id is repeated, or a reference to a tablegroup, table or shard index
//     does not resolve;
//   - a name is repeated where the catalog makes it unique;
//   - a shard's name is not the canonical name of its key range;
//   - a tablegroup's active shards do not cover the keyspace exactly once, or
//     an active tablegroup has no active shards;
//   - the default tablegroup is missing, not active, or has any active shard
//     other than the single shard 0-inf;
//   - a table has more than one active primary shard key;
//   - a routing rule's id is repeated, a reference does not resolve, its
//     scope is unknown or does not match the fields it sets (a table rule
//     has a from_table_id and no to_shard_id, a shard rule the reverse), a
//     shard rule's target shard is not in its to_tablegroup_id, or a table
//     has more than one table rule.
//
// pb is not retained, except for the key ranges of its shards, which must not
// be modified afterward.
func New(pb *shardingcatalogpb.Snapshot) (*Snapshot, error) {
	if pb == nil {
		return nil, mterrors.NewInvalidShardingCatalogSnapshot("snapshot is nil")
	}
	s := &Snapshot{
		routingVersion:    pb.GetRoutingVersion(),
		planEpoch:         pb.GetPlanEpoch(),
		tableGroupsByID:   make(map[int64]*TableGroup, len(pb.GetTablegroups())),
		tableGroupsByName: make(map[string]*TableGroup, len(pb.GetTablegroups())),
		tables:            make(map[tableName]*Table, len(pb.GetTables())),
	}

	for _, tg := range pb.GetTablegroups() {
		if _, dup := s.tableGroupsByID[tg.GetTablegroupId()]; dup {
			return nil, mterrors.NewInvalidShardingCatalogSnapshot("tablegroup id %d is repeated", tg.GetTablegroupId())
		}
		if _, dup := s.tableGroupsByName[tg.GetName()]; dup {
			return nil, mterrors.NewInvalidShardingCatalogSnapshot("tablegroup name %q is repeated", tg.GetName())
		}
		g := &TableGroup{id: tg.GetTablegroupId(), name: tg.GetName(), state: tg.GetState()}
		s.tableGroupsByID[g.id] = g
		s.tableGroupsByName[g.name] = g
		s.tableGroups = append(s.tableGroups, g)
	}
	slices.SortFunc(s.tableGroups, func(a, b *TableGroup) int { return cmp.Compare(a.id, b.id) })

	if err := s.addShards(pb.GetShards()); err != nil {
		return nil, err
	}
	for _, g := range s.tableGroups {
		if err := g.validateLayout(); err != nil {
			return nil, err
		}
	}
	if err := s.validateDefault(); err != nil {
		return nil, err
	}

	tablesByID := make(map[int64]*Table, len(pb.GetTables()))
	for _, t := range pb.GetTables() {
		if _, dup := tablesByID[t.GetTableId()]; dup {
			return nil, mterrors.NewInvalidShardingCatalogSnapshot("table id %d is repeated", t.GetTableId())
		}
		g, ok := s.tableGroupsByID[t.GetTablegroupId()]
		if !ok {
			return nil, mterrors.NewInvalidShardingCatalogSnapshot("table %d refers to tablegroup %d, which does not exist", t.GetTableId(), t.GetTablegroupId())
		}
		key := tableName{schema: t.GetSchemaName(), name: t.GetTableName()}
		if _, dup := s.tables[key]; dup {
			return nil, mterrors.NewInvalidShardingCatalogSnapshot("table %q.%q is repeated", key.schema, key.name)
		}
		table := &Table{id: t.GetTableId(), schema: key.schema, name: key.name, tableGroup: g}
		tablesByID[table.id] = table
		s.tables[key] = table
	}

	indexes := make(map[int64]*ShardIndex, len(pb.GetShardIndices()))
	indexNames := make(map[string]struct{}, len(pb.GetShardIndices()))
	for _, si := range pb.GetShardIndices() {
		if _, dup := indexes[si.GetShardIndexId()]; dup {
			return nil, mterrors.NewInvalidShardingCatalogSnapshot("shard index id %d is repeated", si.GetShardIndexId())
		}
		if _, dup := indexNames[si.GetName()]; dup {
			return nil, mterrors.NewInvalidShardingCatalogSnapshot("shard index name %q is repeated", si.GetName())
		}
		indexNames[si.GetName()] = struct{}{}
		indexes[si.GetShardIndexId()] = &ShardIndex{
			id: si.GetShardIndexId(), name: si.GetName(), typ: si.GetType(), params: si.GetParams(), state: si.GetState(),
		}
	}

	keyIDs := make(map[int64]struct{}, len(pb.GetShardKeys()))
	for _, k := range pb.GetShardKeys() {
		if _, dup := keyIDs[k.GetShardKeyId()]; dup {
			return nil, mterrors.NewInvalidShardingCatalogSnapshot("shard key id %d is repeated", k.GetShardKeyId())
		}
		keyIDs[k.GetShardKeyId()] = struct{}{}
		table, ok := tablesByID[k.GetTableId()]
		if !ok {
			return nil, mterrors.NewInvalidShardingCatalogSnapshot("shard key %d refers to table %d, which does not exist", k.GetShardKeyId(), k.GetTableId())
		}
		index, ok := indexes[k.GetShardIndexId()]
		if !ok {
			return nil, mterrors.NewInvalidShardingCatalogSnapshot("shard key %d refers to shard index %d, which does not exist", k.GetShardKeyId(), k.GetShardIndexId())
		}
		if k.GetRole() != shardingcatalogpb.ShardKeyRole_SHARD_KEY_ROLE_PRIMARY || k.GetState() != shardingcatalogpb.ShardKeyState_SHARD_KEY_STATE_ACTIVE {
			continue
		}
		if table.primaryKey != nil {
			return nil, mterrors.NewInvalidShardingCatalogSnapshot("table %q.%q has two active primary shard keys, %d and %d",
				table.schema, table.name, table.primaryKey.id, k.GetShardKeyId())
		}
		table.primaryKey = &ShardKey{
			id:         k.GetShardKeyId(),
			name:       k.GetName(),
			role:       k.GetRole(),
			state:      k.GetState(),
			columns:    slices.Clone(k.GetColumns()),
			shardIndex: index,
		}
	}

	if err := s.addRoutingRules(pb.GetRoutingRules(), tablesByID, keyIDs); err != nil {
		return nil, err
	}
	return s, nil
}

// addRoutingRules attaches every routing rule to its source, checking ids,
// references and that each rule's scope matches the fields it sets.
func (s *Snapshot) addRoutingRules(rules []*shardingcatalogpb.RoutingRule, tablesByID map[int64]*Table, keyIDs map[int64]struct{}) error {
	shardsByID := make(map[int64]*Shard)
	for _, g := range s.tableGroups {
		for _, sh := range g.shards {
			shardsByID[sh.id] = sh
		}
	}
	ids := make(map[int64]struct{}, len(rules))
	for _, r := range rules {
		id := r.GetRuleId()
		if _, dup := ids[id]; dup {
			return mterrors.NewInvalidShardingCatalogSnapshot("routing rule id %d is repeated", id)
		}
		ids[id] = struct{}{}
		from, ok := s.tableGroupsByID[r.GetFromTablegroupId()]
		if !ok {
			return mterrors.NewInvalidShardingCatalogSnapshot("routing rule %d refers to tablegroup %d, which does not exist", id, r.GetFromTablegroupId())
		}
		to, ok := s.tableGroupsByID[r.GetToTablegroupId()]
		if !ok {
			return mterrors.NewInvalidShardingCatalogSnapshot("routing rule %d refers to tablegroup %d, which does not exist", id, r.GetToTablegroupId())
		}
		rule := &RoutingRule{
			id:             id,
			operationID:    r.GetOperationId(),
			scope:          r.GetScope(),
			fromTableGroup: from,
			toTableGroup:   to,
			createdVersion: r.GetCreatedVersion(),
		}
		if r.ToShardKeyId != nil {
			if _, ok := keyIDs[r.GetToShardKeyId()]; !ok {
				return mterrors.NewInvalidShardingCatalogSnapshot("routing rule %d refers to shard key %d, which does not exist", id, r.GetToShardKeyId())
			}
			rule.toShardKeyID = r.GetToShardKeyId()
		}

		switch r.GetScope() {
		case shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP:
			if r.FromTableId != nil || r.ToShardId == nil {
				return mterrors.NewInvalidShardingCatalogSnapshot("shard rule %d must have a to_shard_id and no from_table_id", id)
			}
			sh, ok := shardsByID[r.GetToShardId()]
			if !ok {
				return mterrors.NewInvalidShardingCatalogSnapshot("routing rule %d refers to shard %d, which does not exist", id, r.GetToShardId())
			}
			if sh.tableGroup != to {
				return mterrors.NewInvalidShardingCatalogSnapshot("shard rule %d targets shard %d of tablegroup %q, not of its to_tablegroup %q",
					id, sh.id, sh.tableGroup.name, to.name)
			}
			rule.toShard = sh
			from.shardRules = append(from.shardRules, rule)
		case shardingcatalogpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLE:
			if r.FromTableId == nil || r.ToShardId != nil {
				return mterrors.NewInvalidShardingCatalogSnapshot("table rule %d must have a from_table_id and no to_shard_id", id)
			}
			t, ok := tablesByID[r.GetFromTableId()]
			if !ok {
				return mterrors.NewInvalidShardingCatalogSnapshot("routing rule %d refers to table %d, which does not exist", id, r.GetFromTableId())
			}
			if t.rule != nil {
				return mterrors.NewInvalidShardingCatalogSnapshot("table %q.%q has two table rules, %d and %d", t.schema, t.name, t.rule.id, id)
			}
			rule.fromTable = t
			t.rule = rule
		default:
			return mterrors.NewInvalidShardingCatalogSnapshot("routing rule %d has unknown scope %s", id, r.GetScope())
		}
	}
	for _, g := range s.tableGroups {
		slices.SortFunc(g.shardRules, func(a, b *RoutingRule) int {
			return bytes.Compare(a.toShard.keyRange.GetStart(), b.toShard.keyRange.GetStart())
		})
	}
	return nil
}

// addShards attaches every shard to its tablegroup, checking ids, references
// and names.
func (s *Snapshot) addShards(shards []*shardingcatalogpb.Shard) error {
	ids := make(map[int64]struct{}, len(shards))
	for _, sh := range shards {
		if _, dup := ids[sh.GetShardId()]; dup {
			return mterrors.NewInvalidShardingCatalogSnapshot("shard id %d is repeated", sh.GetShardId())
		}
		ids[sh.GetShardId()] = struct{}{}
		g, ok := s.tableGroupsByID[sh.GetTablegroupId()]
		if !ok {
			return mterrors.NewInvalidShardingCatalogSnapshot("shard %d refers to tablegroup %d, which does not exist", sh.GetShardId(), sh.GetTablegroupId())
		}
		kr := sh.GetKeyRange()
		if kr == nil {
			kr = &clustermetadatapb.KeyRange{}
		}
		canonical := keyrange.ShardName(kr)
		if sh.GetName() != canonical {
			return mterrors.NewInvalidShardingCatalogSnapshot("shard %d of tablegroup %q is named %q, but its key range is %q", sh.GetShardId(), g.name, sh.GetName(), canonical)
		}
		// A range whose bounds are not canonical (a trailing zero byte, or a
		// start not below its end) still renders to a name, but not to one
		// that parses back to it.
		if _, err := keyrange.ParseShardName(canonical); err != nil {
			return mterrors.NewInvalidShardingCatalogSnapshot("shard %d of tablegroup %q has an invalid key range: %v", sh.GetShardId(), g.name, err)
		}
		for _, other := range g.shards {
			if other.name == sh.GetName() {
				return mterrors.NewInvalidShardingCatalogSnapshot("tablegroup %q has two shards named %q", g.name, sh.GetName())
			}
		}
		shard := &Shard{id: sh.GetShardId(), tableGroup: g, name: sh.GetName(), keyRange: kr, state: sh.GetState()}
		g.shards = append(g.shards, shard)
		if shard.state == shardingcatalogpb.ShardState_SHARD_STATE_ACTIVE {
			g.activeShards = append(g.activeShards, shard)
		}
	}
	for _, g := range s.tableGroups {
		slices.SortFunc(g.shards, func(a, b *Shard) int { return cmp.Compare(a.id, b.id) })
		slices.SortFunc(g.activeShards, func(a, b *Shard) int {
			return bytes.Compare(a.keyRange.GetStart(), b.keyRange.GetStart())
		})
	}
	return nil
}

// validateLayout checks that the tablegroup's active shards partition the
// keyspace. A tablegroup that is not active may have no active shards: one
// being created has only preparing shards.
func (g *TableGroup) validateLayout() error {
	if len(g.activeShards) == 0 {
		if g.state == shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE {
			return mterrors.NewInvalidShardingCatalogSnapshot("active tablegroup %q has no active shards", g.name)
		}
		return nil
	}
	names := make([]string, len(g.activeShards))
	for i, sh := range g.activeShards {
		names[i] = sh.name
	}
	if _, err := keyrange.ValidateShardLayout(names); err != nil {
		return mterrors.NewInvalidShardingCatalogSnapshot("active shards of tablegroup %q: %v", g.name, err)
	}
	return nil
}

// validateDefault checks that the default tablegroup, which hosts the catalog
// itself, exists, is active, and has exactly the active shard 0-inf.
func (s *Snapshot) validateDefault() error {
	g, ok := s.tableGroupsByName[constants.DefaultTableGroup]
	if !ok {
		return mterrors.NewInvalidShardingCatalogSnapshot("the %s tablegroup is missing", constants.DefaultTableGroup)
	}
	if g.state != shardingcatalogpb.TableGroupState_TABLE_GROUP_STATE_ACTIVE {
		return mterrors.NewInvalidShardingCatalogSnapshot("the %s tablegroup is %s, not active", constants.DefaultTableGroup, g.state)
	}
	if len(g.activeShards) != 1 || g.activeShards[0].name != constants.DefaultShard {
		return mterrors.NewInvalidShardingCatalogSnapshot("the %s tablegroup must have the single active shard %s", constants.DefaultTableGroup, constants.DefaultShard)
	}
	return nil
}

// RoutingVersion is the catalog's routing_version when the snapshot was read.
func (s *Snapshot) RoutingVersion() int64 { return s.routingVersion }

// PlanEpoch is the catalog's plan_epoch when the snapshot was read.
func (s *Snapshot) PlanEpoch() int64 { return s.planEpoch }

// TableGroups returns every tablegroup, in any state, ordered by id.
func (s *Snapshot) TableGroups() []*TableGroup { return s.tableGroups }

// TableGroupByName returns the tablegroup with the given name, in any state.
func (s *Snapshot) TableGroupByName(name string) (*TableGroup, bool) {
	g, ok := s.tableGroupsByName[name]
	return g, ok
}

// TableGroupByID returns the tablegroup with the given catalog id, in any
// state.
func (s *Snapshot) TableGroupByID(id int64) (*TableGroup, bool) {
	g, ok := s.tableGroupsByID[id]
	return g, ok
}

// Table returns the table with the given schema and name. Names are matched
// exactly, as stored in the catalog.
func (s *Snapshot) Table(schema, name string) (*Table, bool) {
	t, ok := s.tables[tableName{schema: schema, name: name}]
	return t, ok
}

// ID is the tablegroup's catalog id.
func (g *TableGroup) ID() int64 { return g.id }

// Name is the tablegroup's name.
func (g *TableGroup) Name() string { return g.name }

// State is the tablegroup's lifecycle state.
func (g *TableGroup) State() shardingcatalogpb.TableGroupState { return g.state }

// Shards returns every shard of the tablegroup, in any state, ordered by id.
func (g *TableGroup) Shards() []*Shard { return g.shards }

// ActiveShards returns the tablegroup's active shards in range order. When
// there are any, they cover the whole keyspace exactly once.
func (g *TableGroup) ActiveShards() []*Shard { return g.activeShards }

// ShardRules returns the shard rules from the tablegroup, ordered by the start
// of their target shards' ranges. It is empty when no rule routes the
// tablegroup.
func (g *TableGroup) ShardRules() []*RoutingRule { return g.shardRules }

// IsSharded reports whether the tablegroup has more than one active shard.
// A tablegroup with no active shards yet (one being created) is not sharded.
func (g *TableGroup) IsSharded() bool { return len(g.activeShards) > 1 }

// ShardFor returns the active shard whose key range contains the routing
// identifier id: start <= id < end, comparing bytewise, with an empty start or
// end unbounded. It returns false only when the tablegroup has no active
// shards, since active shards cover the whole keyspace.
func (g *TableGroup) ShardFor(id []byte) (*Shard, bool) {
	// The first shard whose start is above id; the one before it is the
	// last whose start is at or below id. The first active shard starts at
	// the start of the keyspace, so for a non-empty layout that shard exists.
	i, _ := slices.BinarySearchFunc(g.activeShards, id, func(sh *Shard, id []byte) int {
		if bytes.Compare(sh.keyRange.GetStart(), id) <= 0 {
			return -1
		}
		return 1
	})
	if i == 0 {
		return nil, false
	}
	return g.activeShards[i-1], true
}

// ID is the shard's catalog id.
func (sh *Shard) ID() int64 { return sh.id }

// TableGroup is the tablegroup the shard belongs to.
func (sh *Shard) TableGroup() *TableGroup { return sh.tableGroup }

// Name is the shard's canonical name, e.g. "0-80".
func (sh *Shard) Name() string { return sh.name }

// KeyRange is the range of routing identifiers the shard owns.
func (sh *Shard) KeyRange() *clustermetadatapb.KeyRange { return sh.keyRange }

// State is the shard's lifecycle state.
func (sh *Shard) State() shardingcatalogpb.ShardState { return sh.state }

// ID is the table's catalog id.
func (t *Table) ID() int64 { return t.id }

// Schema is the table's schema name.
func (t *Table) Schema() string { return t.schema }

// Name is the table's name.
func (t *Table) Name() string { return t.name }

// TableGroup is the tablegroup the table is placed in.
func (t *Table) TableGroup() *TableGroup { return t.tableGroup }

// PrimaryShardKey returns the table's active primary shard key, or nil when
// it has none.
func (t *Table) PrimaryShardKey() *ShardKey { return t.primaryKey }

// RoutingRule returns the table rule from the table, or nil when it has none.
func (t *Table) RoutingRule() *RoutingRule { return t.rule }

// ID is the shard key's catalog id.
func (k *ShardKey) ID() int64 { return k.id }

// Name is the shard key's name, unique within its table.
func (k *ShardKey) Name() string { return k.name }

// Role is the shard key's role.
func (k *ShardKey) Role() shardingcatalogpb.ShardKeyRole { return k.role }

// State is the shard key's lifecycle state.
func (k *ShardKey) State() shardingcatalogpb.ShardKeyState { return k.state }

// Columns returns the key's column names in key order.
func (k *ShardKey) Columns() []string { return k.columns }

// ShardIndex is the shard index the key computes its routing identifier with.
func (k *ShardKey) ShardIndex() *ShardIndex { return k.shardIndex }

// ID is the shard index's catalog id.
func (si *ShardIndex) ID() int64 { return si.id }

// Name is the shard index's name, unique within the database.
func (si *ShardIndex) Name() string { return si.name }

// Type is the shard index type, e.g. "hash".
func (si *ShardIndex) Type() string { return si.typ }

// Params is the type's settings as a JSON object.
func (si *ShardIndex) Params() string { return si.params }

// State is the shard index's lifecycle state.
func (si *ShardIndex) State() shardingcatalogpb.ShardIndexState { return si.state }

// ID is the rule's catalog id.
func (r *RoutingRule) ID() int64 { return r.id }

// OperationID is the uuid of the operation the rule belongs to.
func (r *RoutingRule) OperationID() string { return r.operationID }

// Scope says whether the rule is a shard rule or a table rule.
func (r *RoutingRule) Scope() shardingcatalogpb.RoutingRuleScope { return r.scope }

// FromTableGroup is the tablegroup whose traffic the rule routes.
func (r *RoutingRule) FromTableGroup() *TableGroup { return r.fromTableGroup }

// FromTable is the table a table rule moves, nil for a shard rule.
func (r *RoutingRule) FromTable() *Table { return r.fromTable }

// ToTableGroup is the tablegroup the rule routes to.
func (r *RoutingRule) ToTableGroup() *TableGroup { return r.toTableGroup }

// ToShard is the shard a shard rule routes to, nil for a table rule.
func (r *RoutingRule) ToShard() *Shard { return r.toShard }

// ToShardKeyID is the catalog id of the shard key a table rule places its
// table by, 0 when there is none.
func (r *RoutingRule) ToShardKeyID() int64 { return r.toShardKeyID }

// CreatedVersion is the routing_version the rule was written at.
func (r *RoutingRule) CreatedVersion() int64 { return r.createdVersion }
