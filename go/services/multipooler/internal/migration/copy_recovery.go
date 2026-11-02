// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

// copyRecoveryPublication is private to this migration. The original publication
// can have other subscribers, so recovery must never remove tables from it.
func copyRecoveryPublication(m *Migration) string { return m.PublicationName() + "_recopy" }

func copySyncSlot(s *CopySnapshot, r CopyRelation) string {
	return fmt.Sprintf("pg_%d_sync_%d_%s", s.SubscriptionOID, r.OID, s.SystemID)
}

// startCopyRecovery distinguishes a missing-slot failure from the normal window
// between dropping the slot and committing SYNCDONE. The supported ALTER
// SUBSCRIPTION lock waits for that transaction, then takes a fresh snapshot.
// A lock timeout only defers the diagnosis. It never justifies a new copy.
func (c *Coordinator) startCopyRecovery(ctx context.Context, m *Migration) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if !m.CopyData {
		return nil
	}
	initial, err := c.target.CopySnapshot(ctx, m.SubscriptionName())
	if err != nil {
		return err
	}
	if initial == nil || !slices.ContainsFunc(initial.Relations, func(r CopyRelation) bool { return r.State == "f" }) {
		return nil
	}
	src, err := c.openSource(ctx, m)
	if err != nil {
		return err
	}
	defer src.close()
	var diagnosed *CopyRecovery
	err = c.target.WithCopyLock(ctx, m.SubscriptionName(), func(s *CopySnapshot) error {
		if err := checkCopySnapshot(m, s, nil); err != nil {
			return err
		}
		if !slices.Equal(s.Publications, []string{m.PublicationName()}) {
			return errors.New("cannot recover table copy: subscription publications changed")
		}
		var slots []string
		for _, r := range s.Relations {
			if r.State == "f" {
				slots = append(slots, copySyncSlot(s, r))
			}
		}
		if len(slots) == 0 {
			return nil
		}
		present, err := src.ReadCopySlots(s.SlotName, slots)
		if err != nil {
			return err
		}
		recovery := &CopyRecovery{Stage: CopyRecoveryDetach, SubscriptionOID: s.SubscriptionOID, SystemID: s.SystemID}
		for _, r := range s.Relations {
			switch {
			case r.State == "f" && !present[copySyncSlot(s, r)]:
				recovery.Tables = append(recovery.Tables, CopyRecoveryTable{Name: r.Name, OID: r.OID})
			case r.State != "r":
				// Finish healthy copies first. This also lets us repair all broken
				// tables together rather than waiting forever for each other.
				return nil
			}
		}
		if len(recovery.Tables) == 0 {
			return nil
		}
		if err := src.ValidateCopyPublication(m.PublicationName(), m.Tables); err != nil {
			return err
		}
		if m.SkipSchemaCopy {
			return errors.New("automatic table copy recovery requires a Multigres-created target schema")
		}
		diagnosed = recovery
		return nil
	})
	if errors.Is(err, ErrCopyLockBusy) {
		return nil
	}
	if err != nil || diagnosed == nil {
		return err
	}
	// Release the diagnostic transaction before Store.Update borrows another
	// admin connection. FINISHEDCOPY cannot resume after its slot disappears.
	if m.CopyRecovery != nil {
		// The previous temporary publication can contain a different set when
		// only some replacement copies failed. It is unused in WAIT.
		if err := src.DropPublication(copyRecoveryPublication(m)); err != nil {
			return err
		}
	}
	if err := c.saveCopyRecovery(ctx, m, diagnosed); err != nil {
		return err
	}
	c.logger.WarnContext(ctx, "restarting table copies after missing synchronization slot", "migration", m.ID, "tables", diagnosed.Tables)
	c.journal(ctx, m, JournalEventPhase, fmt.Sprintf("restart table copies after missing synchronization slot: %v", diagnosed.Tables))
	return nil
}

// reconcileCopyRecovery performs at most one durable step per reconciliation.
// DETACH can repeat a truncate only while the table is unsubscribed. ATTACH is
// saved after that truncate commits, so a promoted standby never truncates a
// table after starting its replacement copy. WAIT never truncates anything.
func (c *Coordinator) reconcileCopyRecovery(ctx context.Context, m *Migration) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	recovery := m.CopyRecovery
	if recovery == nil {
		return nil
	}
	if m.Phase != PhaseCopying || !m.CopyData || m.SkipSchemaCopy {
		return errors.New("cannot recover table copy outside an initial data copy")
	}
	s, err := c.target.CopySnapshot(ctx, m.SubscriptionName())
	if err != nil {
		return err
	}
	if err := checkCopySnapshot(m, s, recovery); err != nil {
		return err
	}
	original, temporary := m.PublicationName(), copyRecoveryPublication(m)
	if len(s.Publications) != 1 || (s.Publications[0] != original && s.Publications[0] != temporary) {
		return errors.New("cannot recover table copy: subscription publications changed")
	}
	src, err := c.openSource(ctx, m)
	if err != nil {
		return err
	}
	defer src.close()
	if err := src.ValidateCopyPublication(original, m.Tables); err != nil {
		return err
	}
	if _, err := src.ReadCopySlots(s.SlotName, nil); err != nil {
		return err
	}
	switch recovery.Stage {
	case CopyRecoveryDetach:
		keep := make([]string, 0, len(m.Tables))
		for _, name := range m.Tables {
			if !slices.ContainsFunc(recovery.Tables, func(t CopyRecoveryTable) bool { return t.Name == name }) {
				keep = append(keep, name)
			}
		}
		if err := src.EnsureCopyPublication(temporary, keep); err != nil {
			return err
		}
		if err := c.target.SetSubscriptionPublications(ctx, m.SubscriptionName(), temporary, recovery); err != nil {
			return err
		}
		if err := c.target.TruncateCopyTables(ctx, m.SubscriptionName(), recovery); err != nil {
			return err
		}
		next := *recovery
		next.Stage = CopyRecoveryAttach
		return c.saveCopyRecovery(ctx, m, &next)
	case CopyRecoveryAttach:
		if err := c.target.SetSubscriptionPublications(ctx, m.SubscriptionName(), original, recovery); err != nil {
			return err
		}
		next := *recovery
		next.Stage = CopyRecoveryWait
		return c.saveCopyRecovery(ctx, m, &next)
	case CopyRecoveryWait:
		if !slices.Equal(s.Publications, []string{original}) {
			return errors.New("cannot finish table copy recovery: original publication is not attached")
		}
		if len(s.Relations) != len(m.Tables) {
			return errors.New("cannot finish table copy recovery: subscription table set changed")
		}
		for _, r := range s.Relations {
			if r.State != "r" {
				// A second failover can hit the same upstream bug during the new
				// copy. Diagnose it with the same lock barrier before retrying.
				return c.startCopyRecovery(ctx, m)
			}
		}
		if err := src.DropPublication(temporary); err != nil {
			return err
		}
		if err := c.saveCopyRecovery(ctx, m, nil); err != nil {
			return err
		}
		c.journal(ctx, m, JournalEventPhase, "table copy recovery completed")
		return nil
	default:
		return fmt.Errorf("unknown table copy recovery stage %q", recovery.Stage)
	}
}

func (c *Coordinator) saveCopyRecovery(ctx context.Context, m *Migration, recovery *CopyRecovery) error {
	next := *m
	next.CopyRecovery = recovery
	if err := c.store.Update(ctx, &next); err != nil {
		return fmt.Errorf("save table copy recovery: %w", err)
	}
	*m = next
	return nil
}

// checkCopySnapshot refuses drift in subscription identity, the system ID, or
// the table set. A missing selected row is expected only during DETACH/ATTACH.
func checkCopySnapshot(m *Migration, s *CopySnapshot, recovery *CopyRecovery) error {
	if s == nil || !s.Enabled || s.SubscriptionOID == 0 || s.SystemID == "" || s.SlotName != m.SubscriptionName() {
		return errors.New("cannot recover table copy: subscription identity or configuration changed")
	}
	if recovery != nil && (s.SubscriptionOID != recovery.SubscriptionOID || s.SystemID != recovery.SystemID) {
		return errors.New("cannot recover table copy: subscription identity changed")
	}
	if recovery != nil {
		selected := make(map[string]bool)
		for _, table := range recovery.Tables {
			if !slices.Contains(m.Tables, table.Name) || selected[table.Name] || table.OID == 0 {
				return fmt.Errorf("cannot recover table copy: invalid selected relation %q", table.Name)
			}
			selected[table.Name] = true
		}
	}
	seen := make(map[string]bool)
	for _, r := range s.Relations {
		if !slices.Contains(m.Tables, r.Name) || seen[r.Name] || !r.Ordinary {
			return fmt.Errorf("cannot recover table copy: unexpected or unsupported relation %q", r.Name)
		}
		seen[r.Name] = true
		selected := false
		if recovery != nil {
			for _, t := range recovery.Tables {
				if t.Name == r.Name {
					selected = true
					if recovery.Stage == CopyRecoveryDetach && r.State != "f" {
						return fmt.Errorf("cannot detach table copy: relation %q left FINISHEDCOPY", r.Name)
					}
					if t.OID != r.OID {
						return fmt.Errorf("cannot recover table copy: relation %q was replaced", r.Name)
					}
				}
			}
			if !selected && r.State != "r" {
				return fmt.Errorf("cannot recover table copy: another table is still copying: %q", r.Name)
			}
		}
	}
	for _, name := range m.Tables {
		if seen[name] {
			continue
		}
		if recovery == nil || recovery.Stage == CopyRecoveryWait || !slices.ContainsFunc(recovery.Tables, func(t CopyRecoveryTable) bool { return t.Name == name }) {
			return fmt.Errorf("cannot recover table copy: subscription relation %q is missing", name)
		}
	}
	return nil
}
