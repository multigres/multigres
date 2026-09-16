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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testDSN = "host=src.example.com port=5432 user=repl password=s3cr3t dbname=appdb sslmode=disable"

func TestRedactDSN(t *testing.T) {
	got := redactDSN(testDSN)
	require.Equal(t, "src.example.com:5432/appdb", got)
	require.NotContains(t, got, "s3cr3t", "redaction must drop the password")
	require.NotContains(t, got, "repl", "redaction must drop the user")

	// A DSN that cannot be parsed yields empty rather than risk leaking anything.
	require.Equal(t, "", redactDSN("host=h port=notaport dbname=d"))
}

func TestSameSourceDatabase(t *testing.T) {
	base := "host=h1 port=5432 user=u password=p dbname=appdb sslmode=disable"

	// Host/port/user/password/TLS may change; the database may not.
	require.NoError(t, sameSourceDatabase(base,
		"host=h2 port=5433 user=u2 password=p2 dbname=appdb sslmode=require"))

	err := sameSourceDatabase(base, "host=h1 port=5432 user=u password=p dbname=other sslmode=disable")
	require.Error(t, err)
	require.Contains(t, err.Error(), "database change is not allowed")

	require.Error(t, sameSourceDatabase("garbage ===", base), "unparseable old DSN")
	require.Error(t, sameSourceDatabase(base, "garbage ==="), "unparseable new DSN")
}

func TestPhaseRank(t *testing.T) {
	// Setup phases sort in linear order so resume can compare progress.
	require.Less(t, phaseRank(PhaseCreated), phaseRank(PhaseValidating))
	require.Less(t, phaseRank(PhaseValidating), phaseRank(PhaseSchemaCopy))
	require.Less(t, phaseRank(PhaseSchemaCopy), phaseRank(PhaseCreatePublication))
	require.Less(t, phaseRank(PhaseCreatePublication), phaseRank(PhaseCopying))
	require.Less(t, phaseRank(PhaseCopying), phaseRank(PhaseImporting))
	// Both steady streaming states share the same (highest linear) rank.
	require.Equal(t, phaseRank(PhaseImporting), phaseRank(PhaseExporting))
	// Non-linear phases sort high so they never re-run setup.
	require.Equal(t, 100, phaseRank(PhaseSwitchingToExport))
	require.Equal(t, 100, phaseRank(PhaseSwitchingToImport))
	require.Equal(t, 100, phaseRank(PhaseFailed))
}

func TestDirectionHelpers(t *testing.T) {
	// directionOf gives the current publisher; during a switch it is the side
	// still being drained (SWITCHING_TO_EXPORT is still importing).
	require.Equal(t, DirectionImport, directionOf(PhaseImporting))
	require.Equal(t, DirectionImport, directionOf(PhaseCopying))
	require.Equal(t, DirectionImport, directionOf(PhaseSwitchingToExport))
	require.Equal(t, DirectionExport, directionOf(PhaseExporting))
	require.Equal(t, DirectionExport, directionOf(PhaseSwitchingToImport))

	require.True(t, isStreaming(PhaseImporting))
	require.True(t, isStreaming(PhaseExporting))
	require.False(t, isStreaming(PhaseCopying))
	require.False(t, isStreaming(PhaseSwitchingToExport))

	require.Equal(t, PhaseSwitchingToExport, switchingPhase(DirectionExport))
	require.Equal(t, PhaseSwitchingToImport, switchingPhase(DirectionImport))
	require.Equal(t, PhaseExporting, streamingPhase(DirectionExport))
	require.Equal(t, PhaseImporting, streamingPhase(DirectionImport))
	require.Equal(t, DirectionExport, switchTarget(PhaseSwitchingToExport))
	require.Equal(t, DirectionImport, switchTarget(PhaseSwitchingToImport))
}

func TestEffectiveDirection(t *testing.T) {
	// PhaseCompleting carries no direction of its own: directionOf falls through to
	// the IMPORT default. That is the trap DropMigration hit — it overwrote the
	// phase to COMPLETING before draining/tearing down, so an EXPORT drop was
	// misrouted as an IMPORT one. effectiveDirection guards against it by reading
	// the direction persisted on the row.
	require.Equal(t, DirectionImport, directionOf(PhaseCompleting),
		"directionOf(COMPLETING) is the lossy default that the persisted Direction exists to override")

	// A completing migration reports its persisted direction, not the phase default.
	require.Equal(t, DirectionExport,
		(&Migration{Phase: PhaseCompleting, Direction: DirectionExport}).effectiveDirection())
	require.Equal(t, DirectionImport,
		(&Migration{Phase: PhaseCompleting, Direction: DirectionImport}).effectiveDirection())

	// With no persisted direction (a row written before the column existed), fall
	// back to the phase — authoritative for every non-completing phase.
	require.Equal(t, DirectionExport, (&Migration{Phase: PhaseExporting}).effectiveDirection())
	require.Equal(t, DirectionImport, (&Migration{Phase: PhaseImporting}).effectiveDirection())
}

func TestProject(t *testing.T) {
	c := &Coordinator{} // project reads no coordinator state
	now := time.Now()
	m := &Migration{
		ID:             "m123",
		Name:           "nightly",
		SourceDSN:      testDSN,
		TargetDatabase: "appdb",
		TargetShard:    "0",
		Phase:          PhaseImporting,
		Tables:         []string{"public.orders"},
		LastError:      "",
		CreatedAt:      now,
	}

	// Without live status.
	p := c.project(m, nil)
	require.Equal(t, "m123", p.ID)
	require.Equal(t, "nightly", p.Name)
	require.Equal(t, "src.example.com:5432/appdb", p.Source)
	require.NotContains(t, p.Source, "s3cr3t", "projection must never carry the password")
	require.Equal(t, "mt_pub_m123", p.PublicationName)
	require.Equal(t, "mt_sub_m123", p.SubscriptionName)
	require.Equal(t, PhaseImporting, p.Phase)
	require.Equal(t, DirectionImport, p.ActiveDirection, "active_direction is derived from the phase")
	require.False(t, p.CaughtUp)

	// With live status merged in.
	p = c.project(m, &SubscriptionStatus{
		TotalRelations: 3, ReadyRelations: 3, CaughtUp: true,
		ReceivedLSN: "0/16B3D80", LatestEndLSN: "0/16B3D80",
	})
	require.EqualValues(t, 3, p.TotalRelations)
	require.EqualValues(t, 3, p.ReadyRelations)
	require.True(t, p.CaughtUp)
	require.Equal(t, "0/16B3D80", p.ReceivedLSN)
}
