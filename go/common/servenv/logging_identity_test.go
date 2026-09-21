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

package servenv

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
)

func TestServiceIdentityInLogs(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   ServiceIdentity
		want map[string]string
	}{
		{"pooler", ServiceIdentity{ServiceName: "multipooler", ServiceInstanceID: "dqxscvcb", Cell: "zone2", Shard: "0-inf", TableGroup: "default"}, map[string]string{"service_instance_id": "zone2-dqxscvcb", "cloud_availability_zone": "zone2", "multigres_shard": "0-inf", "multigres_tablegroup": "default"}},
		{"gateway", ServiceIdentity{ServiceName: "multigateway", ServiceInstanceID: "gw1", Cell: "zone1"}, map[string]string{"service_instance_id": "zone1-gw1", "cloud_availability_zone": "zone1"}},
		{"admin", ServiceIdentity{ServiceName: "multiadmin"}, map[string]string{}},
	} {
		for _, format := range []string{"json", "text"} {
			t.Run(tc.name+"/"+format, func(t *testing.T) {
				original := slog.Default()
				t.Cleanup(func() { slog.SetDefault(original) })
				setup := telemetry.SetupTestTelemetry(t)
				t.Cleanup(func() { require.NoError(t, setup.Telemetry.ShutdownTelemetry(context.Background())) })
				reg := viperutil.NewRegistry()
				logger := NewLogger(reg, setup.Telemetry)
				path := filepath.Join(t.TempDir(), "service.log")
				logger.logOutput.Set(path)
				logger.logFormat.Set(format)
				var derived *slog.Logger
				logger.OnLoggingSetup(func(l *slog.Logger) { derived = l.With("subsystem", "test") })
				sv := NewServEnvWithConfig(reg, logger, viperutil.NewViperConfig(reg), setup.Telemetry)
				require.NoError(t, sv.Init(tc.id))
				sv.GetLogger().Info("identity after init")
				slog.Info("default logger identity")
				derived.Info("derived logger identity")

				data, err := os.ReadFile(path)
				require.NoError(t, err)
				lines := strings.Split(strings.TrimSpace(string(data)), "\n")
				require.GreaterOrEqual(t, len(lines), 4)
				keys := []string{"service_instance_id", "cloud_availability_zone", "multigres_shard", "multigres_tablegroup"}
				for _, line := range lines {
					if format == "json" {
						var record map[string]any
						require.NoError(t, json.Unmarshal([]byte(line), &record))
						for _, key := range keys {
							if value, ok := tc.want[key]; ok {
								assert.Equal(t, value, record[key])
							} else {
								assert.NotContains(t, record, key)
							}
						}
					} else {
						for _, key := range keys {
							if value, ok := tc.want[key]; ok {
								assert.Contains(t, line, key+"="+value)
							} else {
								assert.NotContains(t, line, key+"=")
							}
						}
					}
				}
				records := setup.LogProcessor.GetRecords()
				require.GreaterOrEqual(t, len(records), 2)
				for _, record := range records {
					attrs := map[string]string{}
					record.WalkAttributes(func(attr otellog.KeyValue) bool { attrs[attr.Key] = attr.Value.AsString(); return true })
					for _, key := range keys {
						if value, ok := tc.want[key]; ok {
							assert.Equal(t, value, attrs[key])
						} else {
							assert.NotContains(t, attrs, key)
						}
					}
				}
			})
		}
	}
}
