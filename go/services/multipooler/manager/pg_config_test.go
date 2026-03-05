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

package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestParseApplicationName(t *testing.T) {
	tests := []struct {
		name        string
		appName     string
		expected    *clustermetadatapb.ID
		expectError bool
	}{
		{
			name:    "Valid application name",
			appName: "us-west_replica-1",
			expected: &clustermetadatapb.ID{
				Cell: "us-west",
				Name: "replica-1",
			},
		},
		{
			name:    "Application name with underscores in name part",
			appName: "cell1_standby_server_1",
			expected: &clustermetadatapb.ID{
				Cell: "cell1",
				Name: "standby_server_1",
			},
		},
		{
			name:    "Simple cell and name",
			appName: "zone1_primary",
			expected: &clustermetadatapb.ID{
				Cell: "zone1",
				Name: "primary",
			},
		},
		{
			name:        "Missing underscore separator",
			appName:     "invalidname",
			expectError: true,
		},
		{
			name:        "Empty string",
			appName:     "",
			expectError: true,
		},
		{
			name:        "Only underscore",
			appName:     "_",
			expectError: true,
		},
		{
			name:        "Cell with empty name",
			appName:     "cell_",
			expectError: true,
		},
		{
			name:        "Empty cell with name",
			appName:     "_name",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseApplicationName(tt.appName)

			if tt.expectError {
				require.Error(t, err, "Should return error for invalid format")
				assert.Nil(t, result, "Result should be nil when parsing fails")
			} else {
				require.NoError(t, err, "Should not return error for valid format")
				require.NotNil(t, result, "Result should not be nil")
				assert.Equal(t, tt.expected.Cell, result.Cell, "Cell should match")
				assert.Equal(t, tt.expected.Name, result.Name, "Name should match")
			}
		})
	}
}

func TestParseSynchronousStandbyNames(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		expected    *SyncStandbyConfig
		expectError bool
	}{
		{
			name:  "FIRST with single standby",
			value: `FIRST 1 ("cell1_replica1")`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync: 1,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "cell1", Name: "replica1"},
				},
			},
		},
		{
			name:  "FIRST with multiple standbys",
			value: `FIRST 2 ("us-west_replica1", "us-west_replica2", "us-east_replica1")`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync: 2,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "us-west", Name: "replica1"},
					{Cell: "us-west", Name: "replica2"},
					{Cell: "us-east", Name: "replica1"},
				},
			},
		},
		{
			name:  "ANY with multiple standbys",
			value: `ANY 1 ("zone1_standby1", "zone2_standby1")`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				NumSync: 1,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "zone1", Name: "standby1"},
					{Cell: "zone2", Name: "standby1"},
				},
			},
		},
		{
			name:        "Wildcard configuration not supported",
			value:       "*",
			expectError: true,
		},
		{
			name:        "Wildcard in FIRST not supported",
			value:       "FIRST 1 (*)",
			expectError: true,
		},
		{
			name:        "Wildcard in ANY not supported",
			value:       "ANY 2 (*)",
			expectError: true,
		},
		{
			name:  "FIRST with spaces around members",
			value: `FIRST 1 ( "cell_name1" , "cell_name2" )`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync: 1,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "cell", Name: "name1"},
					{Cell: "cell", Name: "name2"},
				},
			},
		},
		{
			name:  "ANY with higher num_sync",
			value: `ANY 3 ("a_b", "c_d", "e_f", "g_h")`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				NumSync: 3,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "a", Name: "b"},
					{Cell: "c", Name: "d"},
					{Cell: "e", Name: "f"},
					{Cell: "g", Name: "h"},
				},
			},
		},
		{
			name:        "Empty string",
			value:       "",
			expectError: true,
		},
		{
			name:        "Invalid format - missing parentheses",
			value:       "FIRST 1 cell_replica1",
			expectError: true,
		},
		{
			name:        "Invalid format - missing method",
			value:       "1 (cell_replica1)",
			expectError: true,
		},
		{
			name:        "Invalid format - missing num_sync",
			value:       `FIRST ("cell_replica1")`,
			expectError: true,
		},
		{
			name:        "Invalid format - empty member list",
			value:       "FIRST 1 ()",
			expectError: true,
		},
		{
			name:        "Invalid format - invalid num_sync",
			value:       `FIRST abc ("cell_replica1")`,
			expectError: true,
		},
		{
			name:        "Invalid method",
			value:       `QUORUM 1 ("cell_replica1")`,
			expectError: true,
		},
		{
			name:        "Invalid application name format in member",
			value:       `FIRST 1 ("invalidname")`,
			expectError: true,
		},
		{
			name:  "Members without quotes",
			value: "FIRST 1 (cell_replica1)",
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync: 1,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "cell", Name: "replica1"},
				},
			},
		},
		{
			name:  "Mixed quoted and unquoted",
			value: `FIRST 2 ("cell1_name1", cell2_name2)`,
			expected: &SyncStandbyConfig{
				Method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				NumSync: 2,
				StandbyIDs: []*clustermetadatapb.ID{
					{Cell: "cell1", Name: "name1"},
					{Cell: "cell2", Name: "name2"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseSynchronousStandbyNames(tt.value)

			if tt.expectError {
				require.Error(t, err, "Should return error for invalid format")
				assert.Nil(t, result, "Result should be nil when parsing fails")
			} else {
				require.NoError(t, err, "Should not return error for valid format")
				require.NotNil(t, result, "Result should not be nil")
				assert.Equal(t, tt.expected.Method, result.Method, "Method should match")
				assert.Equal(t, tt.expected.NumSync, result.NumSync, "NumSync should match")

				if tt.expected.StandbyIDs == nil {
					assert.Nil(t, result.StandbyIDs, "StandbyIDs should be nil")
				} else {
					require.Len(t, result.StandbyIDs, len(tt.expected.StandbyIDs), "StandbyIDs length should match")
					for i, expectedID := range tt.expected.StandbyIDs {
						assert.Equal(t, expectedID.Cell, result.StandbyIDs[i].Cell, "Cell should match at index %d", i)
						assert.Equal(t, expectedID.Name, result.StandbyIDs[i].Name, "Name should match at index %d", i)
					}
				}
			}
		})
	}
}

func TestParseAndRedactPrimaryConnInfo(t *testing.T) {
	tests := []struct {
		name        string
		connInfo    string
		expected    *multipoolermanagerdata.PrimaryConnInfo
		expectError bool
	}{
		{
			name:     "Complete connection string",
			connInfo: "host=localhost port=5432 user=postgres application_name=test_cell_standby1",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "test_cell_standby1",
				Raw:             "host=localhost port=5432 user=postgres application_name=test_cell_standby1",
			},
		},
		{
			name:     "Missing application_name",
			connInfo: "host=primary.example.com port=5433 user=replicator",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "primary.example.com",
				Port:            5433,
				User:            "replicator",
				ApplicationName: "",
				Raw:             "host=primary.example.com port=5433 user=replicator",
			},
		},
		{
			name:     "Missing port",
			connInfo: "host=localhost user=postgres application_name=test_app",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            0,
				User:            "postgres",
				ApplicationName: "test_app",
				Raw:             "host=localhost user=postgres application_name=test_app",
			},
		},
		{
			name:     "Empty string",
			connInfo: "",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "",
				Port:            0,
				User:            "",
				ApplicationName: "",
				Raw:             "",
			},
		},
		{
			name:     "Extra parameters ignored",
			connInfo: "host=localhost port=5432 user=postgres application_name=test keepalives_idle=30 keepalives_interval=10",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "test",
				Raw:             "host=localhost port=5432 user=postgres application_name=test keepalives_idle=30 keepalives_interval=10",
			},
		},
		{
			name:     "Invalid port ignored",
			connInfo: "host=localhost port=invalid user=postgres",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            0,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=invalid user=postgres",
			},
		},
		{
			name:     "Connection with sslmode",
			connInfo: "host=localhost port=5432 user=postgres sslmode=require",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres sslmode=require",
			},
		},
		{
			name:     "Connection with password (redacted)",
			connInfo: "host=localhost port=5432 user=postgres password=secret123",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres password=[REDACTED]",
			},
		},
		{
			name:     "Connection with passfile",
			connInfo: "host=localhost port=5432 user=postgres passfile=/home/user/.pgpass",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres passfile=/home/user/.pgpass",
			},
		},
		{
			name:     "Connection with multiple SSL parameters",
			connInfo: "host=localhost port=5432 user=postgres sslmode=verify-full sslcert=/path/to/cert.pem sslkey=/path/to/key.pem sslrootcert=/path/to/ca.pem",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres sslmode=verify-full sslcert=/path/to/cert.pem sslkey=/path/to/key.pem sslrootcert=/path/to/ca.pem",
			},
		},
		{
			name:     "Connection with keepalive and timeout parameters",
			connInfo: "host=localhost port=5432 user=postgres keepalives_idle=30 keepalives_interval=10 keepalives_count=5 connect_timeout=10",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres keepalives_idle=30 keepalives_interval=10 keepalives_count=5 connect_timeout=10",
			},
		},
		{
			name:     "Connection with channel_binding",
			connInfo: "host=localhost port=5432 user=postgres channel_binding=require",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres channel_binding=require",
			},
		},
		{
			name:     "Connection with gssencmode",
			connInfo: "host=localhost port=5432 user=postgres gssencmode=prefer",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres gssencmode=prefer",
			},
		},
		{
			name:     "Complex connection with all parsed and unparsed fields",
			connInfo: "host=primary.db.local port=5433 user=replicator application_name=zone1_standby2 sslmode=require keepalives_idle=60 connect_timeout=30",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "primary.db.local",
				Port:            5433,
				User:            "replicator",
				ApplicationName: "zone1_standby2",
				Raw:             "host=primary.db.local port=5433 user=replicator application_name=zone1_standby2 sslmode=require keepalives_idle=60 connect_timeout=30",
			},
		},
		{
			name:     "Connection with hostaddr",
			connInfo: "hostaddr=172.28.40.9 port=5432 user=postgres",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "hostaddr=172.28.40.9 port=5432 user=postgres",
			},
		},
		{
			name:     "Connection with dbname",
			connInfo: "host=localhost port=5432 dbname=mydb user=postgres",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 dbname=mydb user=postgres",
			},
		},
		{
			name:     "Connection with client_encoding",
			connInfo: "host=localhost port=5432 user=postgres client_encoding=UTF8",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres client_encoding=UTF8",
			},
		},
		{
			name:     "Connection with options parameter",
			connInfo: "host=localhost port=5432 user=postgres options=-c\\ geqo=off",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres options=-c\\ geqo=off",
			},
		},
		{
			name:     "Connection with replication mode",
			connInfo: "host=localhost port=5432 user=postgres replication=database",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres replication=database",
			},
		},
		{
			name:     "Connection with target_session_attrs",
			connInfo: "host=localhost port=5432 user=postgres target_session_attrs=read-write",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres target_session_attrs=read-write",
			},
		},
		{
			name:     "Connection with sslcrl and sslcompression",
			connInfo: "host=localhost port=5432 user=postgres sslmode=verify-full sslcrl=/path/to/crl.pem sslcompression=1",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres sslmode=verify-full sslcrl=/path/to/crl.pem sslcompression=1",
			},
		},
		{
			name:     "Connection with requirepeer",
			connInfo: "host=localhost port=5432 user=postgres requirepeer=postgres",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres requirepeer=postgres",
			},
		},
		{
			name:     "Connection with krbsrvname and gsslib",
			connInfo: "host=localhost port=5432 user=postgres krbsrvname=postgres gsslib=gssapi",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres krbsrvname=postgres gsslib=gssapi",
			},
		},
		{
			name:     "Connection with service",
			connInfo: "service=myservice",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "",
				Port:            0,
				User:            "",
				ApplicationName: "",
				Raw:             "service=myservice",
			},
		},
		{
			name:     "Comprehensive connection with password redaction",
			connInfo: "host=prod.db.com port=5433 user=repl_user password=SuperSecret123 application_name=standby1 sslmode=verify-full sslcert=/certs/client.pem sslkey=/certs/client.key keepalives_idle=30 connect_timeout=10",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "prod.db.com",
				Port:            5433,
				User:            "repl_user",
				ApplicationName: "standby1",
				Raw:             "host=prod.db.com port=5433 user=repl_user password=[REDACTED] application_name=standby1 sslmode=verify-full sslcert=/certs/client.pem sslkey=/certs/client.key keepalives_idle=30 connect_timeout=10",
			},
		},
		{
			name:        "Invalid format - space-separated without equals",
			connInfo:    "host localhost port 5432",
			expectError: true,
		},
		{
			name:        "Invalid format - missing equals sign in one parameter",
			connInfo:    "host=localhost port 5432 user=postgres",
			expectError: true,
		},
		{
			name:        "Invalid format - empty key",
			connInfo:    "host=localhost =5432 user=postgres",
			expectError: true,
		},
		{
			name:     "Connection with multiple spaces between parameters",
			connInfo: "host=localhost   port=5432  user=postgres",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres", // Spaces normalized due to split/join
			},
		},
		{
			name:     "Connection with leading and trailing spaces",
			connInfo: "  host=localhost port=5432 user=postgres  ",
			expected: &multipoolermanagerdata.PrimaryConnInfo{
				Host:            "localhost",
				Port:            5432,
				User:            "postgres",
				ApplicationName: "",
				Raw:             "host=localhost port=5432 user=postgres", // Leading/trailing spaces trimmed
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseAndRedactPrimaryConnInfo(tt.connInfo)

			if tt.expectError {
				// Expect parsing to fail for invalid formats
				require.Error(t, err, "Should return error for invalid format")
				assert.Nil(t, result, "Result should be nil when parsing fails")
			} else {
				// Expect parsing to succeed
				require.NoError(t, err, "Should not return error for valid format")
				require.NotNil(t, result, "Result should not be nil")
				assert.Equal(t, tt.expected.Host, result.Host, "Host should match")
				assert.Equal(t, tt.expected.Port, result.Port, "Port should match")
				assert.Equal(t, tt.expected.User, result.User, "User should match")
				assert.Equal(t, tt.expected.ApplicationName, result.ApplicationName, "ApplicationName should match")
				assert.Equal(t, tt.expected.Raw, result.Raw, "Raw should match")
			}
		})
	}
}
