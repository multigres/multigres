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

package shardsetup

import (
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
)

// The names below moved to clustersetup, which holds the harness pieces shared
// by the Multigres and Minigres topologies. They are forwarded here so that
// existing tests keep importing shardsetup unchanged.

// Cluster is what query-serving tests need from a cluster of either topology.
type Cluster = clustersetup.Cluster

// SetupFunc is a function that creates a ShardSetup for testing.
// It receives a testing.T that can be used for logging during setup.
type SetupFunc func(t *testing.T) *ShardSetup

// SharedSetupManager manages a ShardSetup shared by the tests of a package.
type SharedSetupManager = clustersetup.SharedSetupManager[*ShardSetup]

// NewSharedSetupManager creates a SharedSetupManager for a ShardSetup.
func NewSharedSetupManager(setupFunc SetupFunc) *SharedSetupManager {
	return clustersetup.NewSharedSetupManager(setupFunc)
}

// Constants shared with clustersetup.
const (
	DefaultTestUser      = clustersetup.DefaultTestUser
	TestPostgresPassword = clustersetup.TestPostgresPassword
)

// Types shared with clustersetup.
type (
	MultipoolerClient        = clustersetup.MultipoolerClient
	PgctldClient             = clustersetup.PgctldClient
	MultiorchClient          = clustersetup.MultiorchClient
	TestTarget               = clustersetup.TestTarget
	MultigatewayTLSCertPaths = clustersetup.MultigatewayTLSCertPaths
	SetupTestConfig          = clustersetup.SetupTestConfig
	SetupTestOption          = clustersetup.SetupTestOption
	MultipoolerTestClient    = clustersetup.MultipoolerTestClient
	ProcessInstance          = clustersetup.ProcessInstance
)

// Functions shared with clustersetup.
var (
	CreatePgctldInstance         = clustersetup.CreatePgctldInstance
	PrintLogLocation             = clustersetup.PrintLogLocation
	WaitForQueryServingOnPort    = clustersetup.WaitForQueryServingOnPort
	ValidatePoolerType           = clustersetup.ValidatePoolerType
	SaveGUCs                     = clustersetup.SaveGUCs
	ReloadConfig                 = clustersetup.ReloadConfig
	RestoreGUCs                  = clustersetup.RestoreGUCs
	ValidateGUCValue             = clustersetup.ValidateGUCValue
	NewMultipoolerClient         = clustersetup.NewMultipoolerClient
	WaitForManagerReady          = clustersetup.WaitForManagerReady
	QueryStringValue             = clustersetup.QueryStringValue
	NewPgctldClient              = clustersetup.NewPgctldClient
	NewMultiorchClient           = clustersetup.NewMultiorchClient
	GenerateMultigatewayTLSCerts = clustersetup.GenerateMultigatewayTLSCerts
	WithoutReplication           = clustersetup.WithoutReplication
	WithPausedReplication        = clustersetup.WithPausedReplication
	WithResetGuc                 = clustersetup.WithResetGuc
	ParseEvents                  = clustersetup.ParseEvents
	HasEvent                     = clustersetup.HasEvent
	FindEvents                   = clustersetup.FindEvents
	WaitForLogLine               = clustersetup.WaitForLogLine
	WaitForEvent                 = clustersetup.WaitForEvent
	NewMultipoolerTestClient     = clustersetup.NewMultipoolerTestClient
	IsLeader                     = clustersetup.IsLeader
	WaitForPoolerTypeAssigned    = clustersetup.WaitForPoolerTypeAssigned
	TestBasicSelect              = clustersetup.TestBasicSelect
	TestCreateTable              = clustersetup.TestCreateTable
	TestInsertData               = clustersetup.TestInsertData
	TestSelectData               = clustersetup.TestSelectData
	TestQueryLimits              = clustersetup.TestQueryLimits
	TestUpdateData               = clustersetup.TestUpdateData
	TestDeleteData               = clustersetup.TestDeleteData
	TestDropTable                = clustersetup.TestDropTable
	TestDataTypes                = clustersetup.TestDataTypes
	TestMultigresSchemaExists    = clustersetup.TestMultigresSchemaExists
	TestHeartbeatTableExists     = clustersetup.TestHeartbeatTableExists
	TestPrimaryDetection         = clustersetup.TestPrimaryDetection
	WaitForBootstrap             = clustersetup.WaitForBootstrap
	BuildPgctldServerArgs        = clustersetup.BuildPgctldServerArgs
	WaitForPortReady             = clustersetup.WaitForPortReady
	GetTestUserDSN               = clustersetup.GetTestUserDSN
	GetPostgresDSN               = clustersetup.GetPostgresDSN
	RunTestMain                  = clustersetup.RunTestMain
)
