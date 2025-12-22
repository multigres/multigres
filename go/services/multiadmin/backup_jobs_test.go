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

package multiadmin

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

func TestBackupJobTracker_CreateAndGetJob(t *testing.T) {
	tracker := NewBackupJobTracker()
	defer tracker.Stop()

	jobID := tracker.CreateJob(multiadminpb.JobType_JOB_TYPE_BACKUP, "postgres", "test", "0")
	require.NotEmpty(t, jobID)

	status, err := tracker.GetJobStatus(jobID)
	require.NoError(t, err)
	require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_PENDING, status.Status)
	require.Equal(t, multiadminpb.JobType_JOB_TYPE_BACKUP, status.JobType)
}

func TestBackupJobTracker_UpdateJobStatus(t *testing.T) {
	tracker := NewBackupJobTracker()
	defer tracker.Stop()
	jobID := tracker.CreateJob(multiadminpb.JobType_JOB_TYPE_RESTORE, "postgres", "test", "0")

	tracker.UpdateJobStatus(jobID, multiadminpb.JobStatus_JOB_STATUS_RUNNING)
	status, err := tracker.GetJobStatus(jobID)
	require.NoError(t, err)
	require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_RUNNING, status.Status)

	tracker.CompleteJob(jobID, "test-backup-id")
	status, err = tracker.GetJobStatus(jobID)
	require.NoError(t, err)
	require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, status.Status)
	require.Equal(t, "test-backup-id", status.BackupId)
}

func TestBackupJobTracker_FailJob(t *testing.T) {
	tracker := NewBackupJobTracker()
	defer tracker.Stop()
	jobID := tracker.CreateJob(multiadminpb.JobType_JOB_TYPE_BACKUP, "postgres", "test", "0")

	tracker.FailJob(jobID, "backup failed: disk full")
	status, err := tracker.GetJobStatus(jobID)
	require.NoError(t, err)
	require.Equal(t, multiadminpb.JobStatus_JOB_STATUS_FAILED, status.Status)
	require.Equal(t, "backup failed: disk full", status.ErrorMessage)
}

func TestBackupJobTracker_NonExistentJob(t *testing.T) {
	tracker := NewBackupJobTracker()
	defer tracker.Stop()

	_, err := tracker.GetJobStatus("non-existent-job-id")
	require.ErrorContains(t, err, "backup job not found: non-existent-job-id")
}

func TestBackupJobTracker_Expiration(t *testing.T) {
	// Use a very short expiration for testing
	tracker := NewBackupJobTrackerWithExpiration(10 * time.Millisecond)
	defer tracker.Stop()

	jobID := tracker.CreateJob(multiadminpb.JobType_JOB_TYPE_BACKUP, "postgres", "test", "0")
	tracker.CompleteJob(jobID, "test-backup-id")

	// Job should still exist immediately after completion
	_, err := tracker.GetJobStatus(jobID)
	require.NoError(t, err)

	// Wait for expiration and trigger cleanup manually
	time.Sleep(20 * time.Millisecond)
	tracker.removeExpiredJobs()

	// Job should be removed
	_, err = tracker.GetJobStatus(jobID)
	require.ErrorContains(t, err, "backup job not found")
}
