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
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// DefaultBackupJobExpiration is the default time after which completed/failed backup jobs are removed
const DefaultBackupJobExpiration = 24 * time.Hour

// backupJobMetadata stores additional metadata not in the proto
type backupJobMetadata struct {
	response    *multiadminpb.GetBackupJobStatusResponse
	createdAt   time.Time
	updatedAt   time.Time
	completedAt *time.Time
}

// BackupJobTracker manages async backup/restore jobs.
//
// This is a simple in-memory implementation, so process restarts will lose all jobs.
type BackupJobTracker struct {
	mu         sync.Mutex
	jobs       map[string]*backupJobMetadata
	expiration time.Duration

	// Shutdown-related channels
	stop chan struct{}
	done chan struct{}
}

// NewBackupJobTracker creates a new backup job tracker with default expiration (24 hours)
func NewBackupJobTracker() *BackupJobTracker {
	return NewBackupJobTrackerWithExpiration(DefaultBackupJobExpiration)
}

// NewBackupJobTrackerWithExpiration creates a new backup job tracker with custom expiration
func NewBackupJobTrackerWithExpiration(expiration time.Duration) *BackupJobTracker {
	jt := &BackupJobTracker{
		jobs:       make(map[string]*backupJobMetadata),
		expiration: expiration,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	go jt.cleanupLoop()
	return jt
}

// cleanupLoop runs in the background and removes expired jobs
func (jt *BackupJobTracker) cleanupLoop() {
	defer close(jt.done)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-jt.stop:
			return
		case <-ticker.C:
			jt.removeExpiredJobs()
		}
	}
}

// removeExpiredJobs removes jobs that have been completed/failed for longer than the expiration period
func (jt *BackupJobTracker) removeExpiredJobs() {
	jt.mu.Lock()
	defer jt.mu.Unlock()

	now := time.Now()
	for jobID, job := range jt.jobs {
		if job.completedAt != nil && now.Sub(*job.completedAt) > jt.expiration {
			delete(jt.jobs, jobID)
		}
	}
}

// Stop stops the background cleanup goroutine
func (jt *BackupJobTracker) Stop() {
	close(jt.stop)
	<-jt.done
}

// CreateJob creates a new backup job with an auto-generated UUID and returns its ID
func (jt *BackupJobTracker) CreateJob(jobType multiadminpb.JobType, database, tableGroup, shard string) string {
	return jt.CreateJobWithID(uuid.New().String(), jobType, database, tableGroup, shard)
}

// CreateJobWithID creates a new backup job with the specified ID
func (jt *BackupJobTracker) CreateJobWithID(jobID string, jobType multiadminpb.JobType, database, tableGroup, shard string) string {
	jt.mu.Lock()
	defer jt.mu.Unlock()

	now := time.Now()

	jt.jobs[jobID] = &backupJobMetadata{
		response: &multiadminpb.GetBackupJobStatusResponse{
			JobId:      jobID,
			JobType:    jobType,
			Status:     multiadminpb.JobStatus_JOB_STATUS_PENDING,
			Database:   database,
			TableGroup: tableGroup,
			Shard:      shard,
		},
		createdAt: now,
		updatedAt: now,
	}

	return jobID
}

// GetJobStatus retrieves the status of a backup job
func (jt *BackupJobTracker) GetJobStatus(jobID string) (*multiadminpb.GetBackupJobStatusResponse, error) {
	jt.mu.Lock()
	defer jt.mu.Unlock()

	job, exists := jt.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("backup job not found: %s", jobID)
	}

	return job.response, nil
}

// UpdateJobStatus updates the status of a backup job
func (jt *BackupJobTracker) UpdateJobStatus(jobID string, status multiadminpb.JobStatus) {
	jt.mu.Lock()
	defer jt.mu.Unlock()

	if job, exists := jt.jobs[jobID]; exists {
		job.response.Status = status
		job.updatedAt = time.Now()
	}
}

// CompleteJob marks a backup job as completed with results
func (jt *BackupJobTracker) CompleteJob(jobID string, backupID string) {
	jt.mu.Lock()
	defer jt.mu.Unlock()

	if job, exists := jt.jobs[jobID]; exists {
		job.response.Status = multiadminpb.JobStatus_JOB_STATUS_COMPLETED
		job.response.BackupId = backupID
		now := time.Now()
		job.updatedAt = now
		job.completedAt = &now
	}
}

// FailJob marks a backup job as failed with error message
func (jt *BackupJobTracker) FailJob(jobID string, errorMsg string) {
	jt.mu.Lock()
	defer jt.mu.Unlock()

	if job, exists := jt.jobs[jobID]; exists {
		job.response.Status = multiadminpb.JobStatus_JOB_STATUS_FAILED
		job.response.ErrorMessage = errorMsg
		now := time.Now()
		job.updatedAt = now
		job.completedAt = &now
	}
}
