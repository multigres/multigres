// Copyright 2026 Supabase, Inc.
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

package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// LocationFingerprint identifies where a shard's backup repositories live. The
// pgbackrestd sidecar reports the hash of the location it was started with,
// and multipooler compares it with the hash of the location in the topology,
// so the two agree on the repository without the sidecar API ever naming it.
//
// It is the configuration the two must agree on, which is more than the
// repository's identity: the CA file and the verification switch are part of
// it, so a sidecar that verifies the endpoint differently from what the
// topology says (most of all one that does not verify at all) does not match,
// and a changed CA path cannot stay unapplied on a running sidecar unnoticed.
// The repository's identity proper, which a sidecar's state directory is tied
// to for good, is Config.RepositoryScope, and it does not include them: the
// same bucket reached with another CA is the same repository.
//
// Both sides must hash the same bytes, so this one type is the only place the
// fields and their order are written down. Field order fixes the JSON; adding,
// removing or reordering a field changes every hash and must ship to the
// sidecar and multipooler together.
type LocationFingerprint struct {
	LocationType string `json:"location-type"`
	S3Bucket     string `json:"s3-bucket"`
	S3Region     string `json:"s3-region"`
	S3Endpoint   string `json:"s3-endpoint"`
	S3KeyPrefix  string `json:"s3-key-prefix"`
	FSRepoPath   string `json:"fs-repo-path"`
	// How the S3 endpoint's certificate is verified. These two are left out
	// of the JSON when unset, so a location without them hashes the same
	// with or without these fields in the type.
	S3CAFile        string `json:"s3-ca-file,omitempty"`
	S3SkipTLSVerify bool   `json:"s3-skip-tls-verify,omitempty"`
}

// Location type values, as the sidecar's --location-type flag spells them.
const (
	LocationTypeS3         = "s3"
	LocationTypeFilesystem = "filesystem"
)

// DefaultFilesystemRepoPath is the repository of a cluster with no backup
// location in the topology.
const DefaultFilesystemRepoPath = "/backups"

// Hash is the hex SHA-256 of the fingerprint's JSON.
func (f LocationFingerprint) Hash() string {
	b, _ := json.Marshal(f)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// LocationFingerprintOf is the fingerprint of a topology backup location. A
// nil location is the filesystem repository at DefaultFilesystemRepoPath.
func LocationFingerprintOf(loc *clustermetadatapb.BackupLocation) LocationFingerprint {
	fp := LocationFingerprint{LocationType: LocationTypeFilesystem, FSRepoPath: DefaultFilesystemRepoPath}
	switch l := loc.GetLocation().(type) {
	case *clustermetadatapb.BackupLocation_S3:
		fp = LocationFingerprint{
			LocationType: LocationTypeS3,
			S3Bucket:     l.S3.GetBucket(),
			S3Region:     l.S3.GetRegion(),
			S3Endpoint:   l.S3.GetEndpoint(),
			S3KeyPrefix:  l.S3.GetKeyPrefix(),

			S3CAFile:        l.S3.GetCaFile(),
			S3SkipTLSVerify: l.S3.GetSkipTlsVerify(),
		}
	case *clustermetadatapb.BackupLocation_Filesystem:
		fp.FSRepoPath = l.Filesystem.GetPath()
	}
	return fp
}
