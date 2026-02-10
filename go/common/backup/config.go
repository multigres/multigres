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
	"errors"
	"strings"

	"github.com/multigres/multigres/go/common/safepath"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	s3tools "github.com/multigres/multigres/go/tools/s3"
)

// Config provides a unified interface for backup operations
type Config struct {
	proto *clustermetadatapb.BackupLocation
}

// NewConfig creates a Config from a BackupLocation proto
func NewConfig(loc *clustermetadatapb.BackupLocation) (*Config, error) {
	if loc == nil {
		return nil, errors.New("backup location cannot be nil")
	}

	if err := validate(loc); err != nil {
		return nil, err
	}

	return &Config{proto: loc}, nil
}

// Type returns the backup location type for logging/metrics
func (c *Config) Type() string {
	switch c.proto.Location.(type) {
	case *clustermetadatapb.BackupLocation_Filesystem:
		return "filesystem"
	case *clustermetadatapb.BackupLocation_S3:
		return "s3"
	default:
		return "unknown"
	}
}

// FullPath returns the complete backup path for a database/tablegroup/shard
func (c *Config) FullPath(database, tableGroup, shard string) (string, error) {
	if database == "" {
		return "", errors.New("database cannot be empty")
	}
	if tableGroup == "" {
		return "", errors.New("table group cannot be empty")
	}
	if shard == "" {
		return "", errors.New("shard cannot be empty")
	}

	switch loc := c.proto.Location.(type) {
	case *clustermetadatapb.BackupLocation_Filesystem:
		return filesystemFullPath(loc.Filesystem.Path, database, tableGroup, shard)
	case *clustermetadatapb.BackupLocation_S3:
		return s3FullPath(loc.S3, database, tableGroup, shard)
	default:
		return "", errors.New("unknown backup location type")
	}
}

// filesystemFullPath builds a filesystem backup path
func filesystemFullPath(basePath, database, tableGroup, shard string) (string, error) {
	return safepath.Join(basePath, database, tableGroup, shard)
}

// s3FullPath builds an S3 backup path
func s3FullPath(s3 *clustermetadatapb.S3Backup, database, tableGroup, shard string) (string, error) {
	// Start with bucket
	path := "s3://" + s3.Bucket + "/"

	// Add prefix if set
	if s3.KeyPrefix != "" {
		path += strings.TrimSuffix(s3.KeyPrefix, "/") + "/"
	}

	// Add database/tablegroup/shard
	path += database + "/" + tableGroup + "/" + shard

	return path, nil
}

// UsesEnvCredentials returns true if S3 backup uses environment credentials
func (c *Config) UsesEnvCredentials() bool {
	if s3, ok := c.proto.Location.(*clustermetadatapb.BackupLocation_S3); ok {
		return s3.S3.UseEnvCredentials
	}
	return false
}

// PgBackRestCredentials returns credentials for pgBackRest from environment variables.
// Returns nil for non-S3 backups or when UseEnvCredentials is false.
// Returns an error if UseEnvCredentials is true but required env vars are missing.
func (c *Config) PgBackRestCredentials() (map[string]string, error) {
	if !c.UsesEnvCredentials() {
		return nil, nil
	}

	awsCreds, err := s3tools.ReadCredentialsFromEnv()
	if err != nil {
		return nil, err
	}

	creds := map[string]string{
		"repo1-s3-key":        awsCreds.AccessKey,
		"repo1-s3-key-secret": awsCreds.SecretKey,
	}

	// Only include session token if it's set
	if awsCreds.SessionToken != "" {
		creds["repo1-s3-token"] = awsCreds.SessionToken
	}

	return creds, nil
}

// GetS3Config returns the S3 configuration, or nil if not using S3 backups
func (c *Config) GetS3Config() *clustermetadatapb.S3Backup {
	if s3, ok := c.proto.Location.(*clustermetadatapb.BackupLocation_S3); ok {
		return s3.S3
	}
	return nil
}

// PgBackRestConfig returns pgBackRest-specific configuration
func (c *Config) PgBackRestConfig(stanzaName string) (map[string]string, error) {
	switch loc := c.proto.Location.(type) {
	case *clustermetadatapb.BackupLocation_Filesystem:
		return map[string]string{
			"repo1-type": "posix",
			"repo1-path": loc.Filesystem.Path,
		}, nil

	case *clustermetadatapb.BackupLocation_S3:
		config := map[string]string{
			"repo1-type":      "s3",
			"repo1-s3-bucket": loc.S3.Bucket,
			"repo1-s3-region": loc.S3.Region,

			// S3-specific performance and efficiency settings
			"repo1-block":                     "y",
			"repo1-bundle":                    "y",
			"repo1-storage-upload-chunk-size": S3UploadChunkSize,
			"repo1-symlink":                   "n",

			// S3-specific retention policies
			"repo1-retention-diff":      RetentionDifferential,
			"repo1-retention-full":      RetentionFull,
			"repo1-retention-full-type": "time",
			"repo1-retention-history":   RetentionHistory,
		}

		// Set credential type based on configuration
		if loc.S3.UseEnvCredentials {
			// When using env credentials, they should be in a separate file
			// Do not include them in the main config
			config["repo1-s3-key-type"] = "shared"
		} else {
			// Use automatic detection (e.g. IRSA)
			config["repo1-s3-key-type"] = "auto"
		}

		if loc.S3.Endpoint == "" {
			// AWS S3 - generate standard endpoint
			config["repo1-s3-endpoint"] = "s3." + loc.S3.Region + ".amazonaws.com"
			// Use virtual-hosted style (AWS default)
			config["repo1-s3-uri-style"] = "host"
			// TLS verification defaults to 'y', no need to set explicitly
		} else {
			// Custom endpoint (s3mock, etc.)
			config["repo1-s3-endpoint"] = loc.S3.Endpoint
			config["repo1-s3-verify-tls"] = "n"
			config["repo1-s3-uri-style"] = "path"
		}

		// Repo path includes prefix if set
		path := "/" + stanzaName
		if loc.S3.KeyPrefix != "" {
			path = "/" + strings.TrimSuffix(loc.S3.KeyPrefix, "/") + path
		}
		config["repo1-path"] = path

		return config, nil

	default:
		return nil, errors.New("unknown backup location type")
	}
}

// validate checks that the backup location is properly configured
func validate(loc *clustermetadatapb.BackupLocation) error {
	if loc.Location == nil {
		return errors.New("no backup location configured")
	}

	switch v := loc.Location.(type) {
	case *clustermetadatapb.BackupLocation_Filesystem:
		if v.Filesystem == nil {
			return errors.New("filesystem backup config is nil")
		}
		if v.Filesystem.Path == "" {
			return errors.New("filesystem path is required")
		}
	case *clustermetadatapb.BackupLocation_S3:
		if v.S3 == nil {
			return errors.New("s3 backup config is nil")
		}
		if v.S3.Bucket == "" {
			return errors.New("s3 bucket is required")
		}
		if v.S3.Region == "" {
			return errors.New("s3 region is required")
		}
	default:
		return errors.New("unknown backup location type")
	}

	return nil
}
