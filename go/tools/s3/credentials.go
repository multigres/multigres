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

package s3

import (
	"errors"
	"os"
)

// Credentials holds AWS credential information
type Credentials struct {
	AccessKey    string
	SecretKey    string
	SessionToken string // Optional: for temporary credentials (AWS STS)
}

// ReadCredentialsFromEnv reads AWS credentials from environment variables.
// Returns an error if required credentials (AccessKey, SecretKey) are not set.
// SessionToken is optional and may be empty.
func ReadCredentialsFromEnv() (*Credentials, error) {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")

	if accessKey == "" {
		return nil, errors.New("AWS_ACCESS_KEY_ID environment variable is required")
	}
	if secretKey == "" {
		return nil, errors.New("AWS_SECRET_ACCESS_KEY environment variable is required")
	}

	return &Credentials{
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		SessionToken: sessionToken,
	}, nil
}
