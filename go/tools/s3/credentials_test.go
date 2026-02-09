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
	"os"
	"testing"
)

func TestReadCredentialsFromEnv(t *testing.T) {
	tests := []struct {
		name         string
		accessKey    string
		secretKey    string
		sessionToken string
		wantErr      bool
		errContains  string
		checkValues  bool
	}{
		{
			name:         "success with all credentials",
			accessKey:    "AKIAIOSFODNN7EXAMPLE",
			secretKey:    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			sessionToken: "AQoDYXdzEJr...<rest of session token>",
			wantErr:      false,
			checkValues:  true,
		},
		{
			name:         "success with optional SessionToken missing",
			accessKey:    "AKIAIOSFODNN7EXAMPLE",
			secretKey:    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			sessionToken: "",
			wantErr:      false,
			checkValues:  true,
		},
		{
			name:        "error with missing AWS_ACCESS_KEY_ID",
			accessKey:   "",
			secretKey:   "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			wantErr:     true,
			errContains: "AWS_ACCESS_KEY_ID",
		},
		{
			name:        "error with missing AWS_SECRET_ACCESS_KEY",
			accessKey:   "AKIAIOSFODNN7EXAMPLE",
			secretKey:   "",
			wantErr:     true,
			errContains: "AWS_SECRET_ACCESS_KEY",
		},
		{
			name:        "error with both credentials missing",
			accessKey:   "",
			secretKey:   "",
			wantErr:     true,
			errContains: "AWS_ACCESS_KEY_ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original environment
			origAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
			origSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
			origSessionToken := os.Getenv("AWS_SESSION_TOKEN")

			// Restore environment after test
			defer func() {
				restoreEnv("AWS_ACCESS_KEY_ID", origAccessKey)
				restoreEnv("AWS_SECRET_ACCESS_KEY", origSecretKey)
				restoreEnv("AWS_SESSION_TOKEN", origSessionToken)
			}()

			// Set test environment
			setEnv("AWS_ACCESS_KEY_ID", tt.accessKey)
			setEnv("AWS_SECRET_ACCESS_KEY", tt.secretKey)
			setEnv("AWS_SESSION_TOKEN", tt.sessionToken)

			// Call function under test
			creds, err := ReadCredentialsFromEnv()

			// Check error expectations
			if tt.wantErr {
				if err == nil {
					t.Errorf("ReadCredentialsFromEnv() expected error containing %q, got nil", tt.errContains)
					return
				}
				if tt.errContains != "" && !containsString(err.Error(), tt.errContains) {
					t.Errorf("ReadCredentialsFromEnv() error = %q, want error containing %q", err.Error(), tt.errContains)
				}
				return
			}

			// Check success expectations
			if err != nil {
				t.Errorf("ReadCredentialsFromEnv() unexpected error = %v", err)
				return
			}

			if creds == nil {
				t.Error("ReadCredentialsFromEnv() returned nil credentials without error")
				return
			}

			// Check values if requested
			if tt.checkValues {
				if creds.AccessKey != tt.accessKey {
					t.Errorf("ReadCredentialsFromEnv() AccessKey = %q, want %q", creds.AccessKey, tt.accessKey)
				}
				if creds.SecretKey != tt.secretKey {
					t.Errorf("ReadCredentialsFromEnv() SecretKey = %q, want %q", creds.SecretKey, tt.secretKey)
				}
				if creds.SessionToken != tt.sessionToken {
					t.Errorf("ReadCredentialsFromEnv() SessionToken = %q, want %q", creds.SessionToken, tt.sessionToken)
				}
			}
		})
	}
}

// setEnv sets an environment variable, unsetting it if value is empty
func setEnv(key, value string) {
	if value == "" {
		os.Unsetenv(key)
	} else {
		os.Setenv(key, value)
	}
}

// restoreEnv restores an environment variable to its original value
func restoreEnv(key, value string) {
	if value == "" {
		os.Unsetenv(key)
	} else {
		os.Setenv(key, value)
	}
}

// containsString checks if s contains substr
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}
