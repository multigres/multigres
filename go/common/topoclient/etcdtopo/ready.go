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

package etcdtopo

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// WaitForReady waits for etcd to be ready by verifying the gRPC server
// can accept requests. A TCP port being open doesn't mean the gRPC server
// is fully initialized. This function performs an actual Get operation to
// verify the etcd server is fully ready to handle requests.
//
// The clientAddr should be a full URL including scheme (e.g., "http://localhost:2379").
func WaitForReady(ctx context.Context, clientAddr string, timeout time.Duration) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	for {
		if _, err := cli.Get(ctx, "/"); err == nil {
			return nil
		}
		if time.Since(start) > timeout {
			return fmt.Errorf("etcd failed to become ready within %v", timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
