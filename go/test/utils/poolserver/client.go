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

package poolserver

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

// Client is a connection to a running pool server.
// A single Client may be shared across goroutines; all operations are
// serialised by an internal mutex.
type Client struct {
	mu      sync.Mutex
	conn    net.Conn
	scanner *bufio.Scanner
	writer  *bufio.Writer
}

// Connect opens a connection to the pool server at socketPath.
func Connect(socketPath string) (*Client, error) {
	if conn, err := net.Dial("unix", socketPath); err != nil {
		return nil, fmt.Errorf("connect to pool server at %s: %w", socketPath, err)
	} else {
		return &Client{
			conn:    conn,
			scanner: bufio.NewScanner(conn),
			writer:  bufio.NewWriter(conn),
		}, nil
	}
}

// maxAllocRetries is the maximum number of collision retries for AllocPort.
const maxAllocRetries = 10

// Alloc requests a new port from the server.
// The port is immediately bindable — the server does not hold a listener on it.
// The server records the port number in its registry to prevent handing the
// same number to another caller.
//
// In the very rare case where the OS returns a port the server is already
// tracking (collision), Alloc retries automatically up to maxAllocRetries
// times before returning an error.
func (c *Client) AllocPort() (int, error) {
	for range maxAllocRetries {
		resp, err := c.send(cmdAlloc)
		if err != nil {
			return 0, err
		}

		if after, ok := strings.CutPrefix(resp, respPrefixPort+" "); ok {
			return strconv.Atoi(after)
		}

		if !strings.HasPrefix(resp, respErrCollision) {
			return 0, fmt.Errorf("%s: unexpected response: %s", cmdAlloc, resp)
		}

		// Transient collision; retry.
	}
	return 0, fmt.Errorf("%s: failed to allocate a port after %d attempts due to repeated collisions", cmdAlloc, maxAllocRetries)
}

// Return tells the server the port is no longer in use by any process.
// The server removes the port from its tracking tables.
// Call this from t.Cleanup when a test that used the port completes.
func (c *Client) ReturnPort(port int) error {
	if resp, err := c.send(fmt.Sprintf(cmdReturn+" %d", port)); err != nil {
		return err
	} else if resp != respOK {
		return fmt.Errorf("%s %d: unexpected response: %s", cmdReturn, port, resp)
	}
	return nil
}

// Ping checks that the server is alive.
func (c *Client) Ping() error {
	if resp, err := c.send(cmdPing); err != nil {
		return err
	} else if resp != respPong {
		return fmt.Errorf("%s: unexpected response: %s", cmdPing, resp)
	}
	return nil
}

// Close closes the connection to the server.
// The server will automatically clean up any ports still held for this
// connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) send(msg string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := fmt.Fprintln(c.writer, msg); err != nil {
		return "", fmt.Errorf("send %q: %w", msg, err)
	}
	if err := c.writer.Flush(); err != nil {
		return "", fmt.Errorf("flush %q: %w", msg, err)
	}
	if !c.scanner.Scan() {
		if err := c.scanner.Err(); err != nil {
			return "", fmt.Errorf("read response for %q: %w", msg, err)
		}
		return "", fmt.Errorf("read response for %q: connection closed", msg)
	}
	return c.scanner.Text(), nil
}
