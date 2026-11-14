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

// Package backingconn decodes, validates and carries a connection to an external PostgreSQL, for
// the migrator and for the unmanaged pooler that fronts one.
//
// A connection is self-contained: it names the host, port, database, user, password and sslmode
// that a client would otherwise take from its own environment, and carries only the parameters of
// a closed table (params.go), each checked as libpq checks it. It is rendered (LibpqString) as the
// one string every client is dialed with, which also pins what our environment must not decide for
// the host the connection names (params.go), so that what is dialed is what was validated.
package backingconn

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/tools/pgutil"
)

// Conn is a connection to a PostgreSQL, decoded from a libpq connection string (ParseDSN) and
// stored as JSON in the connections table. It carries the parameters a backing connection needs,
// and keeps every other libpq parameter in Params, so that a connection written for the migrator
// loses nothing.
//
// The host and the port are lists, as in libpq's host=a,b port=1,2. A backing connection is not
// given more than one server; ForPooler is where that is refused.
//
// The password is in the struct, and in its JSON. Formatting a Conn, with %v, %+v, %#v or as a
// slog value, never prints it.
type Conn struct {
	// Name is the name of the connection in the catalog. It is not part of the stored JSON: the
	// name is a column of its own.
	Name string `json:"-"`

	Hosts          []string `json:"host,omitempty"`
	Ports          []int    `json:"port,omitempty"`
	Database       string   `json:"dbname,omitempty"`
	User           string   `json:"user,omitempty"`
	Password       string   `json:"password,omitempty"`
	SSLMode        string   `json:"sslmode,omitempty"`
	SSLRootCert    string   `json:"sslrootcert,omitempty"`
	SSLNegotiation string   `json:"sslnegotiation,omitempty"`

	// Params are the libpq parameters a connection may carry that a backing connection does not
	// (hostaddr, connect_timeout, application_name, ...), by their libpq names. The table in
	// params.go says which there are, and Validate checks their values.
	Params map[string]string `json:"params,omitempty"`
}

// Clone returns a copy that shares nothing with c.
func (c *Conn) Clone() *Conn {
	clone := *c
	clone.Hosts = slices.Clone(c.Hosts)
	clone.Ports = slices.Clone(c.Ports)
	clone.Params = maps.Clone(c.Params)
	return &clone
}

// Host returns the host of the connection's one server; a validated connection has one.
func (c *Conn) Host() string {
	if len(c.Hosts) == 0 {
		return ""
	}
	return c.Hosts[0]
}

// Port returns the port of the connection's one server; a validated connection has one.
func (c *Conn) Port() int {
	if len(c.Ports) == 0 {
		return 0
	}
	return c.Ports[0]
}

// DSN renders the connection as a libpq keyword=value string, every value quoted, in a fixed
// order, with exactly the parameters the connection carries (ParseDSN reads it back). It is for
// display and the dsn column: a client dials LibpqString, which adds the pins of params.go.
func (c *Conn) DSN() string {
	return pgutil.ConnInfoString(c.values())
}

// values is every parameter the connection carries, by libpq's names.
func (c *Conn) values() map[string]string {
	kv := map[string]string{}
	if len(c.Hosts) > 0 {
		kv["host"] = strings.Join(c.Hosts, ",")
	}
	if len(c.Ports) > 0 {
		ports := make([]string, len(c.Ports))
		for i, p := range c.Ports {
			ports[i] = strconv.Itoa(p)
		}
		kv["port"] = strings.Join(ports, ",")
	}
	for key, value := range c.fields() {
		if value != "" {
			kv[key] = value
		}
	}
	maps.Copy(kv, c.Params)
	return kv
}

// fields is the parameters that are fields of the connection and are not lists, by libpq's names,
// including the empty ones.
func (c *Conn) fields() map[string]string {
	return map[string]string{
		"dbname": c.Database, "user": c.User, "password": c.Password,
		"sslmode": c.SSLMode, "sslrootcert": c.SSLRootCert, "sslnegotiation": c.SSLNegotiation,
	}
}

const (
	defaultPort = 5432
	// libpq would take the name of the user, or PGDATABASE.
	defaultDatabase = "postgres"
)

// Complete fills in what a connection leaves out, so that no client takes it from PGPORT,
// PGDATABASE or PGSSLMODE: port 5432, database postgres, and sslmode verify-full for
// sslrootcert=system, require otherwise (libpq's prefer would fall back to plaintext). Host, user
// and password have no default: Validate requires them.
func (c *Conn) Complete() {
	if len(c.Ports) == 0 {
		c.Ports = []int{defaultPort}
	}
	if c.Database == "" {
		c.Database = defaultDatabase
	}
	if c.SSLMode == "" {
		c.SSLMode = string(client.DefaultSSLMode(c.SSLRootCert, client.SSLModeRequire))
	}
}

// FromDSN decodes a libpq connection string into a complete (Complete), valid (Validate)
// connection, the way a connection is read when it is created. The error never carries a value
// of the string.
func FromDSN(dsn string) (*Conn, error) {
	conn, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	conn.Complete()
	if err := conn.Validate(); err != nil {
		return nil, err
	}
	return conn, nil
}

// Validate reports why a connection is malformed or not self-contained: what libpq refuses, reads
// differently from what was meant, or would take from the environment of whoever dials. It does
// not ask that a pooler can front it, which needs more (ForPooler).
func (c *Conn) Validate() error {
	if len(c.Hosts) == 0 {
		return errors.New("host is required: without one a client connects to the local socket, or to whatever PGHOST says")
	}
	for _, host := range c.Hosts {
		switch {
		case strings.TrimSpace(host) == "":
			return errors.New("host has an empty entry")
		case strings.HasPrefix(host, "/"):
			// pg_dump and CREATE SUBSCRIPTION would follow a socket directory to a postgres on the
			// machine that dials, with the credentials of the connection.
			return errors.New("host is a socket directory; a network host is required")
		case strings.ContainsAny(host, ",/ \t\r\n\v\f@"):
			return errors.New("host must be a single hostname or IP address")
		}
	}
	if len(c.Ports) == 0 {
		return errors.New("port is required")
	}
	for _, port := range c.Ports {
		if port < 1 || port > 65535 {
			return fmt.Errorf("port must be between 1 and 65535, got %d", port)
		}
	}
	if len(c.Ports) > 1 && len(c.Ports) != len(c.Hosts) {
		return errors.New("port must be one number, or one for each host")
	}
	switch {
	case c.User == "":
		return errors.New("user is required: without one a client uses PGUSER, or the name of the operating system user")
	case c.Database == "":
		return errors.New("database is required: without one a client uses PGDATABASE, or the user name")
	case c.Password == "":
		// A conninfo without a password makes libpq read PGPASSFILE, which for pgctld's postgres
		// holds an entry that matches any host: the local postgres subscribing from the
		// connection would send that password to whatever host the connection names.
		return errors.New("password is required: without one a client looks one up in its environment and in its password file")
	case c.SSLMode == "":
		return errors.New("sslmode is required")
	}
	// libpq takes these exactly (no case folding, no trimming). An empty value is the field left
	// out: sslmode is required above, and an empty sslnegotiation is the default.
	fields := c.fields()
	for _, name := range slices.Sorted(maps.Keys(fields)) {
		if check := params[name].check; check != nil && fields[name] != "" {
			if err := check(fields[name]); err != nil {
				return fmt.Errorf("%s is not valid: %w", name, err)
			}
		}
	}
	for _, text := range append([]string{c.User, c.Database, c.Password, c.SSLRootCert}, c.Hosts...) {
		if err := checkText(text); err != nil {
			return fmt.Errorf("a value is invalid: %w", err)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(c.Params)) {
		def, known := params[key]
		switch {
		case !known || def.class == classCarried:
			return errUnrecognized
		case !def.allowed:
			return fmt.Errorf("parameter %s is not supported: a connection carries only %s", key, strings.Join(namesWhere(func(p param) bool { return p.allowed }), ", "))
		case def.check != nil:
			if err := def.check(c.Params[key]); err != nil {
				return fmt.Errorf("parameter %s is not valid: %w", key, err)
			}
		}
	}
	if addrs, ok := c.Params["hostaddr"]; ok && strings.Count(addrs, ",")+1 != len(c.Hosts) {
		return errors.New("hostaddr must have one address for each host")
	}
	// libpq's rules between these settings, as it applies them when it connects.
	mode := client.SSLMode(c.SSLMode)
	if err := client.ValidateSSLRootCert(c.SSLRootCert, mode); err != nil {
		return err
	}
	return client.ValidateSSLNegotiation(client.SSLNegotiation(c.SSLNegotiation), mode)
}

// ForPooler reports why a pooler cannot front this connection, or nil: it must be valid
// (Validate), carry nothing but what the pooler honors, and name one network server. The default
// primary applies it before serving a connection and the pooler to the one it receives, so a
// connection that one serves is one the other accepts.
//
// One server, because the pooler's admin connection must reach the same server as its application
// connections: what the pooler learns about the server it probes must hold for the server the
// application uses, which a list of hosts could send apart.
func (c *Conn) ForPooler() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if len(c.Params) > 0 {
		// Every key is a libpq parameter name (Validate), never text of the string, so it is
		// safe to name.
		return fmt.Errorf("parameter %s is not supported; a backing connection carries only %s", slices.Sorted(maps.Keys(c.Params))[0],
			strings.Join(namesWhere(func(p param) bool { return p.class == classCarried }), ", "))
	}
	if len(c.Hosts) > 1 {
		return errors.New("host names more than one server; a single server is required")
	}
	return nil
}

// MarshalJSON is the encoding of a connection in the connections table. UnmarshalJSON refuses a
// field this version does not know, so a connection written by a later version is not silently
// read with less than it says.
func (c Conn) MarshalJSON() ([]byte, error) {
	type plain Conn
	return json.Marshal(plain(c))
}

// UnmarshalJSON reads the stored encoding, refusing a field it does not know and anything after
// the object.
func (c *Conn) UnmarshalJSON(data []byte) error {
	type plain Conn
	var p plain
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return fmt.Errorf("decode connection: %w", jsonError(err))
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("decode connection: unexpected data after the object")
	}
	name := c.Name
	*c = Conn(p)
	c.Name = name
	return nil
}

// jsonError drops the text of a decoding error that quotes the input, which holds the password.
func jsonError(err error) error {
	var typeErr *json.UnmarshalTypeError
	switch {
	case errors.As(err, &typeErr):
		return fmt.Errorf("field %q has the wrong type", typeErr.Field)
	case strings.HasPrefix(err.Error(), "json: unknown field"):
		return errors.New("it has a field this version does not know")
	}
	return errors.New("it is not valid JSON")
}

// String describes the connection without its secrets: the password is never printed, and of the
// other libpq parameters only their names are.
func (c Conn) String() string {
	params := slices.Sorted(maps.Keys(c.Params))
	password := ""
	if c.Password != "" {
		password = " password=<redacted>"
	}
	return fmt.Sprintf("Conn{name=%q host=%v port=%v dbname=%q user=%q sslmode=%q params=%v%s}",
		c.Name, c.Hosts, c.Ports, c.Database, c.User, c.SSLMode, params, password)
}

// GoString is String, for %#v.
func (c Conn) GoString() string { return c.String() }

// LogValue is String, for slog.
func (c Conn) LogValue() slog.Value { return slog.StringValue(c.String()) }
