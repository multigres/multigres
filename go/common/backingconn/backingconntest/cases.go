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

// Package backingconntest holds the cases that the two sides of a backing connection, the
// default primary that serves it and the pooler that receives it, are tested against, so that a
// case that one refuses is a case that the other refuses.
package backingconntest

// Case is one connection endpoint and whether it can be used as a backing connection.
type Case struct {
	Name     string
	Host     string
	Port     int
	Database string
	User     string
	// Refused is a fragment of the reason, or empty when the connection is usable.
	Refused string
}

// Cases returns the endpoints to test. Every case differs from the first, a usable connection,
// in one field.
func Cases() []Case {
	good := Case{Name: "usable", Host: "db.example.com", Port: 5432, Database: "app", User: "alice"}
	with := func(name, refused string, mutate func(*Case)) Case {
		c := good
		c.Name, c.Refused = name, refused
		mutate(&c)
		return c
	}
	return []Case{
		good,
		with("an ipv4 address", "", func(c *Case) { c.Host = "10.0.0.7" }),
		with("an ipv6 address", "", func(c *Case) { c.Host = "2001:db8::1" }),
		with("no host", "host is required", func(c *Case) { c.Host = "" }),
		with("a blank host", "host is required", func(c *Case) { c.Host = "  " }),
		with("a host list", "more than one server", func(c *Case) { c.Host = "a.example.com,b.example.com" }),
		with("a socket directory", "socket directory", func(c *Case) { c.Host = "/var/run/postgresql" }),
		with("a host with credentials", "single hostname", func(c *Case) { c.Host = "alice@db.example.com" }),
		with("a host with a path", "single hostname", func(c *Case) { c.Host = "db.example.com/x" }),
		with("a host with a space", "single hostname", func(c *Case) { c.Host = "db.example.com x" }),
		with("a host with a tab", "single hostname", func(c *Case) { c.Host = "db.example.com\tx" }),
		with("a host with a newline", "single hostname", func(c *Case) { c.Host = "db.example.com\nx" }),
		with("port zero", "port", func(c *Case) { c.Port = 0 }),
		with("a port that is too large", "port", func(c *Case) { c.Port = 65536 }),
		with("no database", "database is required", func(c *Case) { c.Database = "" }),
		with("no user", "user is required", func(c *Case) { c.User = "" }),
	}
}
