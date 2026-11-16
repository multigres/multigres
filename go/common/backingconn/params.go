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

package backingconn

import (
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/multigres/multigres/go/tools/pgutil"
)

// class says what a parameter is to us, and so whether the string clients are dialed with pins it
// (see params).
type class uint8

const (
	classCarried class = iota + 1
	classSecret
	classInternal
	classOperational
	classStringOnly
	classService
)

// param describes one libpq connection parameter.
type param struct {
	class class
	// allowed is true when a connection may name the parameter. Every other one is refused: it
	// reads a file, a service or a secret where the connection is dialed, changes what the session
	// is (a walsender, a role, options), or is one a client cannot honor.
	allowed bool
	// check is libpq's rule for a value in Conn.Params, or for a carried field that takes only
	// some values (sslmode, sslnegotiation), which Validate applies when it is not empty; nil
	// accepts any text. libpq takes the values exactly, so a check does not fold case or trim.
	// The error never repeats the value.
	check func(value string) error
	// value is what the string carries for the parameter when the connection does not set it:
	// always for the secret and internal classes, and for a carried parameter that fillsEmpty. It
	// is empty for the others.
	value string
	// fillsEmpty is true for a carried parameter that the string writes, with value, when the
	// connection's field is empty: sslrootcert and sslnegotiation. The other carried fields are
	// always set once a connection is complete and valid, and one that is empty is left out so
	// that the client takes it from its own environment (pg_dump's password).
	fillsEmpty bool
}

// allowedAuth is require_auth: the methods a source may use. A source that asked for gss or sspi
// would be sent our Kerberos credentials, which is the secret class, and libpq reads an empty
// value as a requirement no method satisfies.
const allowedAuth = "password,md5,scram-sha-256,none"

// params is the table the closed set of parameters a connection may carry, the checks of their
// values and the pins of the string clients are dialed with are derived from.
//
// The connection string comes from a user and names a host the user chose, while the environment
// of the dialing process is ours and trusted. So a string pins a parameter only where our
// environment and the user's host together would cause harm. Every parameter has one class:
//
//   - carried: a field of Conn, always in the string. sslrootcert and sslnegotiation have a value
//     for when they are empty.
//   - secret: our credentials or identity could reach the user's host. Always pinned.
//   - internal: a setting meant for our own connections that would change the source connection's
//     target, trust or authentication strength, or its session. Always pinned.
//   - operational: not pinned. The environment's value, or libpq's default, applies.
//   - string-only: has no environment variable and no file, so only the string can set it, and
//     the allowlist governs that. Not pinned.
//   - service: service and servicefile cannot be pinned, since an empty service is a service named
//     "" that libpq and pgx fail to find. The migrator's client refuses to dial with PGSERVICE
//     set.
//
// A pin is libpq's documented default (empty for the parameters unset by default), except
// gssencmode=disable, since prefer would use our Kerberos credential cache toward the user's host,
// sslcertmode=disable, since a connection has no client certificate and one in the dialer's home
// directory must not be offered, and require_auth (allowedAuth). The libpq test of
// go/test/endtoend/backingconn checks every pin against a live psql, pg_dump and CREATE
// SUBSCRIPTION.
//
// Every client is dialed with the one string LibpqString renders. Its pins hold for libpq 17 and
// later (pg_dump, and the postgres that runs CREATE SUBSCRIPTION, load whichever libpq is
// installed), and it never carries a key that libpq 17 refuses as unknown: servicefile, the
// protocol versions and the other parameters of later libpq versions are in the table so that they
// are classified, and are never pinned or allowed. pgx (the migrator's client) puts the keys it
// does not parse itself into RuntimeParams, which it sends to the server as settings, so
// dialConfig drops every key that is a parameter of this table from them, except application_name.
var params = map[string]param{
	"host":     {class: classCarried, allowed: true},
	"port":     {class: classCarried, allowed: true},
	"dbname":   {class: classCarried, allowed: true},
	"user":     {class: classCarried, allowed: true},
	"password": {class: classCarried, allowed: true},
	"sslmode":  {class: classCarried, allowed: true, check: checkEnum("disable", "prefer", "require", "verify-ca", "verify-full")},
	// An explicit '' blocks PGSSLROOTCERT. libpq may still read ~/.postgresql/root.crt from the
	// dialer's home, and if that file exists verification can only get stricter (require becomes
	// verify-ca), never weaker; nothing we deploy writes it.
	"sslrootcert":    {class: classCarried, allowed: true, fillsEmpty: true},
	"sslnegotiation": {class: classCarried, allowed: true, check: checkEnum("postgres", "direct"), value: "postgres", fillsEmpty: true},

	"passfile":      {class: classSecret},
	"sslcert":       {class: classSecret},
	"sslkey":        {class: classSecret},
	"sslpassword":   {class: classSecret},
	"sslcertmode":   {class: classSecret, value: "disable"},
	"gssencmode":    {class: classSecret, allowed: true, check: checkEnum("disable"), value: "disable"},
	"gssdelegation": {class: classSecret, value: "0"},
	"krbsrvname":    {class: classSecret, value: "postgres"},
	"gsslib":        {class: classSecret},

	"hostaddr":                 {class: classInternal, allowed: true, check: checkHostaddr},
	"options":                  {class: classInternal},
	"sslcrl":                   {class: classInternal},
	"sslcrldir":                {class: classInternal},
	"channel_binding":          {class: classInternal, allowed: true, check: checkEnum("disable", "prefer", "require"), value: "prefer"},
	"require_auth":             {class: classInternal, value: allowedAuth},
	"ssl_min_protocol_version": {class: classInternal, value: "TLSv1.2"},
	"ssl_max_protocol_version": {class: classInternal},

	"application_name":     {class: classOperational, allowed: true, check: checkText},
	"connect_timeout":      {class: classOperational, allowed: true, check: checkInt},
	"target_session_attrs": {class: classOperational, allowed: true, check: checkEnum("any", "read-write", "read-only", "primary", "standby", "prefer-standby")},
	"load_balance_hosts":   {class: classOperational, allowed: true, check: checkEnum("disable")},
	"client_encoding":      {class: classOperational},
	"sslsni":               {class: classOperational},
	"sslcompression":       {class: classOperational},
	"requiressl":           {class: classOperational},
	"requirepeer":          {class: classOperational},
	"min_protocol_version": {class: classOperational},
	"max_protocol_version": {class: classOperational},

	"keepalives":                {class: classStringOnly, allowed: true, check: checkInt},
	"keepalives_idle":           {class: classStringOnly, allowed: true, check: checkInt},
	"keepalives_interval":       {class: classStringOnly, allowed: true, check: checkInt},
	"keepalives_count":          {class: classStringOnly, allowed: true, check: checkInt},
	"tcp_user_timeout":          {class: classStringOnly},
	"fallback_application_name": {class: classStringOnly},
	"replication":               {class: classStringOnly},
	"sslkeylogfile":             {class: classStringOnly},
	"scram_client_key":          {class: classStringOnly},
	"scram_server_key":          {class: classStringOnly},
	"oauth_client_id":           {class: classStringOnly},
	"oauth_client_secret":       {class: classStringOnly},
	"oauth_issuer":              {class: classStringOnly},
	"oauth_scope":               {class: classStringOnly},

	"service":     {class: classService},
	"servicefile": {class: classService},
}

// IsParam reports whether name is one of libpq's connection parameters, spelled as libpq spells
// it, for a caller that has names from somewhere other than a string (the options of a statement).
func IsParam(name string) bool {
	_, ok := params[name]
	return ok
}

// paramName is a parameter name for an error message: only a libpq name is shown, as other text
// may be part of a password (an unquoted "password=ab cd=ef" has a parameter "cd").
func paramName(key string) string {
	if IsParam(key) {
		return key
	}
	return "(unrecognized)"
}

// namesWhere lists, sorted, the parameters of the table that satisfy keep.
func namesWhere(keep func(param) bool) []string {
	var names []string
	for name, p := range params {
		if keep(p) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// LibpqString renders the string every client of the connection is dialed with (see params): the
// fields of the connection, the pin of each secret and internal parameter that the connection does
// not set, and each parameter that it sets and may carry. The operational, string-only and service
// parameters that it does not set are left out.
func (c *Conn) LibpqString() string {
	own := c.values()
	kv := map[string]string{}
	for name, p := range params {
		switch value, ok := own[name]; {
		case ok && (p.class == classCarried || p.allowed):
			kv[name] = value
		case p.class == classSecret || p.class == classInternal || p.fillsEmpty:
			kv[name] = p.value
		}
	}
	return pgutil.ConnInfoString(kv)
}

// validText reports whether a value is text libpq reads as it is written: valid UTF-8 without a
// NUL, which ends a string for libpq.
func validText(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

var (
	errText         = errors.New("it is not valid UTF-8 text without NUL")
	errUnrecognized = errors.New("a parameter is not one of libpq's connection parameters (unrecognized)")
)

func checkText(value string) error {
	if !validText(value) {
		return errText
	}
	return nil
}

// checkInt is libpq's integer, stricter: digits only, no sign or surrounding white space.
func checkInt(value string) error {
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil || n < 0 || value != strconv.FormatInt(n, 10) {
		return errors.New("it must be a whole number of at most 2147483647")
	}
	return nil
}

func checkEnum(values ...string) func(string) error {
	return func(value string) error {
		if !slices.Contains(values, value) {
			return fmt.Errorf("it must be one of %s", strings.Join(values, ", "))
		}
		return nil
	}
}

// checkHostaddr is libpq's rule for one entry of hostaddr: an IP address, which libpq does not
// resolve. It is a list, with an entry for each host (Validate).
func checkHostaddr(value string) error {
	for entry := range strings.SplitSeq(value, ",") {
		if addr, err := netip.ParseAddr(entry); err != nil || addr.Zone() != "" {
			return errors.New("it must be a comma-separated list of IP addresses")
		}
	}
	return nil
}
