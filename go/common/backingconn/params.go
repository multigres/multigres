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
)

// param describes one libpq connection parameter.
type param struct {
	// carried is true for the parameters that are fields of Conn; the others a connection may
	// carry are in Conn.Params.
	carried bool
	// allowed is true when a connection may name the parameter. Every other one is refused.
	allowed bool
	// check is libpq's rule for a value in Conn.Params, or for a carried field that takes only
	// some values (sslmode, sslnegotiation), which Validate applies when it is not empty; nil
	// accepts any text. libpq takes the values exactly, so a check does not fold case or trim.
	// The error never repeats the value.
	check func(value string) error
}

// params is the one table the closed set of parameters a connection may carry, and the checks of
// their values, are derived from. Parameters of later libpq versions (servicefile, sslkeylogfile,
// min/max_protocol_version, scram_*, oauth_*) are listed and refused.
var params = map[string]param{
	"host":           {carried: true, allowed: true},
	"port":           {carried: true, allowed: true},
	"dbname":         {carried: true, allowed: true},
	"user":           {carried: true, allowed: true},
	"password":       {carried: true, allowed: true},
	"sslmode":        {carried: true, allowed: true, check: checkEnum("disable", "prefer", "require", "verify-ca", "verify-full")},
	"sslrootcert":    {carried: true, allowed: true},
	"sslnegotiation": {carried: true, allowed: true, check: checkEnum("postgres", "direct")},

	"hostaddr":             {allowed: true, check: checkHostaddr},
	"application_name":     {allowed: true, check: checkText},
	"connect_timeout":      {allowed: true, check: checkInt},
	"keepalives":           {allowed: true, check: checkInt},
	"keepalives_idle":      {allowed: true, check: checkInt},
	"keepalives_interval":  {allowed: true, check: checkInt},
	"keepalives_count":     {allowed: true, check: checkInt},
	"gssencmode":           {allowed: true, check: checkEnum("disable", "prefer")},
	"channel_binding":      {allowed: true, check: checkEnum("disable", "prefer", "require")},
	"target_session_attrs": {allowed: true, check: checkEnum("any", "read-write", "read-only", "primary", "standby", "prefer-standby")},
	"load_balance_hosts":   {allowed: true, check: checkEnum("disable")},

	// Not allowed: each reads a file, a service or a secret where the connection is dialed,
	// changes what the session is (a walsender, a role, options), or is one a client cannot honor.
	"passfile":                  {},
	"service":                   {},
	"servicefile":               {},
	"options":                   {},
	"client_encoding":           {},
	"fallback_application_name": {},
	"sslcompression":            {},
	"sslcert":                   {},
	"sslkey":                    {},
	"sslpassword":               {},
	"sslcertmode":               {},
	"tcp_user_timeout":          {},
	"sslcrl":                    {},
	"sslcrldir":                 {},
	"sslsni":                    {},
	"sslkeylogfile":             {},
	"requirepeer":               {},
	"require_auth":              {},
	"ssl_min_protocol_version":  {},
	"ssl_max_protocol_version":  {},
	"min_protocol_version":      {},
	"max_protocol_version":      {},
	"krbsrvname":                {},
	"gsslib":                    {},
	"gssdelegation":             {},
	"replication":               {},
	"requiressl":                {},
	"scram_client_key":          {},
	"scram_server_key":          {},
	"oauth_client_id":           {},
	"oauth_client_secret":       {},
	"oauth_issuer":              {},
	"oauth_scope":               {},
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
