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
	"net/url"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/tools/pgutil"
)

// Conn is a libpq connection string decoded into the parts a backing connection carries. The
// host and the port are lists, as in libpq's host=a,b port=1,2; whether more than one server is
// served is decided by the caller (Endpoint), not by the decoding.
type Conn struct {
	Hosts          []string
	Ports          []int
	Database       string
	User           string
	Password       string
	SSLMode        string
	SSLRootCert    string
	SSLNegotiation string
}

// carriedParams are the libpq parameters a backing connection carries: where the server is, who
// to connect as, and the TLS settings the pooler honors. Any other parameter is refused rather
// than dropped, since dropping one can weaken the connection (channel_binding=require, sslcert
// and sslkey, options) or send it to a different server (hostaddr, target_session_attrs,
// load_balance_hosts).
var carriedParams = map[string]bool{
	"host": true, "port": true, "dbname": true, "user": true, "password": true,
	"sslmode": true, "sslrootcert": true, "sslnegotiation": true,
}

// libpqParams are the connection parameters libpq knows, which are the only names an error
// message may repeat. A name outside this list may be a fragment of a password (an unquoted
// "password=ab cd=ef" has a parameter "cd"), so it is never echoed.
var libpqParams = map[string]bool{
	"application_name": true, "channel_binding": true, "client_encoding": true,
	"connect_timeout": true, "dbname": true, "fallback_application_name": true,
	"gssdelegation": true, "gssencmode": true, "gsslib": true, "host": true, "hostaddr": true,
	"keepalives": true, "keepalives_count": true, "keepalives_idle": true,
	"keepalives_interval": true, "krbsrvname": true, "load_balance_hosts": true,
	"max_protocol_version": true, "min_protocol_version": true, "options": true,
	"passfile": true, "password": true, "port": true, "replication": true,
	"requirepeer": true, "requiressl": true, "scram_client_key": true,
	"scram_server_key": true, "service": true, "ssl_max_protocol_version": true,
	"ssl_min_protocol_version": true, "sslcert": true, "sslcertmode": true,
	"sslcompression": true, "sslcrl": true, "sslcrldir": true, "sslkey": true,
	"sslmode": true, "sslnegotiation": true, "sslpassword": true, "sslrootcert": true,
	"sslsni": true, "target_session_attrs": true, "tcp_user_timeout": true, "user": true,
}

// paramName renders a parameter name for an error message: only a libpq parameter name is
// shown, never text of the string that merely looks like one.
func paramName(key string) string {
	if libpqParams[key] {
		return key
	}
	return "(unrecognized)"
}

// ParseDSN decodes a libpq connection string, in either form:
//
//	postgres://user:password@host:port/dbname?sslmode=...&sslrootcert=...&sslnegotiation=...
//	host=... port=... dbname=... user=... password=... sslmode=...
//
// It is stricter than libpq, never looser, so that whatever it accepts libpq reads the same way:
//
//   - parameter names are exactly libpq's, in lower case, in both forms: HOST and SslMode are
//     refused, not folded;
//   - a parameter given twice is refused, in both forms, where libpq takes the last;
//   - a keyword-form token without "=" is refused, where a lenient tokenizer drops it and the
//     rest of the string with it (password=abc def is the password abc and a dropped "def");
//   - a URI containing "#" is refused: libpq gives it no meaning in a URI, and a URL parser
//     would cut the string there and drop the query;
//   - a URI query parameter without "=" is refused;
//   - a parameter the connection does not carry is refused, not dropped.
//
// Nothing is read from the environment or a password file: the connection is exactly what the
// string says. An error never carries any part of the string, which holds the password; at most
// it names a libpq parameter.
func ParseDSN(dsn string) (*Conn, error) {
	var (
		params map[string]string
		err    error
	)
	if pgutil.IsConnURI(dsn) {
		params, err = parseURI(dsn)
	} else {
		params, err = parseKeywordValue(dsn)
	}
	if err != nil {
		return nil, err
	}
	if len(params) == 0 {
		return nil, errors.New("it is neither a postgres:// URI nor keyword=value pairs")
	}
	return connFromParams(params)
}

func connFromParams(params map[string]string) (*Conn, error) {
	for key := range params {
		if !carriedParams[key] {
			return nil, fmt.Errorf("parameter %s is not supported; a backing connection carries only host, port, dbname, user, password, sslmode, sslrootcert and sslnegotiation", paramName(key))
		}
	}
	if params["port"] == "" {
		return nil, errors.New("port is required")
	}
	conn := &Conn{
		Database:       params["dbname"],
		User:           params["user"],
		Password:       params["password"],
		SSLMode:        params["sslmode"],
		SSLRootCert:    params["sslrootcert"],
		SSLNegotiation: params["sslnegotiation"],
	}
	if host, ok := params["host"]; ok {
		conn.Hosts = strings.Split(host, ",")
	}
	for p := range strings.SplitSeq(params["port"], ",") {
		port, err := strconv.ParseUint(p, 10, 16)
		if err != nil {
			return nil, errors.New("port is not a single valid port number")
		}
		conn.Ports = append(conn.Ports, int(port))
	}
	return conn, nil
}

// isSpace is libpq's whitespace in a connection string.
func isSpace(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	}
	return false
}

// parseKeywordValue tokenizes keyword=value pairs as libpq does (conninfo_parse): a value is a
// run of non-space characters or a '...' quoted string, a backslash makes the next character
// literal in both, and whitespace may surround "=". It differs from libpq only by refusing what
// libpq would accept with a surprising result (see ParseDSN) or silently drop.
func parseKeywordValue(s string) (map[string]string, error) {
	params := map[string]string{}
	for i := 0; ; {
		for i < len(s) && isSpace(s[i]) {
			i++
		}
		if i == len(s) {
			return params, nil
		}

		start := i
		for i < len(s) && s[i] != '=' && !isSpace(s[i]) {
			i++
		}
		key := s[start:i]
		for i < len(s) && isSpace(s[i]) {
			i++
		}
		if i == len(s) || s[i] != '=' {
			return nil, errors.New(`a parameter has no "="`)
		}
		i++
		for i < len(s) && isSpace(s[i]) {
			i++
		}

		var value strings.Builder
		quoted := i < len(s) && s[i] == '\''
		if quoted {
			i++
		}
		closed := !quoted
		for i < len(s) {
			c := s[i]
			if c == '\\' {
				i++
				if i == len(s) {
					return nil, errors.New("a value ends in a backslash")
				}
				value.WriteByte(s[i])
				i++
				continue
			}
			if quoted && c == '\'' {
				i++
				closed = true
				break
			}
			if !quoted && isSpace(c) {
				break
			}
			value.WriteByte(c)
			i++
		}
		if !closed {
			return nil, errors.New("a quoted value is not closed")
		}
		if _, dup := params[key]; dup {
			return nil, fmt.Errorf("parameter %s is given more than once", paramName(key))
		}
		params[key] = value.String()
	}
}

// parseURI maps a postgres:// URI to the parameters it sets. The authority and path give host,
// port, user, password and dbname; a query parameter that sets one of them again is ambiguous
// and refused, as is a parameter given twice.
func parseURI(raw string) (map[string]string, error) {
	if strings.Contains(raw, "#") {
		return nil, errors.New("the URI contains '#', which libpq does not treat as a fragment; write it as %23")
	}
	main, query, hasQuery := strings.Cut(raw, "?")
	u, err := url.Parse(main)
	if err != nil {
		// url.Parse's error quotes the URL, which holds the password.
		return nil, errors.New("the URI is not valid (for example, its port is not a number)")
	}
	params := map[string]string{}
	if strings.Contains(u.Host, ",") {
		return nil, ErrMultipleHosts
	}
	if h := u.Hostname(); h != "" {
		params["host"] = h
	}
	if p := u.Port(); p != "" {
		params["port"] = p
	}
	if u.User != nil {
		if name := u.User.Username(); name != "" {
			params["user"] = name
		}
		if pw, ok := u.User.Password(); ok {
			params["password"] = pw
		}
	}
	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		params["dbname"] = db
	}
	if !hasQuery || query == "" {
		return params, nil
	}

	seen := map[string]bool{}
	for piece := range strings.SplitSeq(query, "&") {
		// Only "&" separates parameters and only %XX is decoded, as in libpq; url.ParseQuery
		// would also turn "+" into a space and split on ";".
		rawKey, rawValue, ok := strings.Cut(piece, "=")
		if !ok {
			return nil, errors.New(`a URI query parameter has no "="`)
		}
		key, err := url.PathUnescape(rawKey)
		if err != nil {
			return nil, errors.New("the URI has an invalid percent-encoding in its query")
		}
		value, err := url.PathUnescape(rawValue)
		if err != nil {
			return nil, errors.New("the URI has an invalid percent-encoding in its query")
		}
		if seen[key] {
			return nil, fmt.Errorf("parameter %s is given more than once", paramName(key))
		}
		seen[key] = true
		if _, dup := params[key]; dup {
			return nil, fmt.Errorf("parameter %s is set both in the URI and as a query parameter", paramName(key))
		}
		params[key] = value
	}
	return params, nil
}
