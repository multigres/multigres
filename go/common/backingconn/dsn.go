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
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/tools/pgutil"
)

// ParseDSN decodes a libpq connection string, a URI
//
//	postgres://user:password@host:port/dbname?sslmode=...
//
// or keyword=value pairs. It is stricter than libpq, never looser, so that whatever it accepts
// libpq reads the same way. It refuses:
//
//   - a parameter name that is not exactly libpq's (HOST, SslMode), and a parameter given twice,
//     where libpq takes the last;
//   - a keyword-form token without "=", which a lenient tokenizer drops along with the rest of the
//     string (password=abc def is the password abc and a dropped "def");
//   - a keyword-form value that does not start directly after "=": libpq skips the white space
//     and reads the next token as the value (password= sslmode=require is the password
//     "sslmode=require"), so an empty value is written ”;
//   - in a URI, "#" (libpq gives it no meaning, a URL parser would cut the query there), a query
//     parameter without "=" or whose value has a raw "=" or white space (libpq: "extra key/value
//     separator" and "unexpected spaces"; write %3D and %20), and %00;
//   - a value that is not valid UTF-8 without NUL, where libpq stops reading at a NUL;
//   - an empty sslmode or sslnegotiation, which a lenient parser would drop.
//
// A libpq parameter that a backing connection does not carry (sslcert, options, hostaddr, ...) is
// kept in Conn.Params, not dropped: dropping one could weaken the connection or send it to
// another server. Whether a connection that has any is usable is for the caller to decide
// (Conn.ForPooler refuses it; the migrator passes it on to its own client).
//
// Nothing is read from the environment or a password file. An error never carries any part of
// the string, which holds the password; at most it names a libpq parameter.
func ParseDSN(dsn string) (*Conn, error) {
	if !validText(dsn) {
		return nil, errText
	}
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
	conn := &Conn{}
	for _, key := range slices.Sorted(maps.Keys(params)) {
		value := params[key]
		switch key {
		case "host":
			if value != "" {
				conn.Hosts = strings.Split(value, ",")
			}
		case "port":
			if value == "" {
				continue
			}
			for p := range strings.SplitSeq(value, ",") {
				port, err := strconv.ParseUint(p, 10, 16)
				if err != nil {
					return nil, errors.New("port is not a valid port number")
				}
				conn.Ports = append(conn.Ports, int(port))
			}
		case "dbname":
			conn.Database = value
		case "user":
			conn.User = value
		case "password":
			conn.Password = value
		case "sslmode":
			if value == "" {
				return nil, errors.New("sslmode is empty")
			}
			conn.SSLMode = value
		case "sslrootcert":
			conn.SSLRootCert = value
		case "sslnegotiation":
			if value == "" {
				return nil, errors.New("sslnegotiation is empty")
			}
			conn.SSLNegotiation = value
		default:
			if !IsParam(key) {
				return nil, errUnrecognized
			}
			if conn.Params == nil {
				conn.Params = map[string]string{}
			}
			conn.Params[key] = value
		}
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
// literal in both, and whitespace may precede "=". It differs from libpq only by refusing what
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
		if i < len(s) && isSpace(s[i]) {
			return nil, errors.New(`a value must start directly after "=": libpq skips white space there and reads the next word as the value; write an empty value as ''`)
		}

		var value strings.Builder
		quoted := i < len(s) && s[i] == '\''
		if !quoted && i == len(s) {
			return nil, errors.New(`a value is empty; write an empty value as ''`)
		}
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
//
// It does not use net/url, which reads the authority as one host: given a list of servers it
// keeps the last and drops the rest without saying so. The URI is split here as libpq splits it,
// and %XX is decoded in each part.
func parseURI(raw string) (map[string]string, error) {
	if strings.Contains(raw, "#") {
		return nil, errors.New("the URI contains '#', which libpq does not treat as a fragment; write it as %23")
	}
	main, query, hasQuery := strings.Cut(raw, "?")
	_, rest, _ := strings.Cut(main, "://")
	authority, path, _ := strings.Cut(rest, "/")

	errEncoding := errors.New("the URI has an invalid percent-encoding")
	unescape := func(part string) (string, error) {
		value, err := url.PathUnescape(part)
		if err != nil || !validText(value) {
			return "", errEncoding
		}
		return value, nil
	}
	params := map[string]string{}
	if strings.Count(authority, "@") > 1 {
		return nil, errors.New("the URI has more than one '@' before the host; write an '@' in the user name or password as %40")
	}
	hostPart := authority
	if userinfo, after, ok := strings.Cut(authority, "@"); ok {
		hostPart = after
		user, password, hasPassword := strings.Cut(userinfo, ":")
		user, err := unescape(user)
		if err != nil {
			return nil, errEncoding
		}
		if user != "" {
			params["user"] = user
		}
		if hasPassword {
			password, err := unescape(password)
			if err != nil {
				return nil, errEncoding
			}
			params["password"] = password
		}
	}
	if hostPart != "" {
		hosts, ports, err := splitAuthority(hostPart)
		if err != nil {
			return nil, err
		}
		for i, h := range hosts {
			if hosts[i], err = unescape(h); err != nil {
				return nil, errEncoding
			}
		}
		if len(hosts) > 0 {
			params["host"] = strings.Join(hosts, ",")
		}
		if len(ports) > 0 {
			params["port"] = strings.Join(ports, ",")
		}
	}
	db, err := unescape(path)
	if err != nil {
		return nil, errEncoding
	}
	if db != "" {
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
		if strings.Contains(rawValue, "=") {
			return nil, errors.New(`a URI query value has a raw "=": libpq refuses it as an extra key/value separator; write it %3D`)
		}
		if strings.ContainsFunc(rawValue, func(r rune) bool { return r < 0x80 && isSpace(byte(r)) }) {
			return nil, errors.New("a URI query value has white space in it: libpq refuses it; write a space %20")
		}
		key, err := unescape(rawKey)
		if err != nil {
			return nil, errors.New("the URI has an invalid percent-encoding in its query")
		}
		value, err := unescape(rawValue)
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

// splitAuthority splits the host part of a URI, which libpq reads as a comma-separated list of
// host[:port] (an IPv6 address in brackets), into its hosts and ports. Every host has a port or
// none does; libpq would give the others a default, which a list of ports cannot say.
func splitAuthority(authority string) (hosts, ports []string, err error) {
	elems := strings.Split(authority, ",")
	for _, elem := range elems {
		host, port, err := splitHostPort(elem)
		if err != nil {
			return nil, nil, err
		}
		hosts = append(hosts, host)
		if port != "" {
			ports = append(ports, port)
		}
	}
	if len(ports) != 0 && len(ports) != len(elems) {
		return nil, nil, errors.New("in a URI, every host has a port or none does")
	}
	if len(hosts) == 1 && hosts[0] == "" {
		hosts = nil
	}
	return hosts, ports, nil
}

func splitHostPort(elem string) (host, port string, err error) {
	errBad := errors.New("the URI is not valid (for example, its port is not a number)")
	if rest, ok := strings.CutPrefix(elem, "["); ok {
		host, after, found := strings.Cut(rest, "]")
		if !found {
			return "", "", errBad
		}
		if after == "" {
			return host, "", nil
		}
		port, ok := strings.CutPrefix(after, ":")
		if !ok {
			return "", "", errBad
		}
		return host, port, nil
	}
	host, port, hasPort := strings.Cut(elem, ":")
	if hasPort && strings.Contains(port, ":") {
		return "", "", errBad
	}
	return host, port, nil
}
