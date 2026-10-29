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

package manager

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/tools/pgutil"
)

// connectionStore reads the connection directory from the sharding catalog on
// the default primary, through the admin pool: the named connections of
// multigres.connections.
type connectionStore struct {
	query func(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error)
}

func newConnectionStore(pm *MultipoolerManager) *connectionStore {
	return &connectionStore{query: pm.adminQueryArgs}
}

// BackingConnection reads the named connection and decodes its stored form, a
// libpq connection string in the dsn column (a postgres:// URI or keyword=value
// pairs, the form the migrator stores), into the structured connection the
// contract carries. A dsn that does not name exactly one server is refused.
func (s *connectionStore) BackingConnection(ctx context.Context, name string) (*multipoolerservicepb.BackingConnection, bool, error) {
	result, err := s.query(ctx, "SELECT dsn FROM multigres.connections WHERE name = $1", name)
	if err != nil {
		return nil, false, err
	}
	if len(result.Rows) == 0 {
		return nil, false, nil
	}
	var dsn string
	if err := executor.ScanSingleRow(result, &dsn); err != nil {
		return nil, false, errors.Join(errors.New("decode backing connection"), err)
	}
	conn, err := parseBackingDSN(dsn)
	if err != nil {
		return nil, false, mterrors.Wrap(err, fmt.Sprintf("connection %q", name))
	}
	conn.Name = name
	return conn, true, nil
}

// carriedConnParams are the libpq parameters a backing connection carries: where the server
// is, who to connect as, and the TLS settings the pooler honors. Any other parameter is refused
// rather than dropped, since dropping one can weaken the connection (channel_binding=require,
// sslcert and sslkey, options) or send it to a different server (hostaddr, a host list,
// target_session_attrs, load_balance_hosts).
var carriedConnParams = map[string]bool{
	"host": true, "port": true, "dbname": true, "user": true, "password": true,
	"sslmode": true, "sslrootcert": true, "sslnegotiation": true,
}

// errInvalidBackingDSN is a stored dsn that cannot name the one server a backing connection
// must. It is a fact about the persisted connection, not a transient failure, so it is
// FAILED_PRECONDITION. The message never carries a value of the dsn, which holds the password.
func errInvalidBackingDSN(format string, args ...any) error {
	return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "backing connection dsn is not usable: "+format, args...)
}

// parseBackingDSN decodes a stored libpq connection string, in either form:
//
//	postgres://user:password@host:port/dbname?sslmode=...&sslrootcert=...&sslnegotiation=...
//	host=... port=... dbname=... user=... password=... sslmode=...
//
// A postgres:// or postgresql:// URI is parsed here and anything else with the libpq keyword
// tokenizer (pgutil.ParseConnInfo), which reads nothing from the environment or a password file:
// the connection is exactly what the catalog stores. Both forms then go through one validator
// (backingFromParams).
func parseBackingDSN(raw string) (*multipoolerservicepb.BackingConnection, error) {
	var params map[string]string
	if strings.HasPrefix(raw, "postgres://") || strings.HasPrefix(raw, "postgresql://") {
		var err error
		if params, err = uriConnParams(raw); err != nil {
			return nil, err
		}
	} else {
		params = pgutil.ParseConnInfo(raw)
		if len(params) == 0 {
			return nil, errInvalidBackingDSN("it is neither a postgres:// URI nor keyword=value pairs")
		}
	}
	return backingFromParams(params)
}

// uriConnParams maps a postgres:// URI to the parameters it sets. The authority and path give
// host, port, user, password and dbname; a query parameter that sets one of them again is
// ambiguous and refused, as is a parameter given twice.
func uriConnParams(raw string) (map[string]string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		// url.Parse's error quotes the URL, which holds the password.
		return nil, errInvalidBackingDSN("the URI is not valid (for example, its port is not a number)")
	}
	params := map[string]string{}
	if strings.Contains(u.Host, ",") {
		return nil, errInvalidBackingDSN("it names more than one host; a single server is required")
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
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, errInvalidBackingDSN("the URI has an invalid query")
	}
	for key, values := range query {
		key = strings.ToLower(key)
		if len(values) != 1 {
			return nil, errInvalidBackingDSN("parameter %s is given more than once", paramName(key))
		}
		if _, dup := params[key]; dup {
			return nil, errInvalidBackingDSN("parameter %s is set both in the URI and as a query parameter", paramName(key))
		}
		params[key] = values[0]
	}
	return params, nil
}

// paramName renders a parameter name for an error message: only a plain libpq keyword is shown,
// since a malformed one may be part of the secret.
func paramName(key string) string {
	for _, r := range key {
		if (r < 'a' || r > 'z') && r != '_' {
			return "(unrecognized)"
		}
	}
	return key
}

// backingFromParams validates the parameters of either form and builds the connection: exactly
// one server, given explicitly (host, port, dbname and user), the TLS settings carried through,
// and no parameter that is not carried.
func backingFromParams(params map[string]string) (*multipoolerservicepb.BackingConnection, error) {
	for key := range params {
		if !carriedConnParams[key] {
			return nil, errInvalidBackingDSN("parameter %s is not supported; a backing connection carries only host, port, dbname, user, password, sslmode, sslrootcert and sslnegotiation", paramName(key))
		}
	}
	host := params["host"]
	switch {
	case host == "":
		return nil, errInvalidBackingDSN("host is required")
	case strings.Contains(host, ","):
		return nil, errInvalidBackingDSN("it names more than one host; a single server is required")
	case strings.HasPrefix(host, "/"):
		return nil, errInvalidBackingDSN("host is a socket directory; a network host is required")
	}
	if params["port"] == "" {
		return nil, errInvalidBackingDSN("port is required")
	}
	port, err := strconv.ParseUint(params["port"], 10, 16)
	if err != nil || port == 0 {
		return nil, errInvalidBackingDSN("port is not a single valid port number")
	}
	if params["dbname"] == "" {
		return nil, errInvalidBackingDSN("dbname is required")
	}
	if params["user"] == "" {
		return nil, errInvalidBackingDSN("user is required")
	}
	return &multipoolerservicepb.BackingConnection{
		Host:           host,
		Port:           int32(port),
		Database:       params["dbname"],
		User:           params["user"],
		Password:       params["password"],
		SslMode:        params["sslmode"],
		SslRootCert:    params["sslrootcert"],
		SslNegotiation: params["sslnegotiation"],
	}, nil
}
