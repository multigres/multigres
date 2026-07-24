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

package pgsettings

// reportableGUCs maps a canonical (ASCII-lowercased) GUC name to the exact
// spelling PostgreSQL uses in the ParameterStatus wire message. These are the
// GUC_REPORT parameters (guc_tables.c) that a client can change with SET: when
// their value changes, PostgreSQL reports the new value so the client can keep
// its own state in sync (e.g. psql re-parsing input under standard_conforming_
// strings, or a driver tracking client_encoding / DateStyle).
//
// The display spelling matters: clients index ParameterStatus by the exact name
// on the wire, so DateStyle/TimeZone/IntervalStyle must keep their mixed case.
// Read-only GUC_REPORT parameters (server_version, is_superuser, ...) are
// omitted — a client cannot SET them, so they are never re-reported mid-session.
var reportableGUCs = map[string]string{
	"application_name":              "application_name",
	"client_encoding":               "client_encoding",
	"datestyle":                     "DateStyle",
	"default_transaction_read_only": "default_transaction_read_only",
	"intervalstyle":                 "IntervalStyle",
	"session_authorization":         "session_authorization",
	"standard_conforming_strings":   "standard_conforming_strings",
	"timezone":                      "TimeZone",
}

// ReportableGUCName returns the ParameterStatus display name for a client-
// settable GUC_REPORT parameter and whether name is one. name may be given in
// any ASCII case; it is canonicalized with CanonicalGUCName before lookup.
func ReportableGUCName(name string) (string, bool) {
	display, ok := reportableGUCs[CanonicalGUCName(name)]
	return display, ok
}
