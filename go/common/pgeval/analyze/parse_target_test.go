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

package analyze_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/pgeval/analyze"
)

// The expectations are PostgreSQL 17's own column names for the same
// expressions (psql shows them as the header of SELECT <expr>).
func TestFigureColname(t *testing.T) {
	cases := map[string]string{
		"1":                                      "?column?",
		"$1":                                     "?column?",
		"(1)":                                    "?column?",
		"1 + 2":                                  "?column?",
		"-1":                                     "?column?",
		"NOT true":                               "?column?",
		"1 IS NULL":                              "?column?",
		"true IS TRUE":                           "?column?",
		"1 IS DISTINCT FROM 2":                   "?column?",
		"1 IN (1, 2)":                            "?column?",
		"1 BETWEEN 0 AND 2":                      "?column?",
		"abs(-1)":                                "abs",
		"pg_catalog.abs(-1)":                     "abs",
		"nullif(1, 2)":                           "nullif",
		"coalesce(1, 2)":                         "coalesce",
		"greatest(1, 2)":                         "greatest",
		"least(1, 2)":                            "least",
		"1::int4":                                "int4",
		"1::integer":                             "int4",
		"CAST(1 AS bigint)":                      "int8",
		"'1'::pg_catalog.int8":                   "int8",
		"int4 '1'":                               "int4",
		"abs(1)::int8":                           "abs",
		"(1 + 2)::int8":                          "int8",
		"1::int8::int4":                          "int4",
		"CASE WHEN true THEN 1 END":              "case",
		"CASE WHEN true THEN 1 ELSE 2 END":       "case",
		"CASE WHEN true THEN 1 ELSE abs(2) END":  "abs",
		"CASE WHEN true THEN 1 ELSE 2::int8 END": "case",
		"CASE 1 WHEN 1 THEN 2 END":               "case",
		"ARRAY[1, 2]":                            "array",
		"ROW(1, 2)":                              "row",
		"x":                                      "x",
		"t.x":                                    "x",
		"t.*":                                    "t",
		"(x).y":                                  "y",
		"(x)[1]":                                 "x",
		"x COLLATE \"C\"":                        "x",
		"EXISTS (SELECT 1)":                      "exists",
		"ARRAY(SELECT 1)":                        "array",
		"(SELECT 1)":                             "?column?",
		"(SELECT abs(1))":                        "abs",
		"(SELECT 1 AS n)":                        "n",
		"(SELECT 1)::int8":                       "?column?",
		"(SELECT 1 FROM t WHERE false)":          "?column?",
		"(SELECT x FROM t)":                      "x",
		"(SELECT t.x FROM t)":                    "x",
		"(SELECT abs(1) UNION SELECT 2 LIMIT 1)": "abs",
		"(SELECT 1 AS n UNION ALL SELECT 2)":     "n",
		"((SELECT abs(1) UNION SELECT 2) INTERSECT SELECT 3)": "abs",
		"(SELECT 1 EXCEPT SELECT abs(2))":                     "?column?",
		"(SELECT abs(1) UNION SELECT 2)::int8":                "abs",
		"(VALUES (1))":                                        "column1",
		"(VALUES (1), (2) LIMIT 1)":                           "column1",
		"(VALUES (abs(1)))":                                   "column1",
		"(VALUES (1) UNION SELECT abs(2))":                    "column1",
		"(SELECT abs(1) UNION VALUES (2))":                    "abs",
		"(WITH c AS (SELECT 1) SELECT abs(2))":                "abs",
		"(WITH c AS (SELECT 1) SELECT 2 UNION SELECT 3)":      "?column?",
		"1 = ANY (SELECT 1)":                                  "?column?",
		"current_date":                                        "current_date",
		"current_time":                                        "current_time",
		"current_timestamp(3)":                                "current_timestamp",
		"localtime":                                           "localtime",
		"localtimestamp":                                      "localtimestamp",
		"current_role":                                        "current_role",
		"current_user":                                        "current_user",
		"user":                                                "user",
		"session_user":                                        "session_user",
		"current_catalog":                                     "current_catalog",
		"current_schema":                                      "current_schema",
		"xmlconcat('<a/>')":                                   "xmlconcat",
		"xmlelement(name a)":                                  "xmlelement",
		"xmlforest(1 AS a)":                                   "xmlforest",
		"xmlparse(content '<a/>')":                            "xmlparse",
		"xmlpi(name a)":                                       "xmlpi",
		"xmlroot('<a/>', version '1.0')":                      "xmlroot",
		"xmlserialize(content '<a/>' AS text)":                "xmlserialize",
		"'<a/>' IS DOCUMENT":                                  "?column?",
		"json('{}')":                                          "json",
		"json_scalar(1)":                                      "json_scalar",
		"json_serialize('{}')":                                "json_serialize",
		"json_object('a': 1)":                                 "json_object",
		"json_array(1, 2)":                                    "json_array",
		"json_array(SELECT 1)":                                "json_array",
		"json_objectagg('a': 1)":                              "json_objectagg",
		"json_arrayagg(1)":                                    "json_arrayagg",
		"json_exists('{}', '$')":                              "json_exists",
		"json_query('{}', '$')":                               "json_query",
		"json_value('{}', '$')":                               "json_value",
		"grouping(1)":                                         "grouping",
		"merge_action()":                                      "merge_action",
	}
	for sql, want := range cases {
		assert.Equal(t, want, analyze.FigureColname(parseExpr(t, sql)), sql)
	}
	assert.Equal(t, "?column?", analyze.FigureColname(nil))
}

// TestFigureColname_StarIsNotExpanded records the one place the port reads
// the raw tree where upstream has already expanded it: a scalar subquery
// whose first target is `*`. PostgreSQL names it after the first column of
// the range table entry (inner_name, column1, or a table's first column,
// which needs the catalog); the port returns the raw ColumnRef's figured
// name. The gateway never serves a SubLink, so no client sees this.
func TestFigureColname_StarIsNotExpanded(t *testing.T) {
	for sql, port := range map[string]string{
		"(SELECT * FROM (SELECT 1 AS inner_name) s)": "?column?", // PostgreSQL: inner_name
		"(SELECT * FROM (VALUES (1)) v)":             "?column?", // PostgreSQL: column1
		"(SELECT t.* FROM t)":                        "t",        // PostgreSQL: t's first column
	} {
		assert.Equal(t, port, analyze.FigureColname(parseExpr(t, sql)), sql)
	}
}
