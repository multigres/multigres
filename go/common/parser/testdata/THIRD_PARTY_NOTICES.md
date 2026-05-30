# Third-Party Notices — Parser Test Data

The SQL test corpora in this directory are derived from third-party open-source
projects. The originals are redistributed (and, where noted, lightly transformed
into JSON) under the licenses reproduced below. Each license's "retain the
copyright notice, this list of conditions and the following disclaimer" clause is
satisfied by this file.

Transformations applied by the Multigres project are limited to: splitting source
files into individual statements, normalizing whitespace, stripping comments, and
recording the expected deparser output. The SQL statements themselves are
unchanged in substance.

## File provenance

| File(s)                                                   | Upstream source                                                                                                                                                                 | License                  |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| `deparse_cases.json`                                      | libpg_query `test/deparse_tests.c` (tags `17-6.1.0`, `17-6.2.2`); pg_query Ruby gem `spec/lib/pg_query/deparse_spec.rb`                                                         | BSD-3-Clause (pganalyze) |
| `deparse_complex_cases.json` — `deparse/*` entries        | libpg_query `test/sql/deparse/*.sql` (tag `17-6.2.2`)                                                                                                                           | BSD-3-Clause (pganalyze) |
| `deparse_complex_cases.json` — `deparse-depesz/*` entries | libpg_query `test/sql/deparse-depesz/**/*.psql` (tag `17-6.2.2`), based on depesz' pg-sql-prettyprinter                                                                         | BSD-3-Clause (depesz)    |
| `postgres/*.json`                                         | PostgreSQL regression suite (`src/test/regress/sql/*.sql`, `src/pl/plpgsql/src/sql/*.sql`), obtained via libpg_query `test/sql/postgres_regress` and `test/sql/plpgsql_regress` | PostgreSQL License       |

`convert_postgres_tests.py` is the conversion script used to produce the
`postgres/*.json` files from upstream `.sql` sources.

---

## libpg_query / pg_query (pganalyze) — BSD-3-Clause

Source: <https://github.com/pganalyze/libpg_query>, <https://github.com/pganalyze/pg_query>

```text
Copyright (c) 2015, Lukas Fittl <lukas@fittl.com>
Copyright (c) 2016-2023, Duboce Labs, Inc. (pganalyze) <team@pganalyze.com>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

* Neither the name of pg_query nor the names of its contributors may be used
to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
```

---

## depesz pg-sql-prettyprinter fixtures — BSD-3-Clause

Source: <https://gitlab.com/depesz/pg-sql-prettyprinter/> (as vendored and modified
in libpg_query `test/sql/deparse-depesz`)

```text
Copyright (c) 2022-2023, Hubert 'depesz' Lubaczewski <depesz@depesz.com>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
   may be used to endorse or promote products derived from this software
   without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

---

## PostgreSQL regression suite — PostgreSQL License

Source: <https://github.com/postgres/postgres> (`src/test/regress/sql`,
`src/pl/plpgsql/src/sql`)

```text
PostgreSQL Database Management System
(also known as Postgres, formerly known as Postgres95)

Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group

Portions Copyright (c) 1994, The Regents of the University of California

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement
is hereby granted, provided that the above copyright notice and this
paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
SUCH DAMAGE.

THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
"AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
```
