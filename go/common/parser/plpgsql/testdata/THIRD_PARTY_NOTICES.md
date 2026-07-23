# Third-Party Notices — PL/pgSQL Test Data

`pg_corpus_cases.json` contains PL/pgSQL function bodies extracted from
PostgreSQL's PL/pgSQL regression tests (`src/pl/plpgsql/src/sql/*.sql`,
PostgreSQL 17). They are redistributed under the PostgreSQL License, reproduced
below.

Transformations applied by the Multigres project are limited to: extracting each
`CREATE FUNCTION` / `CREATE PROCEDURE … LANGUAGE plpgsql` and `DO` block's body,
de-duplicating, and recording the expected parse error for PostgreSQL's own
negative tests (see `TestGeneratePGCorpusCases` in `../corpus_test.go`). The
PL/pgSQL bodies themselves are unchanged in substance.

## File provenance

| File                   | Upstream source                                           | License            |
| ---------------------- | --------------------------------------------------------- | ------------------ |
| `pg_corpus_cases.json` | PostgreSQL `src/pl/plpgsql/src/sql/*.sql` (PostgreSQL 17) | PostgreSQL License |

## PostgreSQL License

> PostgreSQL Database Management System
> (formerly known as Postgres, then as Postgres95)
>
> Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
>
> Portions Copyright (c) 1994, The Regents of the University of California
>
> Permission to use, copy, modify, and distribute this software and its
> documentation for any purpose, without fee, and without a written agreement is
> hereby granted, provided that the above copyright notice and this paragraph and
> the following two paragraphs appear in all copies.
>
> IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
> DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
> PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
> THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
>
> THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING,
> BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
> PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND
> THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT,
> UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
