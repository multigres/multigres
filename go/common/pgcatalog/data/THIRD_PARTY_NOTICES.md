# Third-Party Notices — PostgreSQL Catalog Data

The `.dat` files in this directory are byte-identical copies of PostgreSQL's
bootstrap catalog data files, redistributed under the PostgreSQL License
reproduced below.

## File provenance

| File(s)            | Upstream source                                 | Version                                               |
| ------------------ | ----------------------------------------------- | ----------------------------------------------------- |
| `pg_type.dat`      | postgres `src/include/catalog/pg_type.dat`      | REL_17_6 (`7885b94dd81b98bbab9ed878680d156df7bf857f`) |
| `pg_proc.dat`      | postgres `src/include/catalog/pg_proc.dat`      | REL_17_6 (`7885b94dd81b98bbab9ed878680d156df7bf857f`) |
| `pg_operator.dat`  | postgres `src/include/catalog/pg_operator.dat`  | REL_17_6 (`7885b94dd81b98bbab9ed878680d156df7bf857f`) |
| `pg_cast.dat`      | postgres `src/include/catalog/pg_cast.dat`      | REL_17_6 (`7885b94dd81b98bbab9ed878680d156df7bf857f`) |
| `pg_collation.dat` | postgres `src/include/catalog/pg_collation.dat` | REL_17_6 (`7885b94dd81b98bbab9ed878680d156df7bf857f`) |

No transformations are applied: the files are exact copies of the upstream
sources at the version above. When updating to a new PostgreSQL release,
re-copy the files verbatim, update this table, and regenerate with
`make pgcatalog`.

---

## PostgreSQL catalog data — PostgreSQL License

Source: <https://github.com/postgres/postgres> (`src/include/catalog/*.dat`)

```text
PostgreSQL Database Management System
(also known as Postgres, formerly known as Postgres95)

Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group

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
