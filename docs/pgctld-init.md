# pgctld init

`pgctld init` initializes a PostgreSQL data directory and applies any one-time,
day-0 setup that must happen before the permanent server starts serving. It is
the multigres equivalent of the `docker-library/postgres` entrypoint's
first-boot behavior.

Everything here runs **once**, when the data directory is empty. If the data
directory is already initialized, `init` is a no-op and returns immediately.

## What it does, in order

1. **`initdb`** creates the data directory with:
   - `--data-checksums`
   - `--auth-local=scram-sha-256 --auth-host=scram-sha-256`
   - `-U <pg-user>` — the superuser role
   - `--pwfile=<resolved password file>` — sets the **superuser** password (see
     [Superuser password](#superuser-password))
   - any extra tokens from `--pg-initdb-args`
2. **Generate `postgresql.conf`** from the template, appending any
   `--pg-initdb-extra-conf` snippets verbatim (last-write-wins).
3. **Transient server** — if any post-initdb step is requested (a non-default
   target database, init SQL, or an init-secrets file), pgctld starts a
   short-lived PostgreSQL instance on a **private temporary Unix socket** so it
   is invisible to multipooler, then runs steps 4–7 against it and stops it.
   1. **Create the target database** (only if `--pg-database` is not the default
      `postgres`).
   2. **Run `--pg-initdb-sql-dirs`** — bulk schema/migration directories, each
      under `SET SESSION AUTHORIZATION <role>`. Establishes the base schema and
      creates roles.
   3. **Run `--pg-initdb-sql-files`** — individual files applied on top as
      targeted overrides/patches.
   4. **Apply `--pg-init-secrets-file`** — role passwords and database settings.
      Runs last, after the roles it targets have been created by 3.ii/3.iii (see
      [Init secrets](#init-secrets)).

## Flags

Every flag has an environment-variable equivalent; the flag takes precedence.

| Flag                     | Env var                      | Default    | Purpose                                                                                                                                               |
| ------------------------ | ---------------------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--pg-user`, `-U`        | `POSTGRES_USER`              | `postgres` | Superuser role created by `initdb`.                                                                                                                   |
| `--pg-database`, `-D`    | `POSTGRES_DB`                | `postgres` | Target database. A non-default value is created on the transient instance.                                                                            |
| `--pg-password-file`     | `POSTGRES_PASSWORD_FILE`     | —          | Path to a file holding the **superuser** password (plaintext, first line). See [Superuser password](#superuser-password).                             |
| `--pg-initdb-args`       | `POSTGRES_INITDB_ARGS`       | —          | Extra tokens appended to the `initdb` command line.                                                                                                   |
| `--pg-initdb-sql-dirs`   | `POSTGRES_INITDB_SQL_DIRS`   | —          | Directories of `.sql` files run after initdb, in `role:path` format. Repeatable / comma-separated. See [Init SQL directories](#init-sql-directories). |
| `--pg-initdb-sql-files`  | `POSTGRES_INITDB_SQL_FILES`  | —          | Individual `.sql` files run after the directories, in order. Repeatable / comma-separated.                                                            |
| `--pg-initdb-extra-conf` | `POSTGRES_INITDB_EXTRA_CONF` | —          | `postgresql.conf` snippets appended verbatim to the generated config. Repeatable; last-write-wins.                                                    |
| `--pg-init-secrets-file` | `POSTGRES_INIT_SECRETS_FILE` | —          | JSON file of per-project role passwords and database settings. See [Init secrets](#init-secrets).                                                     |

> A legacy alias `--init-db-sql-file` is normalized to `--pg-initdb-sql-files`
> for backward compatibility.

## Superuser password

`initdb --pwfile` sets a password for exactly **one** role: the superuser
(`--pg-user`). The password is resolved in order:

1. `--pg-password-file` / `POSTGRES_PASSWORD_FILE` — the file path is handed to
   `initdb --pwfile` directly, so the plaintext never lands in a temp file.
2. `POSTGRES_PASSWORD` env var (legacy) — staged into a randomly named temp file
   only for the duration of the `initdb` exec, then removed.

Password files must be **single-line**; `initdb --pwfile` reads only the first
line.

All other roles are created **without** a password by the init SQL and must be
seeded separately — see [Init secrets](#init-secrets).

## Init SQL directories

Each `--pg-initdb-sql-dirs` entry is `role:path`. pgctld reads every `.sql` file
in `path` in lexicographic order and runs them in a single `psql` session
wrapped in:

```sql
SET SESSION AUTHORIZATION "<role>";
-- files, in order
RESET SESSION AUTHORIZATION;
```

so objects are owned by `<role>`. Each script runs with `ON_ERROR_STOP=1`, so a
failing statement aborts init. Directories run **before** `--pg-initdb-sql-files`.

## Init secrets

`--pg-init-secrets-file` points at a JSON file of **per-project day-0 state**
that cannot be baked into a shared image — role login credentials and database
settings. It is typically a mounted Kubernetes Secret, applied over the local
transient socket during init.

### Format

```json
{
  "roles": {
    "authenticator": "…",
    "supabase_auth_admin": "…"
  },
  "database_settings": {
    "postgres": { "app.settings.jwt_exp": "3600" }
  }
}
```

- **`roles`** — maps a login role (already created by the init SQL) to its
  password. The value is **opaque**: a plaintext password (Postgres hashes it
  per `password_encryption`) or a pre-hashed `SCRAM-SHA-256$…` verifier (stored
  verbatim). Do **not** list the superuser here — it is set via `--pwfile`.
- **`database_settings`** — maps a database name to GUCs applied via
  `ALTER DATABASE <db> SET <key> TO <value>` (e.g. `app.settings.jwt_exp`).
  Values are stored/consumed as text.

Unknown top-level fields are ignored, so a newer payload does not break an older
pgctld.

### Behavior

- Runs **last** in the transient phase, after roles exist. Emits
  `ALTER ROLE … PASSWORD` and `ALTER DATABASE … SET` statements, fed to `psql`
  over **stdin** (never argv/env) so no secret value is exposed via
  `/proc/<pid>/cmdline`.
- The apply session first disables statement logging
  (`SET log_statement = 'none'`, `log_min_error_statement = 'panic'`,
  `log_min_duration_statement = -1`) so the plaintext password in
  `ALTER ROLE … PASSWORD` is not written to the PostgreSQL server log — the
  generated config sets `log_statement = 'ddl'`, which would otherwise log it.
  These `SET`s are session-local to the transient connection.
- **Verification:** after applying role passwords, init queries
  `pg_authid` and **fails** if any login role still has no password. This turns
  a role omitted from the file — or created `LOGIN` by the init SQL but never
  seeded — into a hard init failure instead of a cluster that starts up but
  can't authenticate its services.
  - Limitation: a password set to a _malformed_ pre-hashed verifier cannot be
    detected here (Postgres stores a pre-hashed value verbatim without
    validating it); the check only catches a _missing_ password.
- Because it runs only at first init, it seeds **newly created** clusters. It
  does not repair an already-initialized cluster.

### Verifying manually

```sql
-- As the superuser: every login role should report true.
SELECT rolname, rolpassword IS NOT NULL AS has_password
FROM pg_authid
WHERE rolcanlogin
ORDER BY rolname;
```

## Security notes

- Secret values are never logged: pgctld logs role names and counts only, `psql`
  output is redacted on error, and statement logging is disabled for the apply
  session so the plaintext never reaches the PostgreSQL server log (see
  [Behavior](#behavior)).
- Prefer file-based inputs (`--pg-password-file`, `--pg-init-secrets-file`) over
  env vars so plaintext is not exposed in the process environment.
- Mount secret files read-only (`0400`/`0600`). The init-secrets file may hold
  credentials for multiple roles; give it the same protection as the superuser
  password file.
