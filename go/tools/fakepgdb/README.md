# fakepgdb

A fake PostgreSQL database server for testing, inspired by [Vitess's fakesqldb](https://github.com/vitessio/vitess/tree/main/go/mysql/fakesqldb).

## Features

- **In-memory fake PostgreSQL**: No actual PostgreSQL server required
- **Query matching**: Match exact queries or use regex patterns
- **Ordered/unordered modes**: Test query sequences or independent queries
- **Query logging**: Track which queries were executed
- **Rejection support**: Test error handling by rejecting specific queries
- **Thread-safe**: All methods can be called concurrently

## Usage

### Basic Example

```go
func TestMyCode(t *testing.T) {
    db := fakepgdb.New(t)

    // Add expected query and result
    db.AddQuery("SELECT 1", &fakepgdb.ExpectedResult{
        Columns: []string{"?column?"},
        Rows:    [][]interface{}{{int64(1)}},
    })

    // Get a *sql.DB to use in your code
    sqlDB := db.OpenDB()
    defer sqlDB.Close()

    // Use it like a real database
    var result int
    err := sqlDB.QueryRow("SELECT 1").Scan(&result)
    // ...
}
```

### Query Patterns

```go
db.AddQueryPattern("SELECT \\* FROM users WHERE id = .*", &fakepgdb.ExpectedResult{
    Columns: []string{"id", "name"},
    Rows:    [][]interface{}{{int64(1), "Alice"}},
})

// Matches: SELECT * FROM users WHERE id = 1
// Matches: SELECT * FROM users WHERE id = 999
```

### Ordered Queries

```go
db.OrderMatters()

db.AddExpectedExecuteFetch(fakepgdb.ExpectedExecuteFetch{
    Query: "SELECT 1",
    QueryResult: &fakepgdb.ExpectedResult{
        Columns: []string{"?column?"},
        Rows:    [][]interface{}{{int64(1)}},
    },
})

db.AddExpectedExecuteFetch(fakepgdb.ExpectedExecuteFetch{
    Query: "SELECT 2",
    QueryResult: &fakepgdb.ExpectedResult{
        Columns: []string{"?column?"},
        Rows:    [][]interface{}{{int64(2)}},
    },
})

// Queries must be executed in this order
sqlDB.QueryRow("SELECT 1").Scan(&result)
sqlDB.QueryRow("SELECT 2").Scan(&result)

// Verify all expected queries were executed
db.VerifyAllExecutedOrFail()
```

### Rejecting Queries

```go
db.AddRejectedQuery("SELECT * FROM forbidden", errors.New("access denied"))

// This query will return an error
_, err := sqlDB.Query("SELECT * FROM forbidden")
// err.Error() == "access denied"
```

### Query Logging

```go
db.QueryRow("SELECT 1").Scan(&result)
db.QueryRow("SELECT 2").Scan(&result)

log := db.QueryLog() // "select 1;select 2"
count := db.GetQueryCalledNum("SELECT 1") // 1
```

## Differences from Vitess fakesqldb

- Uses Go's `database/sql/driver` interface instead of MySQL wire protocol
- No network socket - connects directly via driver interface
- Simpler implementation focused on testing needs
- PostgreSQL-specific (though could work for other SQL databases)

## Implementation Notes

This package provides a minimal implementation of the `database/sql/driver` interfaces:

- `driver.Driver`
- `driver.Conn`
- `driver.Stmt`
- `driver.Tx`
- `driver.Rows`
- `driver.Result`

Query matching is case-insensitive and uses exact string matching or regex patterns.
