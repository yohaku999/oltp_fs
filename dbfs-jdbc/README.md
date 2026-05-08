# dbfs-jdbc

`dbfs-jdbc` is a small custom JDBC layer for `dbfs`.

## Current Design

- `DbfsDriver` implements `java.sql.Driver` and exposes the `jdbc:dbfs:` URL scheme.
- JDBC-facing objects such as `Connection`, `Statement`, `PreparedStatement`, and `ResultSet`
	are created as Java dynamic proxies.
- The proxy handlers translate JDBC method calls into a much smaller internal backend interface:
	`DbfsClient.executeQuery(sql, parameters)` and `DbfsClient.executeUpdate(sql, parameters)`.
- `QueryResult` is the internal tabular result shape used to turn backend results back into a JDBC `ResultSet`.

In other words, this module is implemented as a proxy-based JDBC adapter: the public surface is JDBC,
while the backend contract is a small query/update API.

## Current Backend

The current backend is `InMemoryDbfsClient`, which routes calls to `InMemoryTpccDatabase`.
This is a Java in-memory TPCC-specific implementation used for the current benchmark path.

At the moment, `dbfs-jdbc` does not call the C++ `dbfs` engine, does not talk to a standalone
`dbfs` server, and does not model a real client-server process boundary.

## PostgreSQL Tracing

This module also contains `TracingPostgresDriver`, a thin wrapper around the PostgreSQL JDBC driver.
It adds SQL timing and tracing while delegating actual database access to `org.postgresql.Driver`.

## Intended Evolution

The current structure is designed so that `InMemoryDbfsClient` can later be replaced by another
`DbfsClient` implementation, such as a remote client for a standalone `dbfs` server.
