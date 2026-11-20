# Change Log

## [Unreleased]
### Added
- `:commit-mode :async`: writes run on a fiber pool off the caller's thread and
  coalesce to the latest value. `flush!` waits for them.
- `:error-handler` with recommit, `:read-from :storage`, and pluggable
  `:read`/`:write` serialization for the file and SQL backends.
- `swap-vals!`, `reset-vals!`, `compare-and-set!`, `add-watch`, `remove-watch`,
  `set-validator!`/`get-validator` and `with-open` work on a duratom: it
  implements `IAtom`, `IAtom2` and `IRef`. Needs jolt 0.8.28.
- `duratom` and `duragent` multimethod constructors (`:local-file`,
  `:sqlite-db`, `:postgres-db`), `make-duragent`, and `release`.
- Sorted maps and sets, queues and top-level metadata survive a write and read
  (`#sorted/map`, `#sorted/set`, `#queue`, `#duratom/iobj`). The readers are in
  `duratom.readers`.
### Fixed
- A duratom refuses writes after `destroy`. A file-atom used to write its file
  again, and a sql-atom failed on its closed connection.
- A metadata-only or collection-type-only change is persisted instead of being
  skipped as equal.
### Changed
- The SQL backend writes with one upsert statement.

## [0.1.0] - 2026-11-09
### Fixed
- `swap!` is atomic: Duratom implements glimmer's `IAtomicSwap` (glimmer
  bumped to v0.1.7), so concurrent swaps no longer lose writes.
- A stored `false` or `nil` is read back as the value instead of being replaced
  by `init`.
### Changed
- Rewritten for jolt. A duratom is now a durable atom over a pluggable
  `duratom.backends/StorageBackend` protocol, with file and sqlite/postgres
  (via jolt-lang/db) backends. It implements `glimmer.ratom/IReactiveCell`, so
  it composes with glimmer's `@`/`reset!`/`swap!`/`cursor`/`reaction`.
