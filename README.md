# duratom

A durable atom for [jolt](https://github.com/jolt-lang/jolt): an `@`-derefable
reactive cell that persists every mutation to a pluggable backend. It implements
glimmer's `IReactiveCell` protocol, so a duratom drops into any glimmer program
unchanged — `@`, `reset!`, `swap!`, `cursor` and `reaction` all work while each
change is written through to storage.

## Install

```clojure
;; deps.edn
{:deps {io.github.jlt-commons/duratom
        {:git/url "https://github.com/jlt-commons/duratom.git"
         :git/sha "<full-sha>"}}}
```

## Quick start

```clojure
(require '[duratom.core :as d])

(def counts (d/file-atom "counts.edn" {:clicks 0}))

@counts                     ;; => {:clicks 0}
(swap! counts update :clicks inc)
@counts                     ;; => {:clicks 1}

(d/backend-snapshot counts) ;; => {:clicks 1} — read back from disk

(d/destroy counts)          ;; delete the file and release the backend
```

Every `reset!` / `swap!` persists, so a new atom over the same path reads the
stored state:

```clojure
@(d/file-atom "counts.edn") ;; => {:clicks 1}
```

`init` may be a value, a no-arg fn, or a delay; it is ignored when the backend
already holds a value.

## Commit modes

Commits are synchronous by default: `swap!` returns once the value is stored,
and a failed write throws to the caller with the in-memory value unchanged.

With `:commit-mode :async`, `swap!` updates memory and returns. A writer fiber
persists the value in the background on a small fiber pool of its own (file IO
and sqlite calls pin a carrier, so they stay off the default pool). When swaps
outpace the writer it writes only the latest value and skips the ones in
between. On a 480 KB EDN value a sync swap takes ~27 ms and an async one
~0.2 ms.

```clojure
(def a (d/file-atom "n.edn" 0 {:commit-mode :async}))
(dotimes [_ 1000] (swap! a inc))   ;; returns at once
(d/flush! a)                       ;; block until 1000 is on disk
```

`flush!` takes an optional timeout and rethrows a failed write. Pending writes
also land before `destroy`/`release` and before the process exits.

Other options, in the last argument of `file-atom`/`sql-atom`/`make-duratom`
or under `:rw` in `duratom`:

- `:error-handler (fn [error recommit] ...)` takes write failures instead of
  throwing; `(recommit)` retries the write. With a handler, a sync write that
  fails still moves the in-memory value.
- `:read-from :storage` makes `@` and `swap!` read the backend instead of
  memory. Needs sync commits.
- `:read`/`:write` replace the EDN serialization. For a file they take a path
  (`(fn [path])`, `(fn [path value])`); for SQL they convert to and from the
  column value, with `:column-type` for the table.

## What the EDN format keeps

The default serialization round-trips values plain EDN would lose. Requiring
`duratom.core` (or `duratom.readers`) installs global `print-method`s, as
jimpil/duratom does:

- sorted maps and sets print as `#sorted/map {...}` / `#sorted/set #{...}`
  and read back sorted (with the default comparator)
- queues print as `#queue [...]`
- metadata on the top-level value is stored as `#duratom/iobj [value meta]`

`duratom.readers/default` holds the matching EDN readers for reading those
files elsewhere. These printers apply to every `pr` in the process.

## The clojure.core reference API

A duratom answers `@`, `reset!`, `swap!`, `swap-vals!`, `reset-vals!`,
`compare-and-set!`, `add-watch`, `remove-watch`, `set-validator!`,
`get-validator`, `with-meta` and `with-open` (closing destroys it). jolt's
built-in versions of these only accept its own reference types, so
`duratom.core` rebinds them the way `glimmer.ratom` rebinds `deref`/`swap!`;
anything that is not a duratom goes straight to the original fn.

After `destroy` (or `release`, which keeps the stored value and only closes the
backend) the value stays readable and every write throws
`IllegalStateException`.

## jimpil/duratom style constructors

```clojure
(d/duratom :local-file :file-path "n.edn" :init 0 :rw {:commit-mode :async})
(d/duratom :sqlite-db :db-config "sqlite:state.db" :table-name "t" :row-id 0 :init {})
(d/duratom :postgres-db :db-config "postgres://..." :table-name "t" :init {})

;; a duragent persists after every action
(def ag (d/duragent :local-file :file-path "ag.edn" :init 0))
(send-off ag inc)
```

`duratom` and `duragent` are multimethods, so a new backend can get its own
keyword: `(defmethod d/duratom ::redis [_ & {:keys [init rw]}] (d/make-duratom (my-backend) init rw))`.

## SQL backend

`sql-atom` persists a single EDN value to an `(id, value)` table through
[jolt-lang/db](https://github.com/jolt-lang/db) — sqlite or postgres:

```clojure
(def counters (d/sql-atom "sqlite:state.db" "counters" {:n 0}))
;; postgres:
(def counters (d/sql-atom "postgres://user:pass@127.0.0.1:5432/app" "counters" {:n 0}))
```

The table is created if missing. Writes are a single upsert. The connection
stays open for the atom's lifetime, so `"sqlite::memory:"` keeps its data
across mutations; `release` or `destroy` closes it. A `dbspec` map
(`{:vendor :sqlite :name ...}`) works too.

## The StorageBackend protocol

A backend is anything implementing `duratom.backends/StorageBackend`. That is the
seam for adding a new durable store — S3, Redis, a remote API, … (jimpil/duratom
ships those; here they are left to the protocol, since each needs a client
library):

```clojure
(require '[duratom.core :as d]
         '[duratom.backends :as b])

(defrecord MyBackend [conn]
  b/StorageBackend
  (load-state [_]     ...)   ; read the persisted value, or nil
  (persist! [_ v]     ...)   ; store v, return v
  (clear! [_]         ...)   ; remove the stored value
  (close-backend [_]  ...))  ; release resources

(def a (d/make-duratom (->MyBackend conn) {:initial true}))
```

`load-state` returning nil means "empty": `make-duratom` starts the atom at its
`init` argument and persists it. A stored `false` is always a value. A backend
that can store nil also implements `b/StoredValue` (`(stored? [_] ...)`, true
when a value is present), so a persisted nil is not mistaken for empty; the file
and SQL backends both do.

## Reactive integration with glimmer

`Duratom` implements `glimmer.ratom/IReactiveCell`, so it composes with the rest
of the reactive model. It also implements `IAtomicSwap`, so `swap!` reads,
persists and updates under one lock: concurrent swaps from several threads do
not lose writes. That needs glimmer v0.1.7 or later.

```clojure
(require '[glimmer.ratom :as r])

(def a  (d/file-atom "n.edn" 2))
(def sq (r/reaction (* @a @a)))   ;; 4

(reset! a 5)
@sq                               ;; 25 — recomputed, and 5 persisted
```

## Development

```bash
jolt -M:test
```

The suite runs headless: jimpil/duratom's backend scenario over the file and
sqlite backends in both commit modes (duratom and duragent), the error-handler
and recommit tests, the reference API, and glimmer reaction composition.

## License

Eclipse Public License, same as Clojure. Forked from
[jimpil/duratom](https://github.com/jimpil/duratom).
