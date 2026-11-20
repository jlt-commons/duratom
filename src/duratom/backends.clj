(ns duratom.backends
  "Durable-storage backends.

  A backend is any value implementing `StorageBackend`. The file backend
  serializes to a local file; the SQL backend persists to sqlite or postgres
  through the jolt-lang/db library (the published clojure.jdbc on a java.sql
  shim). Add a new backend by implementing the protocol — duratom.core builds on
  any backend, not a closed set."
  (:require [duratom.utils :as ut]
            [jolt.fs :as fs]
            [db.jdbc]        ;; registers the java.sql shim over db.sqlite/db.pg
            [jdbc.core :as jdbc]))

(defprotocol StorageBackend
  (load-state [backend]     "Read and return the persisted value, or nil when none exists.")
  (persist! [backend v]     "Persist `v`, replacing whatever was stored. Returns `v`.")
  (clear! [backend]         "Remove the persisted value. Returns nil.")
  (close-backend [backend]  "Release backend-held resources. Returns nil."))

(defprotocol StoredValue
  "Optional. A backend that can persist nil implements this so a stored nil is
  told apart from no value at all; load-state returns nil for both. Without it,
  duratom.core treats a nil from load-state as nothing stored (false is always a
  value)."
  (stored? [backend] "True when the backend holds a persisted value, nil included."))

(defn stored-state
  "[v] when `backend` holds a value v (nil or false included), else nil."
  [backend]
  (let [v (load-state backend)]
    (when (if (satisfies? StoredValue backend)
            (or (some? v) (stored? backend))
            (some? v))
      [v])))

;; --- file backend -------------------------------------------------------------

(defrecord FileBackend [path read-fn write-fn]
  StorageBackend
  (load-state [_]
    (when (ut/file-present? path)
      (try (read-fn path)
           (catch Exception e
             (throw (ex-info (str "Unable to read data from file " path "!")
                             {:file-path path} e))))))
  (persist! [_ v]
    ;; write a temp file and rename it over the target, so a crash mid-write
    ;; never leaves a torn file behind
    (let [tmp (str path ".tmp")]
      (write-fn tmp v)
      (fs/move tmp path {:replace-existing true})
      v))
  (clear! [_] (when (fs/exists? path) (fs/delete path)) nil)
  (close-backend [_] nil)
  StoredValue
  (stored? [_] (ut/file-present? path)))

(defn file-backend
  "A backend that persists a single value to `path` on the local filesystem.
  `rw` may supply `:read` (fn [path] -> value) and `:write` (fn [path value]);
  the default is EDN."
  ([path] (file-backend path nil))
  ([path {:keys [read write]}]
   (->FileBackend path (or read ut/read-edn-file) (or write ut/write-edn-file))))

;; --- SQL backend --------------------------------------------------------------

(defrecord SQLBackend [table row-id conn read-fn write-fn]
  StorageBackend
  (load-state [_]
    (when-let [row (jdbc/fetch-one conn [(str "select value from " table " where id = ?") row-id])]
      (read-fn (:value row))))
  (persist! [_ v]
    ;; one upsert statement: atomic, and no update-then-insert race with another
    ;; process writing the same row
    (jdbc/execute! conn [(str "insert into " table " (id, value) values (?, ?)"
                              " on conflict (id) do update set value = excluded.value")
                         row-id (write-fn v)])
    v)
  (clear! [_]
    (jdbc/execute! conn [(str "delete from " table " where id = ?") row-id])
    nil)
  (close-backend [_]
    (.close conn)
    nil)
  StoredValue
  (stored? [_]
    (some? (jdbc/fetch-one conn [(str "select id from " table " where id = ?") row-id]))))

(defn sql-backend
  "A backend that persists a single value to `table` (columns id, value) over a
  jolt-lang/db connection spec: a \"sqlite:path\" / \"sqlite::memory:\" string, a
  `{:vendor :sqlite :name ...}` map, or a postgres uri. Creates the table if
  missing. The connection is held open for the backend's lifetime, so an
  in-memory sqlite spec keeps its data across mutations.

  `rw` may supply `:write` (fn [value] -> column value), `:read` (its inverse)
  and `:column-type` (default \"text\"); the default is EDN text."
  ([dbspec table] (sql-backend dbspec table 0 nil))
  ([dbspec table row-id] (sql-backend dbspec table row-id nil))
  ([dbspec table row-id {:keys [read write column-type]}]
   (let [conn (jdbc/connection dbspec)]
     (jdbc/execute! conn (str "create table if not exists " table
                              " (id integer primary key, value "
                              (name (or column-type "text")) " not null)"))
     (->SQLBackend table row-id conn
                   (or read ut/read-edn-string)
                   (or write ut/pr-str-fully)))))
