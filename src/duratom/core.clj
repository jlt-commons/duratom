(ns duratom.core
  "A durable atom: a glimmer reactive cell whose mutations are persisted through
  a pluggable `duratom.backends/StorageBackend`.

  Because it implements `glimmer.ratom/IReactiveCell`, a duratom works everywhere
  a glimmer atom does — @, reset!, swap!, cursor, reaction — while persisting
  every change to its backend. It also answers the rest of the clojure.core
  reference API: swap-vals!, reset-vals!, compare-and-set!, add-watch,
  remove-watch, set-validator! and get-validator.

  Commits are synchronous by default: swap! returns once the value is stored,
  and a failed write throws to the caller. With `:commit-mode :async`, swap!
  updates memory and returns; a writer fiber persists the latest value in the
  background, skipping intermediate values that a newer one has replaced."
  (:require [glimmer.ratom :as ratom]
            [duratom.backends :as backends]
            [duratom.utils :as ut]
            [jolt.fibers :as fib]))

;; Captured before this namespace rebinds the clojure.core reference fns (at the
;; bottom), so the internals below always reach the host implementations.
(defonce ^:private host-swap! clojure.core/swap!)
(defonce ^:private host-reset! clojure.core/reset!)
(defonce ^:private host-swap-vals! clojure.core/swap-vals!)
(defonce ^:private host-reset-vals! clojure.core/reset-vals!)
(defonce ^:private host-compare-and-set! clojure.core/compare-and-set!)
(defonce ^:private host-add-watch clojure.core/add-watch)
(defonce ^:private host-remove-watch clojure.core/remove-watch)
(defonce ^:private host-set-validator! clojure.core/set-validator!)
(defonce ^:private host-get-validator clojure.core/get-validator)

(defn- released-error []
  (IllegalStateException. "duratom/duragent has been released!"))

(declare destroy)

;; --- async writer -------------------------------------------------------------
;;
;; File IO and sqlite calls pin the carrier they run on, so writers get a fiber
;; pool of their own instead of sharing the default one with application fibers.

(defonce ^:private default-pool (delay (fib/pool {:name :duratom-io :size 2})))

;; Writer fibers still running. Fibers are daemons, so a shutdown hook waits for
;; this to drain; otherwise a process that exits right after a swap! would drop
;; the pending write.
(defonce ^:private inflight (atom 0))

(defonce ^:no-doc drain-on-exit-hook
  (jolt.host/add-shutdown-hook
    (fn []
      (let [deadline (+ (System/currentTimeMillis) 10000)]
        (while (and (pos? @inflight) (< (System/currentTimeMillis) deadline))
          (Thread/sleep 1))))))

(defn- mark-written!
  [cstate seq]
  (host-swap! cstate (fn [s] (if (>= seq (:written s)) (assoc s :written seq :error nil) s))))

(defn- async-recommit
  "Persist the latest queued value now, on the caller's thread."
  [dura]
  (fn recommit []
    (let [{:keys [seq val]} @(:cstate dura)]
      (backends/persist! (:backend dura) val)
      (mark-written! (:cstate dura) seq))))

(defn- run-writer
  [dura]
  (let [{:keys [backend cstate opts]} dura
        handler (:error-handler opts)]
    (try
      (loop []
        (let [{:keys [seq val]} @cstate]
          (try
            (backends/persist! backend val)
            (mark-written! cstate seq)
            (catch Throwable e
              (host-swap! cstate assoc :error e)
              (when handler
                ;; a throwing handler must not kill the writer
                (try (handler e (async-recommit dura)) (catch Throwable _ nil)))))
          ;; idle only if nothing was queued while writing; enqueue! sees
          ;; :running false and starts a fresh writer
          (let [[_ s] (host-swap-vals! cstate
                                       (fn [s] (if (= seq (:seq s)) (assoc s :running false) s)))]
            (when (:running s) (recur)))))
      (finally
        (host-swap! inflight dec)))))

(defn- enqueue!
  "Queue `v` for the writer. Called under the duratom's lock, so values queue in
  the order they were set."
  [dura v]
  (let [[old _] (host-swap-vals! (:cstate dura)
                                 (fn [s] (assoc s :seq (inc (:seq s)) :val v :running true)))]
    (when-not (:running old)
      (host-swap! inflight inc)
      (fib/spawn #(run-writer dura) {:pool (or (:pool (:opts dura)) @default-pool)}))))

;; --- state transitions --------------------------------------------------------

(defn- async? [dura] (= :async (:commit-mode (:opts dura))))
(defn- storage-reads? [dura] (= :storage (:read-from (:opts dura))))

(defn- check-live! [dura]
  (when @(:released dura) (throw (released-error))))

(defn- validate! [validator v]
  (when (and validator (not (validator v)))
    (throw (IllegalStateException. "Invalid reference state"))))

(defn- unchanged?
  "True when writing `new` over `old` would store nothing new. Metadata and the
  collection type count: both survive a round-trip through storage."
  [old new]
  (or (identical? old new)
      (and (= old new) (= (meta old) (meta new)) (= (type old) (type new)))))

(defn- current-value [dura]
  (if (and (storage-reads? dura) (not @(:released dura)))
    (first (backends/stored-state (:backend dura)))
    @(:state dura)))

(defn- commit-sync!
  "Persist `new`, then make it the state. Without an error handler a failed write
  throws and the state does not move. With one, the state moves and the handler
  gets the error and a fn that retries the write."
  [dura new]
  (let [{:keys [backend state opts]} dura]
    (try
      (backends/persist! backend new)
      (host-reset! state new)
      (catch Throwable e
        (if-let [handler (:error-handler opts)]
          (do (host-reset! state new)
              (try (handler e (fn recommit [] (backends/persist! backend @state)))
                   (catch Throwable _ nil)))
          (throw e))))))

(defn- apply-change! [dura new]
  (if (async? dura)
    (do (host-reset! (:state dura) new)
        (enqueue! dura new))
    (commit-sync! dura new)))

(defn- notify! [dura old new]
  (ratom/-notify-watches! dura)
  (doseq [[k f] @(:key-watches dura)]
    (f k dura old new)))

(defn- transition!
  "Set the state to (f old) under the lock, persisting it per the commit mode.
  Watchers run after the lock is released. Returns [old new]."
  [dura f]
  (check-live! dura)
  (let [[old new changed?]
        (locking (:lock dura)
          (check-live! dura)
          (let [old (current-value dura)
                new (f old)]
            (validate! @(:validator dura) new)
            (if (unchanged? old new)
              [old new false]
              (do (apply-change! dura new)
                  [old new true]))))]
    (when changed?
      (notify! dura old new))
    [old new]))

(defn- cas!
  [dura oldv newv]
  (check-live! dura)
  (let [hit? (locking (:lock dura)
               (check-live! dura)
               (let [cur (current-value dura)]
                 ;; an atom compares by identity; a value read back from storage
                 ;; is never identical, so storage reads compare by value
                 (when (if (storage-reads? dura) (= cur oldv) (identical? cur oldv))
                   (validate! @(:validator dura) newv)
                   (apply-change! dura newv)
                   true)))]
    (when hit?
      (notify! dura oldv newv))
    (boolean hit?)))

(defrecord Duratom [backend state watches key-watches validator lock released opts cstate]
  ratom/IReactiveCell
  (-value [this] (current-value this))
  (-reset! [this v] (transition! this (constantly v)) v)
  (-add-watch! [_ w] (host-swap! watches conj w))
  (-remove-watch! [_ w] (host-swap! watches disj w))
  (-notify-watches! [this] (doseq [w @watches] (w this)))
  ratom/IAtomicSwap
  (-swap! [this f] (second (transition! this f)))
  java.io.Closeable
  (close [this] (destroy this)))

(defn duratom?
  "True when `x` is a duratom."
  [x]
  (instance? Duratom x))

(defn- mode [k v allowed]
  (let [m (some-> v name keyword)]
    (if (contains? allowed m)
      m
      (throw (IllegalArgumentException.
               (str k " must be one of " (sort allowed) ", got " (pr-str v)))))))

(defn- normalize-opts [opts]
  (let [commit (if (some? (:commit-mode opts)) (mode :commit-mode (:commit-mode opts) #{:sync :async}) :sync)
        read-from (if (some? (:read-from opts)) (mode :read-from (:read-from opts) #{:memory :storage}) :memory)]
    (when (and (= :storage read-from) (= :async commit))
      (throw (IllegalArgumentException.
               ":read-from :storage needs :commit-mode :sync; an async write may not have landed when the next read goes to storage")))
    (assoc opts :commit-mode commit :read-from read-from)))

(defn make-duratom
  "Build a durable atom over `backend` (any `duratom.backends/StorageBackend`).
  If the backend already holds a value it becomes the initial state; otherwise
  the atom starts at `init` and persists it.

  opts:
    :commit-mode   :sync (default) or :async
    :read-from     :memory (default) or :storage — deref and swap! read the
                   backend; needs :sync
    :error-handler (fn [error recommit]) — called when a write fails; recommit
                   retries the write. Without one, a sync write error throws to
                   the caller and an async one is rethrown by `flush!`.
    :pool          a jolt.fibers pool for async writers (default: a shared
                   two-carrier pool)"
  ([backend init] (make-duratom backend init nil))
  ([backend init opts]
   (let [opts (normalize-opts opts)
         d (map->Duratom {:backend backend
                          :state (atom nil)
                          :watches (atom #{})
                          :key-watches (atom {})
                          :validator (atom nil)
                          :lock (Object.)
                          :released (atom false)
                          :opts opts
                          :cstate (when (= :async (:commit-mode opts))
                                    (atom {:seq 0 :val nil :written 0 :running false :error nil}))})]
     (if-let [[stored] (backends/stored-state backend)]
       (host-reset! (:state d) stored)
       (let [v (ut/->init init)]
         ;; persisted directly: the state starts at nil, so going through the
         ;; transition would skip an init of nil as unchanged
         (backends/persist! backend v)
         (host-reset! (:state d) v)))
     d)))

(defn flush!
  "Block until every write queued on `dura` so far has reached storage. Returns
  true, or false if `timeout-ms` passes first or the last write failed and an
  error handler took the error. Without a handler, rethrows the write's error.
  A sync duratom has nothing pending and returns true at once."
  ([dura] (flush! dura nil))
  ([dura timeout-ms]
   (if-let [cstate (:cstate dura)]
     (let [target (:seq @cstate)
           deadline (some->> timeout-ms (+ (System/currentTimeMillis)))]
       (loop []
         (let [{:keys [written running error]} @cstate]
           (cond
             (>= written target) true
             (and (not running) error) (if (:error-handler (:opts dura)) false (throw error))
             (and deadline (> (System/currentTimeMillis) deadline)) false
             :else (do (Thread/sleep 1) (recur))))))
     true)))

(defn- shut!
  [dura clear?]
  (when (locking (:lock dura)
          (when-not @(:released dura)
            (host-reset! (:released dura) true)
            true))
    ;; let a pending async write land before clearing, or it would recreate
    ;; the storage afterwards
    (try (flush! dura) (catch Throwable _ nil))
    (let [backend (:backend dura)]
      (try
        (when clear? (backends/clear! backend))
        (finally
          (backends/close-backend backend)))))
  nil)

;; --- duragent -----------------------------------------------------------------

(defn make-duragent
  "An agent whose state is persisted to `backend` after every action. Like
  `make-duratom`, a stored value wins over `init`.

  opts:
    :error-handler (fn [agent error]) for failed actions, and
                   (fn [agent error recommit]) for a failed write
    :meta          metadata for the agent"
  ([backend init] (make-duragent backend init nil))
  ([backend init {:keys [error-handler meta]}]
   (let [released (atom false)
         start (if-let [[stored] (backends/stored-state backend)]
                 stored
                 (let [v (ut/->init init)]
                   (backends/persist! backend v)
                   v))
         ag (agent start
                   :error-mode :continue
                   ;; only the agent's error handler sees this, not the sender
                   :validator (fn [_] (when @released (throw (released-error))) true))
         shut (fn [clear?]
                (when (compare-and-set! released false true)
                  (await ag)
                  (try (when clear? (backends/clear! backend))
                       (finally (backends/close-backend backend))))
                nil)]
     (add-watch ag ::commit
                (fn [_ a _ n]
                  (try (backends/persist! backend n)
                       (catch Throwable e
                         (when error-handler
                           (error-handler a e #(backends/persist! backend @a)))))))
     (set-error-handler! ag (fn [a e] (when error-handler (error-handler a e))))
     (reset-meta! ag (merge meta
                            {::destroy #(shut true)
                             ::release #(shut false)
                             ::snapshot #(backends/load-state backend)}))
     ag)))

;; --- public API ---------------------------------------------------------------

(defn backend-snapshot
  "The value currently persisted in `dura`'s (or a duragent's) backend, read fresh."
  [dura]
  (if (duratom? dura)
    (backends/load-state (:backend dura))
    (when-let [snap (::snapshot (meta dura))] (snap))))

(defn destroy
  "Remove `dura`'s persisted value and release its backend resources. Pending
  async writes land first. Afterwards the value stays readable in memory and
  every write throws. Idempotent."
  [dura]
  (if (duratom? dura)
    (shut! dura true)
    (when-let [f (::destroy (meta dura))] (f))))

(defn release
  "Like `destroy`, but keep the persisted value: let pending writes land, refuse
  further writes and close the backend (e.g. its database connection)."
  [dura]
  (if (duratom? dura)
    (shut! dura false)
    (when-let [f (::release (meta dura))] (f))))

(defn file-atom
  "A durable atom persisted as EDN to `path` on the local filesystem. opts are
  `make-duratom`'s plus the file backend's `:read`/`:write`."
  ([path] (file-atom path nil nil))
  ([path init] (file-atom path init nil))
  ([path init opts] (make-duratom (backends/file-backend path opts) init opts)))

(defn sql-atom
  "A durable atom persisted to `table` over a jolt-lang/db `dbspec`
  (e.g. \"sqlite::memory:\" or a postgres uri). opts are `make-duratom`'s plus
  the SQL backend's `:read`/`:write`/`:column-type`."
  ([dbspec table] (sql-atom dbspec table 0 nil nil))
  ([dbspec table init] (sql-atom dbspec table 0 init nil))
  ([dbspec table row-id init] (sql-atom dbspec table row-id init nil))
  ([dbspec table row-id init opts]
   (make-duratom (backends/sql-backend dbspec table row-id opts) init opts)))

(defmulti duratom
  "Keyword constructor, as in jimpil/duratom. Built in: :local-file
  (:file-path), :sqlite-db and :postgres-db (:db-config, :table-name, :row-id).
  All take :init and :rw (`make-duratom`'s opts plus the backend's :read/:write).
  Add a backend with (defmethod duratom ::mine [_ & kvs] (make-duratom ...))."
  (fn [backed-by & _] backed-by))

(defmethod duratom :local-file [_ & {:keys [file-path init rw]}]
  (file-atom file-path init rw))

(defmethod duratom :sqlite-db [_ & {:keys [db-config table-name row-id init rw]}]
  (sql-atom db-config table-name (or row-id 0) init rw))

(defmethod duratom :postgres-db [_ & {:keys [db-config table-name row-id init rw]}]
  (sql-atom db-config table-name (or row-id 0) init rw))

(defmulti duragent
  "duragent is to agent what duratom is to atom. Same keys as `duratom`, plus
  :meta; the error handler comes from (:error-handler rw)."
  (fn [backed-by & _] backed-by))

(defmethod duragent :local-file [_ & {:keys [file-path init rw meta]}]
  (make-duragent (backends/file-backend file-path rw) init
                 {:error-handler (:error-handler rw) :meta meta}))

(defmethod duragent :sqlite-db [_ & {:keys [db-config table-name row-id init rw meta]}]
  (make-duragent (backends/sql-backend db-config table-name (or row-id 0) rw) init
                 {:error-handler (:error-handler rw) :meta meta}))

(defmethod duragent :postgres-db [_ & {:keys [db-config table-name row-id init rw meta]}]
  (make-duragent (backends/sql-backend db-config table-name (or row-id 0) rw) init
                 {:error-handler (:error-handler rw) :meta meta}))

;; --- clojure.core reference fns -------------------------------------------------
;; jolt's swap-vals!, compare-and-set!, add-watch and the validator fns only
;; accept its built-in reference types, even for a type that implements
;; clojure.lang.IAtom / IRef. glimmer.ratom rebinds deref/reset!/swap! for the
;; same reason; these rebind the rest, delegating everything that is not a
;; duratom to the host fn.

(defonce ^:no-doc core-fns-installed
  (do
    (alter-var-root #'clojure.core/swap-vals!
                    (fn [_] (fn [r f & args]
                              (if (duratom? r)
                                (transition! r #(apply f % args))
                                (apply host-swap-vals! r f args)))))
    (alter-var-root #'clojure.core/reset-vals!
                    (fn [_] (fn [r v]
                              (if (duratom? r)
                                (transition! r (constantly v))
                                (host-reset-vals! r v)))))
    (alter-var-root #'clojure.core/compare-and-set!
                    (fn [_] (fn [r oldv newv]
                              (if (duratom? r)
                                (cas! r oldv newv)
                                (host-compare-and-set! r oldv newv)))))
    (alter-var-root #'clojure.core/add-watch
                    (fn [_] (fn [r k f]
                              (if (duratom? r)
                                (do (host-swap! (:key-watches r) assoc k f) r)
                                (host-add-watch r k f)))))
    (alter-var-root #'clojure.core/remove-watch
                    (fn [_] (fn [r k]
                              (if (duratom? r)
                                (do (host-swap! (:key-watches r) dissoc k) r)
                                (host-remove-watch r k)))))
    (alter-var-root #'clojure.core/set-validator!
                    (fn [_] (fn [r f]
                              (if (duratom? r)
                                (do (validate! f (current-value r))
                                    (host-reset! (:validator r) f)
                                    nil)
                                (host-set-validator! r f)))))
    (alter-var-root #'clojure.core/get-validator
                    (fn [_] (fn [r]
                              (if (duratom? r)
                                @(:validator r)
                                (host-get-validator r)))))
    true))
