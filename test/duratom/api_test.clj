(ns duratom.api-test
  "The clojure.core reference API on a duratom, async commit behaviour,
  storage reads, pluggable serialization and the duratom multimethod seam."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [duratom.core :as d]
            [duratom.backends :as b]
            [jolt.fs :as fs]))

(defn- uniq [label]
  (str "/tmp/" label "-" (System/currentTimeMillis) "-" (rand-int 1000000)))

(defrecord MemBackend [store writes delay-ms]
  b/StorageBackend
  (load-state [_] @store)
  (persist! [_ v]
    (when (pos? delay-ms) (Thread/sleep delay-ms))
    (swap! writes conj v)
    (reset! store v))
  (clear! [_] (reset! store nil) nil)
  (close-backend [_] nil))

(defn- mem
  ([] (mem 0))
  ([delay-ms] (->MemBackend (atom nil) (atom []) delay-ms)))

(deftest compare-and-set
  (let [backend (mem)
        a (d/make-duratom backend {:n 0})
        v @a]
    (is (false? (compare-and-set! a {:n 0} {:n 1}))
        "an equal but not identical old value does not match, as with an atom")
    (is (true? (compare-and-set! a v {:n 1})))
    (is (= {:n 1} @a @(:store backend)))))

(deftest validators
  (let [backend (mem)
        a (d/make-duratom backend 1)]
    (set-validator! a pos?)
    (is (= pos? (get-validator a)))
    (is (thrown? IllegalStateException (reset! a -1)))
    (is (thrown? IllegalStateException (swap! a -))
        "swap! is validated too")
    (is (= 1 @a @(:store backend)) "a rejected value is neither kept nor persisted")
    (is (thrown? IllegalStateException (set-validator! a neg?))
        "installing a validator checks the current value")
    (set-validator! a nil)
    (reset! a -5)
    (is (= -5 @a))))

(deftest key-watches
  (let [a (d/make-duratom (mem) 0)
        seen (atom [])]
    (is (identical? a (add-watch a :w (fn [k r o n] (swap! seen conj [k (identical? r a) o n])))))
    (swap! a inc)
    (reset! a 10)
    (is (= [[:w true 0 1] [:w true 1 10]] @seen))
    (remove-watch a :w)
    (swap! a inc)
    (is (= 2 (count @seen)) "a removed watch no longer fires")))

(deftest no-op-writes-are-skipped
  (let [backend (mem)
        a (d/make-duratom backend {:n 1})]
    (reset! (:writes backend) [])
    (swap! a identity)
    (reset! a {:n 1})
    (is (= [] @(:writes backend)) "an unchanged value is not written again")
    (reset! a (with-meta {:n 1} {:m 1}))
    (is (= 1 (count @(:writes backend))) "a metadata-only change is a change")
    (reset! a (sorted-map :n 1))
    (is (= 2 (count @(:writes backend))) "so is a change of collection type")))

(deftest duratom-metadata-and-close
  (let [path (str (uniq "api-meta") ".edn")
        a (with-meta (d/file-atom path {:n 1}) {:service-id 7})]
    (is (= {:service-id 7} (meta a)))
    (swap! a update :n inc)
    (is (= {:n 2} @a))
    (with-open [_ a] nil)
    (is (not (fs/exists? path)) "closing a duratom destroys its storage")))

(deftest async-commits
  (testing "swap! returns before the write and later writes coalesce"
    (let [backend (mem 20)
          a (d/make-duratom backend 0 {:commit-mode :async})]
      (reset! (:writes backend) [])
      (let [t0 (System/nanoTime)]
        (dotimes [_ 50] (swap! a inc))
        (is (< (/ (- (System/nanoTime) t0) 1e6) 200)
            "50 swaps did not wait on 50 x 20 ms of writes"))
      (is (= 50 @a))
      (is (true? (d/flush! a)))
      (is (= 50 @(:store backend)))
      (is (< (count @(:writes backend)) 50) "intermediate values were skipped")
      (is (= 50 (last @(:writes backend))))))
  (testing "concurrent async swaps lose nothing"
    (let [backend (mem)
          a (d/make-duratom backend 0 {:commit-mode :async})
          workers (doall (for [_ (range 8)] (future (dotimes [_ 200] (swap! a inc)))))]
      (run! deref workers)
      (d/flush! a)
      (is (= 1600 @a @(:store backend)))))
  (testing "destroy waits for the pending write, then clears"
    (let [backend (mem 30)
          a (d/make-duratom backend 0 {:commit-mode :async})]
      (swap! a inc)
      (d/destroy a)
      (is (nil? @(:store backend)) "the late write did not resurrect the value")))
  (testing "flush! on a sync duratom is a no-op"
    (is (true? (d/flush! (d/make-duratom (mem) 0))))))

(deftest storage-reads
  (let [backend (mem)
        a (d/make-duratom backend {:n 1} {:read-from :storage})]
    (reset! (:store backend) {:n 41})
    (is (= {:n 41} @a) "deref reads storage")
    (is (= {:n 42} (swap! a update :n inc)) "swap! starts from storage")
    (is (= {:n 42} @(:store backend))))
  (is (thrown? IllegalArgumentException
               (d/make-duratom (mem) 0 {:read-from :storage :commit-mode :async}))
      "storage reads need sync commits, or a read could miss a pending write"))

(deftest released-duratoms
  (let [path (str (uniq "api-release") ".edn")
        a (d/file-atom path {:n 1})]
    (d/destroy a)
    (is (thrown? IllegalStateException (swap! a assoc :z 1)))
    (is (thrown? IllegalStateException (reset! a {})))
    (is (thrown? IllegalStateException (compare-and-set! a @a {})))
    (is (not (fs/exists? path)) "the file was not written again")
    (d/destroy a)
    (is (= {:n 1} @a) "destroy is idempotent"))
  (testing "a released sql-atom refuses writes instead of hitting a closed connection"
    (let [a (d/sql-atom "sqlite::memory:" "t" {:n 1})]
      (d/destroy a)
      (is (thrown? IllegalStateException (swap! a assoc :z 1)))))
  (testing "release keeps the stored value"
    (let [path (str (uniq "api-keep") ".edn")
          a (d/file-atom path {:n 1} {:commit-mode :async})]
      (swap! a update :n inc)
      (d/release a)
      (is (thrown? IllegalStateException (swap! a update :n inc)))
      (is (= {:n 2} @(d/file-atom path)))
      (fs/delete path))))

(deftest custom-rw
  (testing "file: :read takes a path, :write a path and a value"
    (let [path (str (uniq "api-rw") ".txt")
          rw {:read (fn [p] (edn/read-string (str/reverse (slurp p))))
              :write (fn [p v] (spit p (str/reverse (pr-str v))))}
          a (d/duratom :local-file :file-path path :init {:x 1} :rw rw)]
      (swap! a assoc :y 2)
      (is (= "}2 y: ,1 x:{" (slurp path)))
      (is (= {:x 1 :y 2} @(d/duratom :local-file :file-path path :rw rw)))
      (d/destroy a)))
  (testing "sql: :write returns the stored string, :read takes it back"
    (let [db (str (uniq "api-rw") ".db")
          spec (str "sqlite:" db)
          rw {:read (fn [s] (edn/read-string (str/reverse s)))
              :write (fn [v] (str/reverse (pr-str v)))}
          a (d/duratom :sqlite-db :db-config spec :table-name "rw" :init {:x 1} :rw rw)]
      (swap! a assoc :y 2)
      (let [a2 (d/duratom :sqlite-db :db-config spec :table-name "rw" :rw rw)]
        (is (= {:x 1 :y 2} @a2))
        (d/release a2))
      (d/destroy a)
      (fs/delete db))))

(defmethod d/duratom ::mem [_ & {:keys [backend init rw]}]
  (d/make-duratom backend init rw))

(deftest duratom-multimethod-is-open
  (let [backend (mem)
        a (d/duratom ::mem :backend backend :init {:n 1} :rw {:commit-mode :async})]
    (swap! a update :n inc)
    (d/flush! a)
    (is (= {:n 2} @(:store backend)))))
