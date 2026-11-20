(ns duratom.parity-test
  "The scenario jimpil/duratom's core_test runs against every backend: a
  duratom or duragent over the file and sqlite backends, in sync and async
  commit modes, starting both from empty storage and from stored contents."
  (:require [clojure.test :refer [deftest is testing]]
            [duratom.core :as d]
            [duratom.backends :as b]
            [jolt.fs :as fs]))

(defn- uniq [label]
  (str "/tmp/" label "-" (System/currentTimeMillis) "-" (rand-int 1000000)))

(defn- settle!
  "Wait until `dura`'s pending commits have reached storage."
  [dura]
  (if (d/duratom? dura)
    (d/flush! dura)
    (await dura)))

(defn- common*
  [dura exists?]
  (let [atom? (d/duratom? dura)
        f (if atom? swap! send-off)
        transitions (atom [])]
    (when atom?
      (add-watch dura :log (fn [k _r o n] (swap! transitions conj [k o n]))))

    (-> dura (doto (f assoc :z 3)) (doto (f dissoc :x)))
    (settle! dura)
    (is (= {:z 3 :y 2} @dura))
    (is (= (d/backend-snapshot dura) @dura))

    (-> dura (doto (f (constantly [1 2 3]))) (doto (f (comp vec rest))))
    (settle! dura)
    (is (= [2 3] @dura))
    (is (= (d/backend-snapshot dura) @dura))

    (if atom?
      (is (= [[2 3] [1 2 3]] (reset-vals! dura [1 2 3])))
      (f dura (constantly [1 2 3])))
    (settle! dura)

    (if atom?
      (is (= [[1 2 3] [2 3]] (swap-vals! dura (comp vec rest))))
      (f dura (comp vec rest)))
    (settle! dura)
    (is (= [2 3] (d/backend-snapshot dura) @dura))

    (when atom?
      (is (= [:log {:x 1 :y 2} {:x 1 :y 2 :z 3}] (first @transitions)))
      (is (= [:log [1 2 3] [2 3]] (last @transitions))))

    (f dura (partial into (sorted-set)))
    (settle! dura)
    (is (sorted? @dura))
    (is (= (sorted-set 2 3) (d/backend-snapshot dura) @dura))
    (is (sorted? (d/backend-snapshot dura)) "a sorted set reads back sorted")

    (f dura #(with-meta % {:a 1 :b 2}))
    (settle! dura)
    (is (= (sorted-set 2 3) (d/backend-snapshot dura) @dura))
    (is (= {:a 1 :b 2} (meta (d/backend-snapshot dura)) (meta @dura))
        "top-level metadata is persisted")

    (d/destroy dura)
    (settle! dura)

    (testing "the value stays readable in memory after destroy"
      (is (sorted? @dura))
      (is (= #{2 3} @dura)))
    (if atom?
      (is (thrown? IllegalStateException (f dura conj 4)))
      (do (f dura conj 4) (await dura)))
    (is (= #{2 3} @dura) "a released duratom/duragent takes no more writes")
    (is (false? (exists?)) "storage was not cleaned up")))

(def init {:x 1 :y 2})

(defn- file-tests* [mode]
  (let [path (str (uniq "parity") ".edn")
        exists? #(fs/exists? path)
        mk #(d/duratom :local-file :file-path path :init init
                       :rw {:commit-mode mode})]
    (testing "empty storage"
      (common* (mk) exists?))
    (testing "storage already holds a value"
      (spit path (pr-str init))
      (common* (d/duratom :local-file :file-path path :init :ignored
                          :rw {:commit-mode mode})
               exists?))
    (when (= :async mode)
      (testing "duragent"
        (let [p (promise)]
          (common* (d/duragent :local-file :file-path path :init init
                               :rw {:error-handler (fn [_ e] (deliver p e))})
                   exists?)
          (is (= "duratom/duragent has been released!"
                 (ex-message (deref p 1000 nil)))))))))

(deftest file-backed-tests
  (testing "async commits" (file-tests* :async))
  (testing "sync commits" (file-tests* :sync)))

(defn- sqlite-tests* [mode]
  (let [db-path (str (uniq "parity") ".db")
        spec (str "sqlite:" db-path)
        table "atom_state"
        stored? (fn []
                  (let [backend (b/sql-backend spec table 0)]
                    (try (b/stored? backend)
                         (finally (b/close-backend backend)))))
        mk (fn [init]
             (d/duratom :sqlite-db :db-config spec :table-name table :row-id 0
                        :init init :rw {:commit-mode mode}))]
    (try
      (testing "empty storage"
        (common* (mk init) stored?))
      (testing "storage already holds a value"
        (let [seed (mk init)]
          (d/flush! seed)
          (d/release seed))
        (common* (mk :ignored) stored?))
      (when (= :async mode)
        (testing "duragent"
          (common* (d/duragent :sqlite-db :db-config spec :table-name table
                               :row-id 0 :init init)
                   stored?)))
      (finally
        (when (fs/exists? db-path) (fs/delete db-path))))))

(deftest sqlite-backed-tests
  (testing "async commits" (sqlite-tests* :async))
  (testing "sync commits" (sqlite-tests* :sync)))
