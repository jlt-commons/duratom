(ns duratom.error-handling-test
  "Persist failures and the error-handler/recommit contract, ported from
  jimpil/duratom's error_handling_test. Failures are injected through a backend
  rather than by redefining the file IO."
  (:require [clojure.test :refer [deftest is testing]]
            [duratom.core :as d]
            [duratom.backends :as b]))

(defrecord FlakyBackend [store calls fail?]
  b/StorageBackend
  (load-state [_] @store)
  (persist! [_ v]
    (let [n (swap! calls inc)]
      (when (fail? n)
        (throw (IllegalStateException. (str "persist failed on call " n))))
      (reset! store v)))
  (clear! [_] (reset! store nil) nil)
  (close-backend [_] nil))

(defn- flaky
  "A backend holding `init`, whose nth persist! throws when (fail? n). The
  init is stored up front, so the first persist! is the first mutation's."
  [init fail?]
  (->FlakyBackend (atom init) (atom 0) fail?))

(defn- persist-errors* [mode]
  (let [errors (atom [])
        backend (flaky {:x 1 :y 2} (constantly true))
        dura (d/make-duratom backend nil
                             {:commit-mode mode
                              :error-handler (fn [e _] (swap! errors conj e))})]
    (swap! dura update :x inc)
    (d/flush! dura)
    (swap! dura update :y inc)
    (d/flush! dura)
    (is (= {:x 2 :y 3} @dura) "the in-memory value moves on when a handler is set")
    (is (= 2 (count @errors)))
    (is (every? #(instance? IllegalStateException %) @errors))
    (is (= {:x 1 :y 2} @(:store backend)) "nothing reached storage")))

(deftest persist-errors
  (testing "asynchronous error handler" (persist-errors* :async))
  (testing "synchronous error handler" (persist-errors* :sync)))

(defn- recommit* [mode]
  (let [errors (atom 0)
        ;; calls 1 and 3 fail; the handler's recommit is call 2 and 4
        backend (flaky {:x 1 :y 2} #{1 3})
        dura (d/make-duratom backend nil
                             {:commit-mode mode
                              :error-handler (fn [_ recommit]
                                               (swap! errors inc)
                                               (recommit))})]
    (swap! dura update :x inc)
    (d/flush! dura)
    (swap! dura update :y inc)
    (d/flush! dura)
    (is (= {:x 2 :y 3} @dura))
    (is (= {:x 2 :y 3} @(:store backend)) "the recommit landed")
    (is (= 2 @errors))))

(deftest recommit-tests
  (testing "asynchronous recommits" (recommit* :async))
  (testing "synchronous recommits" (recommit* :sync)))

(deftest sync-failure-without-handler
  (let [backend (flaky {:n 0} (constantly true))
        dura (d/make-duratom backend nil {:commit-mode :sync})]
    (is (thrown? IllegalStateException (swap! dura update :n inc))
        "the caller sees the persist error")
    (is (= {:n 0} @dura) "and the in-memory value does not move")))

(deftest async-failure-without-handler
  (let [backend (flaky {:n 0} #{1})
        dura (d/make-duratom backend nil {:commit-mode :async})]
    (swap! dura update :n inc)
    (is (thrown? IllegalStateException (d/flush! dura))
        "flush! surfaces the failed write")
    (is (= {:n 1} @dura))
    (swap! dura update :n inc)
    (is (true? (d/flush! dura)) "a later successful write clears the error")
    (is (= {:n 2} @(:store backend)))))
