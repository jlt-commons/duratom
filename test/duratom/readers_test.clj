(ns duratom.readers-test
  "EDN round trips for the values plain EDN loses: sorted collections, queues
  and top-level metadata."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [duratom.readers :refer [sorted-readers queue-reader meta-reader]]
            [duratom.utils :as ut]))

(defn- round-trip [data readers]
  (let [back (edn/read-string {:readers readers} (ut/pr-str-fully data))]
    (is (= data back))
    (is (= (type data) (type back)))
    back))

(deftest printers-readers-round-trip
  (testing "sorted maps"
    (let [back (round-trip (into (sorted-map) (zipmap (range 20) (range 20))) sorted-readers)]
      (is (= (range 20) (keys back)))))
  (testing "sorted sets"
    (round-trip (into (sorted-set) (range 20)) sorted-readers))
  (testing "queues"
    (let [back (round-trip (into clojure.lang.PersistentQueue/EMPTY [:a :b :c :d]) queue-reader)]
      (is (= :a (peek back)))))
  (testing "nested sorted collections"
    (round-trip {:m (sorted-map :b 2 :a 1) :s [(sorted-set 3 1 2)]} sorted-readers))
  (testing "top-level metadata via #duratom/iobj"
    (let [v (with-meta [1 2 3] {:whatever "foo"})
          back (round-trip v meta-reader)]
      (is (= {:whatever "foo"} (meta back)))))
  (testing "values without metadata print as plain EDN"
    (is (= "[1 2]" (ut/pr-str-fully [1 2])))
    (is (= "{:a 1}" (ut/pr-str-fully {:a 1})))))

(deftest default-readers-cover-everything
  (let [v (with-meta (sorted-map :q (into clojure.lang.PersistentQueue/EMPTY [1 2])
                                 :s (sorted-set 2 1))
            {:k 1})
        back (ut/read-edn-string (ut/pr-str-fully v))]
    (is (= v back))
    (is (sorted? back))
    (is (= {:k 1} (meta back)))
    (is (= [1 2] (seq (:q back))))))

(deftest file-round-trip
  (let [v (with-meta (sorted-map 2 :b 1 :a) {:k 1})
        path (str "/tmp/readers-" (System/currentTimeMillis) "-" (rand-int 1000000) ".edn")]
    (ut/write-edn-file path v)
    (let [back (ut/read-edn-file path)]
      (is (= v back))
      (is (sorted? back))
      (is (= {:k 1} (meta back))))
    (jolt.fs/delete path)))
