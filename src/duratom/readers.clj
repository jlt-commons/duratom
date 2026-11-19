(ns duratom.readers
  "EDN printers and readers for the values plain EDN loses: sorted maps and sets
  print as #sorted/map and #sorted/set, queues as #queue, and a top-level value
  with metadata as #duratom/iobj [value meta]. The printers are global
  print-method overrides, as in jimpil/duratom, so any pr of these types is
  tagged. A sorted collection reads back with the default comparator."
  (:import (clojure.lang PersistentTreeMap PersistentTreeSet PersistentQueue)))

(defn- print-elements [^java.io.Writer w open close print-one xs]
  (.write w open)
  (loop [xs (seq xs) first? true]
    (when xs
      (when-not first? (.write w " "))
      (print-one (first xs))
      (recur (next xs) false)))
  (.write w close))

(defmethod print-method PersistentTreeMap [m ^java.io.Writer w]
  (.write w "#sorted/map ")
  (print-elements w "{" "}"
                  (fn [[k v]] (print-method k w) (.write w " ") (print-method v w))
                  m))

(defmethod print-method PersistentTreeSet [s ^java.io.Writer w]
  (.write w "#sorted/set ")
  (print-elements w "#{" "}" #(print-method % w) s))

(defmethod print-method PersistentQueue [q ^java.io.Writer w]
  (.write w "#queue ")
  (print-elements w "[" "]" #(print-method % w) q))

(def sorted-readers
  "EDN readers for #sorted/map and #sorted/set."
  {'sorted/map #(into (sorted-map) %)
   'sorted/set #(into (sorted-set) %)})

(def queue-reader
  "EDN reader for #queue."
  {'queue #(into PersistentQueue/EMPTY %)})

(def meta-reader
  "EDN reader for #duratom/iobj [value meta]."
  {'duratom/iobj (fn [[v m]] (with-meta v m))})

(def default
  "The EDN read options duratom uses: every reader above."
  {:readers (merge sorted-readers queue-reader meta-reader)})
