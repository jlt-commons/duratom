(ns duratom.readers
  "EDN printers and readers for the values plain EDN loses: sorted maps and sets
  print as #sorted/map and #sorted/set, queues as #queue, and a top-level value
  with metadata as #duratom/iobj [value meta]. The printers are global
  print-method overrides, as in jimpil/duratom, so any pr of these types is
  tagged. A sorted collection reads back with the default comparator."
  (:import (clojure.lang PersistentTreeMap PersistentTreeSet PersistentQueue)))

;; The built-in printers for the abstractions, so a tagged collection prints its
;; body the way the host does (as upstream borrows them).
(def ^:private map-print-method (get (methods print-method) clojure.lang.IPersistentMap))
(def ^:private set-print-method (get (methods print-method) clojure.lang.IPersistentSet))
(def ^:private vector-print-method (get (methods print-method) clojure.lang.IPersistentVector))

(defmethod print-method PersistentTreeMap [m w]
  (.write ^java.io.Writer w "#sorted/map ")
  (map-print-method m w))

(defmethod print-method PersistentTreeSet [s w]
  (.write ^java.io.Writer w "#sorted/set ")
  (set-print-method s w))

(defmethod print-method PersistentQueue [q w]
  (.write ^java.io.Writer w "#queue ")
  ;; jolt's vector printer takes only vectors; upstream's takes any sequential
  (vector-print-method (vec q) w))

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
