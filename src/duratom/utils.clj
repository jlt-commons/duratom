(ns duratom.utils
  "EDN serialization and file IO shared by the duratom backends."
  (:require [clojure.edn :as edn]
            [jolt.fs :as fs]))

(def edn-opts
  "EDN read options: return nil at end of input, so an empty file reads as nil."
  {:eof nil})

(defn pr-str-fully
  "`pr-str` with printing limits and metadata printing off, so large
  collections serialize whole and read back as EDN."
  [& xs]
  (binding [*print-length* nil
            *print-level*  nil
            *print-meta*   nil]
    (apply pr-str xs)))

(defn read-edn-string
  "Read one EDN value from string `s`; nil on empty input."
  [s]
  (edn/read-string edn-opts s))

(defn write-edn-file
  "Serialize `data` as EDN to `path`. Returns `path`."
  [path data]
  (spit path (pr-str-fully data))
  path)

(defn file-present?
  "True when `path` exists and is non-empty, so it holds a value (nil included)."
  [path]
  (and (fs/exists? path) (pos? (fs/size path))))

(defn read-edn-file
  "Read one EDN value from `path`."
  [path]
  (read-edn-string (slurp path)))

(defn ->init
  "Resolve an initial-value spec: call a fn, force a delay, else return as-is."
  [x]
  (if (fn? x) (x) (force x)))
