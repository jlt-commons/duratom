(ns duratom.test-runner
  (:require [clojure.test :as t]
            [duratom.core-test]
            [duratom.parity-test]
            [duratom.error-handling-test]
            [duratom.api-test]))

(defn -main [& _]
  (try
    (let [result (t/run-tests 'duratom.core-test
                              'duratom.parity-test
                              'duratom.error-handling-test
                              'duratom.api-test)]
      (when (pos? (+ (:fail result) (:error result)))
        (throw (ex-info "duratom tests failed" result)))
      (println "all passed"))
    (finally
      ;; futures and duragent send-offs keep their pool alive for 60 s otherwise
      (shutdown-agents))))
