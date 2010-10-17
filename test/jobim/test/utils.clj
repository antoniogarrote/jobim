(ns jobim.test.utils
  (:use [jobim.utils] :reload)
  (:use [clojure.test]))

(deftest test-timeout
  (letfn [(sleep ([t] (Thread/sleep t) :sleep-success))
          (sleep-ex ([t] (Thread/sleep t) (throw (Exception. "ERROR!"))))]
    (is (= :sleep-success (apply-with-timeout sleep [2000] 5000)))
    (is (= :timeout (apply-with-timeout sleep [5000] 2000)))
    (is (= :exception (try (apply-with-timeout sleep-ex [2000] 5000)
                           (catch Exception ex :exception))))))
