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

(deftest test-should-respond-to
  (do (defprotocol TestP (a [this] "a") (b [this] "b"))
      (deftype TestPImplA [] jobim.test.utils.TestP (a [this] "a"))
      (let [obj (jobim.test.utils.TestPImplA.)]
        (is (respond-to-java-method? obj "a"))
        (is (= false (respond-to-java-method? obj "b"))))))
