(ns jobim.test.utils
  (:use [jobim.utils] :reload)
  (:use [clojure.test]))

;;(deftest test-timeout
;;  (println "*** test-timeout-1")
;;  (letfn [(sleep ([t] (Thread/sleep t) :sleep-success))
;;          (sleep-ex ([t] (Thread/sleep t) (throw (Exception. "ERROR!"))))]
;;    (is (= :sleep-success (apply-with-timeout sleep [2000] 5000)))
;;    (is (= :timeout (apply-with-timeout sleep [5000] 2000)))
;;    (is (= :exception (try (apply-with-timeout sleep-ex [2000] 5000)
;;                           (catch Exception ex :exception))))))

(deftest test-should-respond-to
  (do (is (= true (respond-to-java-method? (String. "test") "toUpperCase"))
          (= false (respond-to-java-method? (String. "test") "wadus")))))


(deftest test-timeout
  (println "*** test-timeout")
  (let [t (make-timer)
        c (atom 0)]
    (with-timer t 1000 #(swap! c (fn [cp] (inc cp))))
    (Thread/sleep 3000)
    (cancel-timer t)
    (let [computed @c]
      (is (> computed 0))
      (Thread/sleep 2000)
      (is (= computed @c)))))
