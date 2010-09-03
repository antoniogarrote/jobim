(ns jobim.test.core
  (:use [jobim.core] :reload)
  (:use [clojure.test]))

(deftest java-encoding-decoding-test
  (let [orig (java.util.Date.)]
    (is (= (str orig) (str (java-decode (java-encode orig)))))))

(deftest zk-paths-tests
  (is (= (str "/jobim/processes/" 1) (zk-process-path 1)))
  (is (= (str "/jobim/links/" 1) (zk-link-tx-path 1))))
