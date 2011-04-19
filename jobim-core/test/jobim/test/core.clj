(ns jobim.test.core
  (:use [jobim.core] :reload)
  (:use [clojure.test]))

(deftest zk-paths-tests
  (is (= (str "/jobim/links/" 1) (zk-link-tx-path 1))))
