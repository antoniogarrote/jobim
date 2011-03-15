(ns jobim.test.services.coordination.zookeeper
  (:use [jobim.definitions] :reload)
  (:use [jobim.services.coordination.zookeeper] :reload)
  (:use [clojure.test]))

(println "*********************************************************************************************")
(println "                     testing ZooKeeper communication layer                                   " )
(println "*********************************************************************************************")
(println "***********      To run these tests an instance of Zookeeper                        *********")
(println "***********      must be running at localhost:2181                                  *********")
(println "*********************************************************************************************")

(deftest test-data-access
  (println "*** test-data-access")
  (let [coord (make-coordination-service :zookeeper ["localhost:2181" {:timeout 3000}])
        _ (connect-coordination coord)]
    (create coord "/jobim/test" "init")
    (is (= "init" (String. (get-data coord "/jobim/test"))))
    (is (true? (exists? coord "/jobim/test")))
    (set-data coord "/jobim/test" "data")
    (is (= "data" (String. (get-data coord "/jobim/test"))))
    (delete coord "/jobim/test")
    (is (false? (exists? coord "/jobim/test")))))



(deftest test-groups
  (println "*** test-groups")
  (let [coord (make-coordination-service :zookeeper ["localhost:2181" {:timeout 3000}])
        _ (connect-coordination coord)]
    (when (not (exists? coord "/jobim"))
      (create-persistent coord "/jobim"))
    (when (not (exists? coord "/jobim/tests"))
      (create-persistent  coord "/jobim/tests"))
    (create coord "/jobim/tests/t1" "init1")
    (join-group coord "/jobim/tests" "t2" "data2")
    (let [children (get-children coord "/jobim/tests")]
      (is (= 2 (count children)))
      (is (= "init1" (String. (get-data coord (str "/jobim/tests" "/" (first (sort children)))))))
      (is (= "data2" (String. (get-data coord (str "/jobim/tests" "/" (second (sort children))))))))))

;; (deftest test-2pc-success
;;   (println "*** test-2pc-success")
;;   (let [coord (make-coordination-service :zookeeper ["localhost:2181" {:timeout 3000}])
;;         _ (connect-coordination coord)]
;;     (make-2-phase-commit coord "tx" ["a" "b"])
;;     (let [val (atom 0)
;;           post (promise)]
;;       (future (let [res (commit coord "tx" "a")]
;;                 (when (= res "commit")
;;                   (swap! val inc))
;;                 (deliver post :done)))
;;       (is (= 0 @val))
;;       (commit coord "tx" "b")
;;       @post
;;       (is (= 1 @val)))))


;; (deftest test-2pc-rollback
;;   (println "*** test-2pc-rollback")
;;   (let [coord (make-coordination-service :zookeeper ["localhost:2181" {:timeout 3000}])
;;         _ (connect-coordination coord)]
;;     (make-2-phase-commit coord "tx" ["a" "b"])
;;     (let [val (atom 0)
;;           post (promise)]
;;       (future (let [res (commit coord "tx" "a")]
;;                 (when (= res "commit")
;;                   (swap! val inc))
;;                 (deliver post :done)))
;;       (is (= 0 @val))
;;       (rollback coord "tx" "b")
;;       @post
;;       (is (= 0 @val)))))
