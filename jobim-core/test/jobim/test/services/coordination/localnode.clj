(ns jobim.test.services.coordination.localnode
  (:use [jobim.definitions] :reload)
  (:use [jobim.services.coordination.localnode] :reload)
  (:use [clojure.test]))

(deftest test-data-access
  (println "*** test-data-access")
  (let [coord (make-coordination-service :local {})]
    (create coord "/jobim/test" "init")
    (is (= "init" (get-data coord "/jobim/test")))
    (is (true? (exists? coord "/jobim/test")))
    (set-data coord "/jobim/test" "data")
    (is (= "data" (get-data coord "/jobim/test")))
    (delete coord "/jobim/test")
    (is (false? (exists? coord "/jobim/test")))))

(deftest test-connect
  (println "*** test-connect")
  (let [coord (make-coordination-service :local {})]
    (is (= :ignore (connect-coordination coord)))))


(deftest test-groups
  (println "*** test-groups")
  (let [coord (make-coordination-service :local {})]
    (create coord "/jobim/tests/t1" "init1")
    (create coord "/jobim/tests/t2" "init2")
    (join-group coord "/jobim/tests" "t1" "data1")
    (let [children (get-children coord "/jobim/tests")]
      (is (= 2 (count children)))
      (is (= "data1" (get-data coord (str "/jobim/tests" "/" (first (sort children))))))
      (is (= "init2" (get-data coord (str "/jobim/tests" "/" (second (sort children)))))))))

(deftest test-2pc-success
  (println "*** test-2pc-success")
  (let [coord (make-coordination-service :local {})]
    (make-2-phase-commit coord "tx" ["a" "b"])
    (let [val (atom 0)
          post (promise)]
      (future (let [res (commit coord "tx" "a")]
                (when (= res "commit")
                  (swap! val inc))
                (deliver post :done)))
      (is (= 0 @val))
      (commit coord "tx" "b")
      @post
      (is (= 1 @val)))))


(deftest test-2pc-rollback
  (println "*** test-2pc-rollback")
  (let [coord (make-coordination-service :local {})]
    (make-2-phase-commit coord "tx" ["a" "b"])
    (let [val (atom 0)
          post (promise)]
      (future (let [res (commit coord "tx" "a")]
                (when (= res "commit")
                  (swap! val inc))
                (deliver post :done)))
      (is (= 0 @val))
      (rollback coord "tx" "b")
      @post
      (is (= 0 @val)))))
