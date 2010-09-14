(ns jobim.test.jobim
  (:use [jobim] :reload)
  (:use [jobim.examples.actors] :reload)
  (:use [jobim.examples.fsm] :reload)
  (:use [jobim.behaviours.fsm] :reload)
  (:use [clojure.test]))


(defonce *messaging-to-test* :rabbitmq)

(println "*********************************************************************************************")
(if (= *messaging-to-test* :rabbitmq)
  (println "***********      To run these tests an instance of Zookeeper, RabbitMQ 2.0          *********")
  (println "***********      To run these tests an instance of Zookeeper                        *********"))
(println "***********      and a remote node correctly configured must be running             *********")
(println "*********************************************************************************************")


(if (= *messaging-to-test* :rabbitmq)
  (println "\n\n\n             testing RABBITMQ communication layer                      \n\n\n" )
  (println "\n\n\n             testing ZEROMQ communication layer                      \n\n\n" ))


(defonce *remote-node-name* "remote-test")
(defonce *local-node-name* "local-test")
(if (= *messaging-to-test* :rabbitmq)
  (defonce *test-node-config* {:node-name "local-test"
                               :messaging-type :rabbitmq
                               :messaging-options {:host "localhost"}
                               :zookeeper-options ["localhost:2181" {:timeout 3000}]})
  (defonce *test-node-config* {:node-name "local-test"
                               :messaging-type :zeromq
                               :messaging-options {:protocol-and-port "tcp://192.168.1.35:5554"}
                               :zookeeper-options ["localhost:2181" {:timeout 3000}]}))

(defonce *test-node-name*  "local-test")

(jobim.core/bootstrap-node (:node-name *test-node-config*) (:messaging-type *test-node-config*) (:messaging-options *test-node-config*) (:zookeeper-options *test-node-config*))

(spawn-in-repl)

(deftest test-spawn
  (println "*** test-spawn")
  (let [pid (spawn jobim.examples.actors/ping)]
    (is (not (nil? pid)))))

(deftest test-spawn-from-string
  (println "*** test-spawn-from-string")
  (let [pid (spawn "jobim.examples.actors/ping")]
    (is (not (nil? pid)))))


(deftest test-remote-spawn-plus-register-name-test-send
  (println "*** test-remote-spawn-plus-register-name-test-send")
  (let [pid1 (rpc-blocking-call (resolve-node-name *remote-node-name*) "jobim/spawn" ["jobim.examples.actors/ping"])]
    (rpc-blocking-call (resolve-node-name *remote-node-name*) "jobim/register-name" ["remoteping" pid1])
    (is (= pid1 (get (registered-names) "remoteping")))
    (let [prom (promise)
          prom2 (promise)
          prom3 (promise)
          pid2 (spawn (fn [] (let [m (receive)] (deliver prom m))))
          pid3 (spawn (fn [] (link (resolve-name "remoteping"))
                        (deliver prom2 "go on")
                        (let [m (receive)] (deliver prom3 m))))]
      (send! (resolve-name "remoteping") [pid2 "hey"])
      (is (= "hey" @prom))
      @prom2
      (send! (resolve-name "remoteping") "exception")
      (is (= (:signal @prom3) :link-broken)))))


(deftest test-send
  (println "*** test-send")
  (let [prom (promise)
        pid1 (spawn jobim.examples.actors/ping)
        pid2 (spawn (fn [] (let [m (receive)] (deliver prom m))))]
    (send! pid1 [pid2 "hey"])
    (is (= "hey" @prom))))


(deftest test-link
  (println "*** test-link")
  (let [prom (promise)
        pid1 (spawn jobim.examples.actors/ping)
        pid2 (spawn (fn [] (link pid1) (send! pid1 "exception") (let [m (receive)] (deliver prom (:signal m)))))]
    (is (= @prom :link-broken))))

(deftest test-evented-actor
  (println "*** test-evented-actor")
  (let [prom (promise)
        evt (spawn-evented jobim.examples.actors/ping-evented)
        pid (spawn (fn [] (let [m (receive)] (deliver prom m))))]
    (send! evt [pid "hey"])
    (is (= "actor test evented says hey" @prom))))

(deftest test-evented-actor2
  (println "*** test-evented-actor2")
  (let [prom (promise)
        evt (spawn-evented "jobim.examples.actors/ping-evented-2")
        pid (spawn (fn [] (let [m (receive)] (deliver prom m))))]
    (send! evt [pid "hey"])
    (is (= "actor test evented2 says hey" @prom))
    (let [before (count (keys (deref jobim.core/*evented-table*)))]
      (send! evt "exit")
      (Thread/sleep 5000)
      (is (= (dec before)
             (count (keys (deref jobim.core/*evented-table*))))))))

(deftest test-fsm-1
  (println "*** test-fsm-1")
  (let [fsm (make-lock [1 2])]
    (is (= :locked (state fsm)))
    (push-button fsm 1)
    (push-button fsm 2)
    (is (= :open (state fsm)))
    (lock fsm)
    (is (= :locked (state fsm)))))
