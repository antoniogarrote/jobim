(ns jobim.test.jobim-local
  (:use [jobim] :reload)
  (:use [jobim.examples.actors] :reload)
  (:use [jobim.examples.fsm] :reload)
  (:use [jobim.behaviours.fsm] :reload)
  (:use [clojure.test])
  (:require [jobim.examples.server :as jserver]))


;; initialization of the local node
(use 'jobim.services.serialization.java)
(use 'jobim.services.messaging.localnode)
(use 'jobim.services.coordination.localnode)
(bootstrap-local "local-test")
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
 (let [*remote-node-name* "local-test"
       pid1 (rpc-blocking-call (resolve-node-name *remote-node-name*) "jobim/spawn" ["jobim.examples.actors/ping"])]
   (rpc-blocking-call (resolve-node-name *remote-node-name*) "jobim/register-name" ["remoteping" pid1])
   (is (= pid1 (get (registered-names) "remoteping")))
   (let [prom (promise)
         prom2 (promise)
         prom3 (promise)
         pid2 (spawn (fn [] (let [m (receive)] (deliver prom m))))
         pid3 (spawn (fn [] (link (resolve-name "remoteping"))
                       (deliver prom2 "go on")
                       (let [m (receive)]
                         (deliver prom3 m))))]
     (send! (resolve-name "remoteping") [pid2 "hey"])
     (is (= "hey" @prom))
     @prom2
     (send! (resolve-name "remoteping") "exception")
     (is (= (:signal @prom3) :link-broken)))))


(deftest test-process-alive
  (println "*** test-process-alive")
  (let [prom (promise)
        pid (spawn (fn [] (let [m (receive)] (deliver prom m))))]
    (is (process-alive? pid))
    (send! pid :exit)
    (Thread/sleep 1000)
    @prom
    (is (not (process-alive? pid)))))

(deftest test-sending-non-existent
  (println "*** test-sending-non-existent")
  (let [pid (spawn (fn [] :nothing))]
    (is (not (process-alive? pid)))
    (is (= :ok (send! pid :something)))))

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
 
(deftest test-server-1
  (println "*** test-server1")
  (let [server (jserver/make-channel-manager)
        chn (jserver/alloc server)]
    (is (not (nil? chn)))
    (jserver/free server chn)
    (is (= chn (jserver/alloc server)))))
 
(deftest test-server-2
  (println "*** test-server2")
  (let [server (jserver/make-channel-manager-evented)
        chn (jserver/alloc server)]
    (is (not (nil? chn)))
    (jserver/free server chn)
    (is (= chn (jserver/alloc server)))))


(deftest test-selective-reception-not-evented-1
(println "*** test-selective-reception-not-evented-1")
(let [step1 (promise)
      step2 (promise)
      pid1 (spawn #(let [v1 (receive odd?)
                         _ (deliver step1 v1)
                         v2 (receive)
                         _ (deliver step2 v2)]
                     :exit))]
  (send! pid1 2)
  (send! pid1 1)
  (is (= 1 @step1))
  (is (= 2 @step2))))

(deftest test-selective-reception-not-evented-2
(println "*** test-selective-reception-not-evented-2")
(let [step1 (promise)
      step2 (promise)
      step3 (promise)
      pid1 (spawn #(let [v1 (receive odd?)
                         _ (deliver step1 v1)
                         v2 (receive (fn [x] (> x 4)))
                         _ (deliver step2 v2)
                         v3 (receive)
                         _ (deliver step3 v3)]
                     :exit))]
  (send! pid1 2)
  (send! pid1 1)
  (send! pid1 6)
  (is (= 1 @step1))
  (is (= 6 @step2))
  (is (= 2 @step3))))

(deftest test-selective-reception-evented-1
  (println "*** test-selective-reception-evented-1")
  (let [step1 (promise)
        step2 (promise)
        pid1 (spawn-evented (fn [] (jobim.core/react odd?
                                                    (fn [msg]
                                                      (do (deliver step1 msg)
                                                          (jobim.core/react (fn [msg] (deliver step2 msg))))))))]
    (send! pid1 2)
    (send! pid1 1)
    (is (= 1 @step1))
    (is (= 2 @step2))))
