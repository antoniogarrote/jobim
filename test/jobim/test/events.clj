(ns jobim.test.events
  (:use [jobim.events] :reload)
  (:require [jobim.core :as jcore])
  (:use [clojure.test]))

(run-multiplexer 1)

(defn reset-multiplexer
  ([] (do
        (alter-var-root #'jobim.events/*events-handlers* (fn [_] (ref {})))
        (alter-var-root #'jobim.events/*events-queue* (fn [_] (java.util.concurrent.LinkedBlockingQueue.))))))


(deftest test-register-handle-send-event-1
 (let [counter (atom 0)
       counter2 (atom 0)
       prom1 (promise)
       prom2 (promise)
       handler (fn [data] (if (= (:key data) "test")
                            (do (swap! counter (fn [_] (:data data)))
                                (deliver prom1 :ok))
                            (do (swap! counter2 (fn [_] (:data data)))
                                (deliver prom2 :ok))))]
   (listen "test" handler)
   (listen "test2" handler)
   (Thread/sleep 1000)

   (publish "test" 1)

   (Thread/sleep 1000)
   (unlisten "test" handler)
   (publish "test" 2)
   (publish "test2" 2)
   (Thread/sleep 1000)
   (unlisten "test2" handler)
   (Thread/sleep 1000)
   (publish "test3" 3)
   @prom1
   @prom2
   (is (= @counter 1))
   (is (= @counter2 2))))

;; Now tested in jobim.test.jobim

;(deftest test-evented-actor
;  (let [acum (atom [])
;        pid (let [pid "34134134.56"
;                  rpid (jcore/pid-to-process-number pid)
;                  q (java.util.concurrent.LinkedBlockingQueue.)]
;              (dosync (alter jcore/*process-table* (fn [table] (assoc table rpid {:mbox q
;                                                                                  :dictionary {}}))))
;              (binding [jobim.core/*pid* pid]
;                (jcore/react-loop
;                 0 (fn [counter]
;                             (let [banner "-hey-"]
;                               (jcore/react (fn [msg]
;                                              (swap! acum (fn [old] (conj old (str msg banner counter))))
;                                              (jcore/react-recur (inc counter)))))))
;                      jobim.core/*pid*))]
;    (jcore/send-to-evented pid 0)
;    (jcore/send-to-evented pid 1)
;    (Thread/sleep 10000)
;    (is (= @acum ["0-hey-0" "1-hey-1"]))))
;
;(deftest test-evented-future
;  (let [acum (atom [])
;        prom (promise)
;        pid (let [pid "34134134.56"
;                  rpid (jcore/pid-to-process-number pid)
;                  q (java.util.concurrent.LinkedBlockingQueue.)]
;              (dosync (alter jcore/*process-table* (fn [table] (assoc table rpid {:mbox q
;                                                                                  :dictionary {}}))))
;              (binding [jobim.core/*pid* pid]
;                (jcore/react-loop
;                 0 (fn [counter]
;                     (let [banner "-hey-"]
;                       (jcore/react (fn [msg]
;                                      (swap! acum (fn [old] (conj old (str msg banner counter))))
;                                      (jcore/react-future (fn [] (+ 30 counter))
;                                                          (fn [val]
;                                                            (swap! acum (fn [old] (conj old (str msg banner val))))
;                                                            (if (= counter 1)
;                                                              (deliver prom :ok)
;                                                              (jcore/react-recur (inc counter))))))))))
;                      jobim.core/*pid*))]
;    (jcore/send-to-evented pid 0)
;    (jcore/send-to-evented pid 1)
;    @prom
;    (is (= @acum ["0-hey-0" "0-hey-30" "1-hey-1" "1-hey-31"]))))
