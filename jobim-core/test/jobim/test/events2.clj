(ns jobim.test.events2
  (:use [jobim.events2] :reload)
  (:use [clojure.test]))

(run-multiplexer 2)


(deftest test-register-handle-send-event-1
  (println "*** test-register-handle-send-event-1")
  (let [counter (atom 0)
        counter2 (atom 0)
        prom1 (promise)
        prom2 (promise)
        handler (fn [data] (if (= (:key data) "test")
                             (do (swap! counter (fn [_] (:data data)))
                                 (deliver prom1 :ok))
                             (do (swap! counter2 (fn [_] (:data data)))
                                 (deliver prom2 :ok))))]
    (listen-once "test" handler)
    (listen-once "test2" handler)
    (publish "test" 1)
    (publish "test" 2)
    (publish "test2" 2)
    (publish "test3" 3)
    @prom1
    @prom2
    (is (= @counter 1))
    (is (= @counter2 2))))

(deftest test-register-handle-send-event-2
  (println "*** test-register-handle-send-event-2")
  (let [counter (atom 0)
        counter2 (atom 0)
        prom1 (promise)
        prom2 (promise)
        handler (fn [data]
                  (if (= (:key data) "testb")
                    (do (swap! counter (fn [_] (:data data)))
                        (deliver prom1 :ok))
                    (do (swap! counter2 (fn [_] (:data data)))
                        (deliver prom2 :ok))))]
    (listen "testb" handler)
    (listen "testb2" handler)
    (publish "testb" 1)
    (publish "testb2" 2)
    @prom1
    @prom2
    (println (str "let's check"))
    (is (= @counter 1))
    (is (= @counter2 2))))
