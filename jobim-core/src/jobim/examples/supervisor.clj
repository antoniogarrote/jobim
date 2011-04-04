(ns jobim.examples.supervisor
  (:use [jobim])
  (:require [jobim.behaviours.server :as gen-server]
            [jobim.behaviours.supervisor :as supervisor]))


;; Test server

(gen-server/def-server ExceptionTestServer

  (init [this _] [])

  (handle-call [this request from state]
               (println (str "RECEIVED " request " PID " (self) " ALIVE"))
               (gen-server/reply :alive []))

  (handle-cast [this request state] (if (= request :exception)
                                      (throw (Exception. (str "I DIED! " (self))))
                                      (gen-server/noreply state)))

  (handle-info [this request state] (gen-server/noreply state))

  (terminate [this state] (println (str "CALLING TO TERMINATE " (self)))))

;; Public interface

(defn make-test-client
  ([n] (gen-server/start (str "test-client-" n)  (ExceptionTestServer.) [])))


(defn start-supervisor
  ([] (start-supervisor :one-for-one))
  ([restart-strategy]
     (supervisor/start
      (supervisor/supervisor-specification
       restart-strategy                 ; restart strategy -> from arguments
       1                                ; one restart max
       20000                            ; each 20 secs
       ; Children specifications
       [(supervisor/child-specification
         "test-client-1"
         "jobim.examples.supervisor/make-test-client"
         [1])
        (supervisor/child-specification
         "test-client-2"
         "jobim.examples.supervisor/make-test-client"
         [2])
        (supervisor/child-specification
         "test-client-3"
         "jobim.examples.supervisor/make-test-client"
         [3])]))))


(defn ping-client
  ([n] (gen-server/send-call! (resolve-name (str "test-client-" n)) :ping)))


(defn trigger-exception
  ([n] (gen-server/send-cast! (resolve-name (str "test-client-" n)) :exception)))
