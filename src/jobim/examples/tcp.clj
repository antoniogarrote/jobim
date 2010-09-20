(ns jobim.examples.tcp
  (:use [jobim]
        [jobim.behaviours.tcp]
        [clojure.contrib.logging :only [log]]
        [matchure]))

(defn server-actor
  ([]
     (let [s (start-server 12345)]
       (loop [msg (receive)
              counter 0]
         (cond-match
          [[:tcp ?tcp-act ?data] msg]  (do (println (str (self)  " - RECEIVED " (String. data)))
                                           (send-tcp! tcp-act (.getBytes (str "PONG-" counter "-" (String. data))))
                                           (recur (receive) (inc counter)))
          [[:tcp :close ?ex] msg]      (do (println (str (self) " - TCP socket closed: " ex))
                                           (println (str "exiting..."))))))))

(defn client-actor
  ([c st]
     (log :error (str "Starting client " c " " st))
     (let [cs (start-client "localhost" 12345)]
       (loop [r 2]
         (when (> r 0)
           (do
             (send-tcp! cs (str "count-" c))
             (Thread/sleep st)
             (let [[tcp ch msg] (receive)]
               (log :error (str "Client " c " val " (String. msg)))
               (recur (dec r)))))))))


;; (use 'jobim)
;; (use 'jobim.behaviours.tcp)
;;
;; (bootstrap-node "node-config.clj")
;; (spawn-in-repl)
;;
;; (def *s* (spawn jobim.behaviours.tcp/server-actor))
;; (spawn jobim.behaviours.tcp/first-client)
;; (spawn jobim.behaviours.tcp/second-client)
