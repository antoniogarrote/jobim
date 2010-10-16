(ns jobim.examples.actors
  (:use [jobim]
        [clojure.contrib.logging :only [log]]
        [matchure]))

(defn ping
  ([]
     (loop [continue true
            msg (receive)]
       (cond-match
        [#"exit" msg]       (recur false msg)
        [#"exception" msg]  (throw (Exception. "Ping actor received exception"))
        [[?from ?data] msg] (do (send! from data)
                                (recur true (receive)))))))

(defn ping-evented
  ([]
     (let [name "test evented"]
       (react-loop []
         (react [msg]
             (cond-match
                [#"exit" msg]       :exit
                [#"exception" msg]  (throw (Exception. "Ping actor received exception"))
                [[?from ?data] msg] (do (send! from (str "actor " name " says " data))
                                        (react-recur))))))))

(defn ping-evented-2
  ([]
     (let [name "test evented2"]
       (react-loop [should-exit false]
         (when (not should-exit)
           (react [msg]
                  (cond-match
                   [#"exit" msg]       (react-recur true)
                   [[?from ?data] msg] (do (send! from (str "actor " name " says " data))
                                           (react-recur false)))))))))

;; benchmark actors

(defn sink
  ([]
     (loop []
       (let [[from data] (receive)]
           (send! from [(self) (inc data)])
           (recur)))))

(defn ping-benchmarking-evented
  ([]
     (react-loop
      (react (fn [[from data]]
               (Thread/sleep (Math/floor (rand 1000)))
               (send! from [(self) data]))))))

(defn ping-benchmarking
  ([]
     (loop [msg (receive)]
       (cond-match
        [[?from ?data] msg]  (do (Thread/sleep (Math/floor (rand 1000)))
                                 (send! from [(self) data])
                                 (recur (receive)))))))
