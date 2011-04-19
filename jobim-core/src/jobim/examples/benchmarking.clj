(ns jobim.examples.benchmarking
  (:use [jobim]
        [jobim.utils]))

(defn avg
  ([c]
     (/ (apply + c) (count c))))

(defmacro with-time
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     (/ (double (- (. System (nanoTime)) start#)) 1000000.0)))

;; base time

(defn fib
  ([x] (if (< x 2)
         x
         (+ (fib (dec x)) (fib (- x 2))))))

;; Spawn test

(defn do-spawn-test
  ([x]
     (if (> x 0)
       (do (spawn (fn [] x))
           (do-spawn-test (- x 1)))
       :ok)))

(defn spawn-test
  ([total]
     (/ (with-time (do-spawn-test total)) total)))

(defn do-spawn-test-evented
  ([x]
     (if (> x 0)
       (do
         (spawn-evented (fn [] x))
         (do-spawn-test-evented (- x 1)))
       :ok)))

(defn spawn-test-evented
  ([total]
     (/ (with-time (do-spawn-test-evented total)) total)))

;; Send test

(defn send-test
  ([times]
     (let [acum (atom [])
           wait (promise)]
       (spawn #(do
                 (doseq [i (range 0 times)]
                   (let [t (with-time
                             (do (send! (self) 1)
                                 (receive)))]
                     (swap! acum conj t)))
                 (deliver wait @acum)))
       @wait)))

(defn send-test-evented
  ([times]
     (let [acum (atom [])
           wait (promise)]
       (spawn-evented #(react-loop [counter times]
                                   (if (> counter 0)
                                     (let [before (. System (nanoTime))]
                                       (send! (self) 1)
                                       (react [_]
                                              (let [after (. System (nanoTime))]
                                                (swap! acum conj (/ (double (- after before)) 1000000.0))
                                                (react-recur (dec counter)))))
                                     (deliver wait @acum))))
       @wait)))

;; ring

(defn make-ring
  ([c next initial prom]
     (react-loop [next next]
                   (do
                     (react [x]
                            (if (string? x)
                              (do
                               ;(println (str (self) ") LINKING TO " x))
                               (react-recur x))
                              (if (zero? x)
                                (do ;(println (str (self) ") EXITING!"))
                                    (if (not (= next initial))
                                      (do
                                        ;(println (str (self) ") SENDING 0 to " next))
                                        (send! next x)
                                        (react-break))
                                      (do
                                        ;(println (str (self) ") NEXT " next " == " initial))
                                        (deliver prom :ok)
                                        (react-break))))
                                (do
                                  ;(println (str (self) ") FORWARDING " (dec x) " to " next))
                                  (send! next (dec x))
                                  (react-recur next)))))))))

(defn ring-test
  ([num]
     (let [sync (promise)
           initial (spawn-evented #(make-ring 0 nil nil sync))]
       (loop [ids (range 1 num)
              last-pid initial]
         (do
           ;(println (str "iter " (first ids)))
           (if (empty? ids)
             (send! initial last-pid)
             (let [next-pid (spawn-evented #(make-ring (first ids) last-pid initial sync))]
               (recur (rest ids)
                      next-pid)))))
       (send! initial (* num 0))
       @sync)))

(defn ring-test-driver
  ([max num]
     (loop [vals (range 0 max)
            acum []]
       (if (empty? vals)
         acum
         (do
           (Thread/sleep 1000)
           (System/gc)
           (recur (rest vals)
                  (conj acum (with-time (ring-test num)))))))))

(defn actors-creation-test
  ([num] (with-time (doseq [i (range 0 num)] (spawn-evented #(+ 1 1))))))


;; Ping-Pong test

(defn ping-pong-server
  ([] (spawn
       #(loop [[from data] (receive)]
          (send! from data)
          (recur (receive))))))

(defn ping-pong-client
  ([server data]
     (do
       (send! server (mlist (self) data))
       (receive))))

(defn ping-pong-test
  ([num size]
     (let [data (vec (take size (repeat true)))
           remote (ping-pong-server)]
       (loop [times []
              it num]
         (if (= it 0)
           times
           (let [time (with-time (ping-pong-client remote data))]
             (recur (conj times time)
                    (dec it))))))))

(defn ping-pong-server-evented
  ([] (spawn-evented
       #(react-loop []
          (react [[from data]]
                 (do
                   (send! from data)
                   (react-recur)))))))


(defn ping-pong-test-evented
  ([num size]
     (let [data (vec (take size (repeat true)))
           remote (ping-pong-server-evented)
           sync (promise)]
       (spawn-evented
        #(react-loop [times []
                     it num]
             (if (= it 0)
               (deliver sync times)
               (let [time (with-time (send! remote (mlist (self) data)))]
                 (react [_]
                   (react-recur (conj times time)
                                (dec it)))))))
       @sync)))



;; Ping-Pong remote test

(defn ping-pong-remote-test
  ([remote-node num size]
     (let [data (apply mlist (vec (take size (repeat true))))
           remote (rpc-blocking-call (resolve-node-name remote-node)
                                     "jobim.examples.benchmarking/ping-pong-server" (mlist))]
       (loop [times []
              it num]
         (if (= it 0)
           times
           (let [time (with-time (ping-pong-client remote data))]
             (recur (conj times time)
                    (dec it))))))))

(defn ping-pong-remote-test-evented
  ([remote-node num size]
     (let [data  (apply mlist (vec (take size (repeat true))))
           remote (rpc-blocking-call (resolve-node-name remote-node)
                                     "jobim.examples.benchmarking/ping-pong-server-evented" (mlist))
           sync (promise)]
       (spawn-evented
        #(react-loop [times []
                      it num]
             (if (= it 0)
               (deliver sync times)
               (let [time (with-time (send! remote (mlist (self) data)))]
                 (react [_]
                   (react-recur (conj times time)
                                (dec it)))))))
       @sync)))
