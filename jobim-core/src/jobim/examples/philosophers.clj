(ns jobim.examples.philosophers
  (:use [jobim]
        [matchure]))

;;;;;;;;;;;;;
;;; Forks
;;;;;;;;;;;;

(defn do-forks
  ([forks-list]
     (loop [forks-list forks-list]
       (let [msg (receive)]
         (cond-match
          [["grab-forks" ?left ?right] msg]       (recur (filter (fn [fork] (and (not= fork left)
                                                                                (not= fork right)))
                                                                forks-list))
          [["release-forks" ?left ?right] msg]    (recur (concat [left right] forks-list))

          [["available" ?left ?right ?from] msg]  (do (send! from ["are-available" (and (some #(= % right) forks-list)
                                                                                        (some #(= % left) forks-list))])
                                                      (recur forks-list))

          ["die" msg]                             (println "Forks put away\r\n"))))))

;; Public forks API

(defn grab-forks
  ([[left right]]
     (send! (resolve-name "forks") ["grab-forks" left right])))

(defn release-forks
  ([[left right]]
     (send! (resolve-name "forks") ["release-forks" left right])))

(defn forks-available?
  ([[left right]]
     (send! (resolve-name "forks") ["available" left right (self)])
     (let [msg (receive #(= (first %) "are-available"))]
       (not (nil? (second msg))))))

(defn forks-die
  ([] (send! (resolve-name "forks") "die")))

(defn register-forks
  ([num-forks]
     (register-name "forks" (spawn (fn [] (do-forks (range num-forks)))))))


;;;;;;;;;;;;;
;;;; Waiter
;;;;;;;;;;;;;

(defn process-wait-list
  ([clients-forks]
     (loop [client-forks clients-forks]
       (if (empty? client-forks)
         false
         (let [[client forks] (first client-forks)]
           (if (forks-available? forks)
             (do
               (send! client "served")
               true)
             (recur (rest clients-forks))))))))



(defn do-waiter
  ([wait-list client-count eating-count busy]
     (loop [msg          (receive)
            wait-list    wait-list
            client-count client-count
            eating-count eating-count
            busy         busy]
       (if (and (empty? wait-list)
                (= 0 client-count)
                (= 0 eating-count))
         (do (forks-die)
             (send! (resolve-name "dining-room") "all-gone"))
         (do
           (condp = (first msg)
               "waiting"  (let [client-forks (second msg)
                                wait-list (conj wait-list client-forks)
                                busy (if (and (not busy) (< eating-count 2))
                                       (process-wait-list wait-list)
                                       busy)]
                            (recur (receive) wait-list client-count eating-count busy))
               "eating"   (let [client-forks (second msg)]
                          (recur (receive) (filter #(not= % client-forks) wait-list)
                                 client-count (inc eating-count) false))
               "finished" (recur (receive) wait-list client-count (dec eating-count) (process-wait-list wait-list))
               "leaving"  (recur (receive) wait-list (dec client-count) eating-count busy)
               (do (println (str "STRANGE MESSAGE " msg))
                   (throw (Exception. "ERROR!")))))))))

;; Public waiter API

(defn waiter-wait
  ([forks]
     (send! (resolve-name "waiter") ["waiting" [(self) forks]])))

(defn waiter-eating
  ([forks]
     (send! (resolve-name "waiter") ["eating" [(self) forks]])))

(defn waiter-finished
  ([] (send! (resolve-name "waiter") ["finished"])))

(defn waiter-leaving
  ([] (send! (resolve-name "waiter") ["leaving"])))

(defn register-waiter
  ([clients]
     (register-name "waiter" (spawn (fn [] (do-waiter [] (dec clients), 0, false))))))

;;;;;;;;;;;;;;;;;;
;;;; Philospher
;;;;;;;;;;;;;;;;;;

(defn do-philosopher
  ([name & args]
     (loop [forks (first args)
            cycle (second args)]
       (if (= 0 cycle)
         (waiter-leaving)
         (do
           (println (str name " is thinking (" cycle " remaining).\r\n"))
           (Thread/sleep (rand 1000))
           (println (str name " is hungry.\r\n"))
           (waiter-wait forks)
           (let [_served (receive)]
             (grab-forks forks)
             (waiter-eating forks)
             (println (str name " is eating. \r\n")))
           (Thread/sleep (rand 1000))
           (release-forks forks)
           (waiter-finished)
           (recur forks (dec cycle)))))))


;; Run it

(defn dining
  ([] (let [clients 5
            life-span 20]
        (register-name "dining-room" (self))
        (register-forks clients)
        (register-waiter clients)
        (doseq [[philosopher forks] [["Plato" [4 0]] ["Aristotle" [0 1]] ["Kant" [1 2]] ["Descartes" [2 3]] ["Wittgenstein" [3 4]]]]
          (spawn (fn [] (do-philosopher philosopher forks life-span))))
        (receive)
        (println "Dining room closed.\r\n"))))
