(ns jobim.behaviours.supervisor
  (:use [jobim]
        [jobim.utils]
        [clojure.contrib.logging :only [log]]
        [matchure]))

(defn child-specification
  ([id function args]
     {:id id :function function :args args}))

(defonce *restart-strategies* [:one-for-one :one-for-all :rest-for-one])

(defn- check-supervisor-specification
  ([specification]
     (letfn [(check-restart-strategy [s] (if (in? *restart-strategies* (:restart-strategy s)) true
                                             (throw (Exception. (str "Wrong supervisor specification, unknown :restart-strategy '" s "', '"
                                                                     ":one-for-one, :one-for-all, :rest-for-one must be specified")))) s)
             (check-max-restarts [s] (if (number? (:max-restarts s)) s (throw (Exception. "A :max-restarts number must be specified"))))
             (check-max-time [s] (if (number? (:max-time s)) s (throw (Exception. "A :max-time number must be specified"))))
             (check-child-specification [s] (doseq [c (:child-specifications s)]
                                              (when (nil? (:id c)) (throw (Exception. "unkown :id for child specification " c)))
                                              (when (nil? (:function c)) (throw (Exception. "unkown :function for child specification " c)))
                                              (when (nil? (:args c)) (throw (Exception. "unkown :args for child specification " c)))) s)]
       (-> specification check-restart-strategy check-max-restarts check-max-time check-child-specification))))

(defn supervisor-specification
  ([restart-strategy max-restarts max-time child-specifications]
     (let [specification {:restart-strategy     restart-strategy
                          :max-restarts         max-restarts
                          :max-time             max-time
                          :child-specifications child-specifications}]
       (check-supervisor-specification specification))))

;; Supervisor thread

; child-table    -> atom -> map pid -> id
; specifications -> atom -> map id -> [f args]
; timer          -> obj  -> timer scheduling limit-checker-thread
; restarts-count -> ref  -> count of restarts

(defn- start-and-link-child
  "Starts and link to a child process"
  ([child-specification child-table]
     (let [id       (:id child-specification)
           function (:function child-specification)
           args     (:args child-specification)]
       (let [pid (require-apply function args)]
         (link pid)
         (swap! child-table #(assoc % pid id))))))

(defn- make-restart-limit-checker-thread
  "Periodically observes the count of restarts and send a signal to
   the supervisor in case the max-limit is reached"
  ([supervisor-pid restarts-count timer max-restarts max-time]
     (with-timer timer max-time
       (fn [] (if (<= @restarts-count max-restarts)
                (dosync (alter restarts-count (fn [_] 0)))
                (do (send! supervisor-pid :max-restarts-reached)
                    (cancel-timer timer)))))))

(defn- make-specifications-table
  ([specifications]
     (atom (reduce (fn [m s] (assoc m (:id s) s)) specifications))))


(defn- make-child-pid-table
  ([child-table specifications]
     (doseq [s specifications]
       (start-and-link-child s child-table))
     child-table)

  ([specifications]
     (let [child-table (atom {})]
       (make-child-pid-table child-table specifications))))


(defn- remove-from-child-pid-table
  ([child-table pid]
     (swap! child-table #(dissoc % pid))))

(defn- specification-for-pid
  ([child-pids-table specifications-table pid]
     (let [id (get @child-pids-table pid)]
       (get @specifications-table id))))

(defn- terminate-children
  "Sends terminate messages to all the processes and waits until all the
   processes have finished execution"
  ([pids]
     (doseq [pid pids]
       (send! pid :terminate))
     (loop [remaining pids]
       (if (empty? remaining) :ok
           (do (Thread/sleep 1000)
               (recur (reduce
                       (fn [acum pid]
                         (if (nil? (process-info pid))
                           acum
                           (conj acum pid)))
                       [] pids)))))))

(defn- reset-child-table
  ([child-pid-table] (swap! child-pid-table (fn [_] {}))))

(defn- find-rest-pids
  ([child-pids pid]
     (loop [to-check child-pids]
       (if (= pid (first to-check))
         (rest to-check)
         (recur (rest to-check))))))

(defn- apply-restart-strategy
  ([from cause supervisor-specification child-pids-table child-specifications-table]
     (log :debug (str "*** supervisor " (self)  " : got exception from  " from ", cause: " cause))
     (let [pid-specification (specification-for-pid child-pids-table child-specifications-table from)
           rest-pids (find-rest-pids (reverse (keys @child-pids-table)) from)
           rest-pid-specifications (doall (concat [pid-specification]
                                                  (map #(specification-for-pid child-pids-table child-specifications-table %) rest-pids)))]
       ;; remove the failing process
       (remove-from-child-pid-table child-pids-table from)
       ;; Apply the right action according to the restart strategy
       (condp = (:restart-strategy supervisor-specification)
           ;; Only the failing child is restarted
           :one-for-one  (do (Thread/sleep 1000)
                             (start-and-link-child pid-specification child-pids-table))
           ;; Remaining processes are terminated and all restarted
           :one-for-all  (do (terminate-children (keys @child-pids-table))
                             (reset-child-table child-pids-table)
                             (make-child-pid-table child-pids-table (:child-specifications supervisor-specification)))
           ;; Rest of processes in the specification are terminated and
           ;; restarted altogether with the failing process
           :rest-for-one (do (terminate-children rest-pids)
                             (Thread/sleep 1000)
                             (doseq [pid rest-pids]
                               (swap! child-pids-table #(dissoc % pid)))
                             (doseq [specification rest-pid-specifications]
                               (start-and-link-child specification child-pids-table)))
           (throw (Exception. (str "Unknown restart-strategy "
                                   (:restart-strategy supervisor-specification))))))))

(defn- supervisor-loop
  "Supervisor main loop"
  ([timer restarts-count supervisor-specification]
     (let [child-specifications-table (make-specifications-table (:child-specifications supervisor-specification))
           child-pids-table           (make-child-pid-table (:child-specifications supervisor-specification))]
       (loop [msg (receive)]
         ;; Handle the child messages / signals
         ;; with the right semantics
         (cond-match
          [{:signal :link-broken, :from ?from, :cause ?cause} msg]
             (do (dosync (alter restarts-count #(inc %)))
                 (apply-restart-strategy from cause supervisor-specification child-pids-table child-specifications-table))
           [:max-restarts-reached msg]
             (do (println (str "MAX RESTARTS REACHED"))
                 (terminate-children (keys @child-pids-table))
                 (throw (Exception. (str "Max numer of restarts reached in supervisor: " (self)))))
           [?other msg]
             (println (str "MESSAGE: " msg)))
         (recur (receive))))))

(defn start
  "Starts a new supervisor"
  ([supervisor-specification]
     (let [timer (make-timer)
           restarts-count (ref 0)
           supervisor-pid (spawn #(supervisor-loop timer restarts-count supervisor-specification))]
       (make-restart-limit-checker-thread supervisor-pid
                                          restarts-count
                                          timer
                                          (:max-restarts supervisor-specification)
                                          (:max-time supervisor-specification))
       supervisor-pid))
  ([name supervisor-specification]
     (let [pid (start supervisor-specification)]
       (register-name name pid) pid)))
