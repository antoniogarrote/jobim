(ns jobim.behaviours.fsm
  (:use [jobim]
        [matchure]))


(defprotocol FSM
  (init [this initial-message] "Returns the initial state of the FSM")
  (next-transition [this current-state state-data  message] "Defines which transtion will be applied provided the current state and the incoming message")
  (handle-info [this current-state state-data message] "Handles messages distinct to events")
  (terminate [this state-name state-data] "Clean up code when the FSM is going to be terminated externally"))


(defn action-ignore
  "Sets the same state and data for the FSM"
  ([state-name state-data msg] [:next-state state-name state-data]))

(defn action-next-state
  "Sets the new state for the FSM"
  ([next-state next-data] [:next-state next-state next-data]))

(defn action-stop
  "Stops the FSM"
  ([] [:stop :ignore :ignore]))

(defn fsm-actor
  "Defines the loop of a fsm actor"
  ([initial-state-name initial-state-data fsm]
     (loop [msg (receive)
            state-name initial-state-name
            state-data initial-state-data]
       (cond-match
        [[:fsm-next-state ?data] msg]  (let [transition-fn (next-transition fsm state-name state-data data)
                                             [action next-state-name next-state-data] (apply transition-fn [state-name state-data data])]
                                         (condp = action
                                           :next-state (recur (receive)
                                                              next-state-name
                                                              next-state-data)
                                           :stop       (terminate fsm state-name state-data)
                                           (throw (Exception. (str "Unknown action returned from transition function: " action)))))

        [_ msg]                        (let [[action next-state-name next-state-data] (handle-info fsm state-name state-data msg)]
                                         (condp = action
                                           :next-state (recur (receive)
                                                              next-state-name
                                                              next-state-data)
                                           :stop       (terminate fsm state-name state-data)
                                           (throw (Exception. (str "Unknown action returned from transition function: " action)))))))))

(defn fsm-evented-actor
  "Defiles the evented loop of a fsm actor"
  ([initial-state-name initial-state-data fsm]
     (react-loop [state-name initial-state-name
                  state-data initial-state-data]
                 (react [msg]
                        (cond-match
                         [[:fsm-next-state ?data] msg]  (let [transition-fn (next-transition fsm state-name state-data data)
                                                              [action next-state-name next-state-data] (apply transition-fn [state-name state-data data])]
                                                          (condp = action
                                                            :next-state (react-recur next-state-name
                                                                                     next-state-data)
                                                            :stop       (terminate fsm state-name state-data)
                                                            (throw (Exception. (str "Unknown action returned from transition function: " action)))))

                         [_ msg]                        (let [[action next-state-name next-state-data] (handle-info fsm state-name state-data msg)]
                                                          (condp = action
                                                            :next-state (react-recur next-state-name
                                                                                     next-state-data)
                                                            :stop       (terminate fsm state-name state-data)
                                                            (throw (Exception. (str "Unknown action returned from transition function: " action))))))))))


(defn start
  "Starts a new FSM behaviour"
  ([fsm initial-msg]
     (let [[initial-state-name initial-state-data] (init fsm initial-msg)
             pid (spawn #(fsm-actor initial-state-name initial-state-data fsm))]
       pid))
  ([name fsm initial-msg]
     (let [pid (start fsm initial-msg)]
       (register-name name pid))))


(defn start-evented
  "Starts a new evented FSM behaviour"
  ([fsm initial-msg]
     (let [[initial-state-name initial-state-data] (init fsm initial-msg)
           pid (spawn-evented #(fsm-evented-actor initial-state-name initial-state-data fsm))]
       pid))
  ([name fsm initial-msg]
     (let [pid (start-evented fsm initial-msg)]
       (register-name name pid))))

(defn send-event!
  "Sends a message to a FSM"
  ([pid msg]
     (send! pid [:fsm-next-state msg])))

(defmacro def-fsm
  ([name & defs]
     `(deftype ~name [] jobim.behaviours.fsm.FSM
        ~@defs)))
