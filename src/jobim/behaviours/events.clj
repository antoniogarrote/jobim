(ns jobim.behaviours.events
  (:use [jobim]
        [jobim.utils]
        [clojure.contrib.logging :only [log]]
        [matchure]))


;; Protocols

(defprotocol EventManager
  (add-handler [this handler-protocol initial-state]
               "Adds a new handler to the EventManager")
  (notify [this event]
          "Notifies the EventManager about one event")
  (delete-handler [this handler-protocol]
                  "Deletes a handler from the EventManager")
  (stop [this]
        "Stops the EventManager and all registered event handlers"))

(defprotocol EventHandler
  (init [this initial-state]
        "Starts the event handler")
  (handle-event [this event state]
                "Handles one event sent from the EventManager where
                 the handler is registered")
  (terminate [this state]
             "Called when the event manager will stop receiving notifications"))

;; Event Handler impl.

(defn event-handler-actor
  ([state implementation]
     (fn []
       (loop [state state
              msg (receive)]
         (log :debug (str "*** handler " (self) " got something " msg))
         (cond-match
          [[:event ?evt] msg] (let [result (handle-event implementation evt state)]
                                (when (not= result :remove-handler)
                                  (recur (second result)
                                         (receive))))
          [:terminate] (terminate implementation state))))))


;; Event Manager impl.

(defn- do-add-handler
  ([state handler-protocol initial-state kind]
     (let [handlers (:handlers @state)
           prot (if (coll? handler-protocol) (second handler-protocol) handler-protocol)
           handler-id (if (coll? handler-protocol) (first handler-protocol) (str "_handler_" (gensym)))
           inst (eval-class prot)
           prot-handlers (or (get handlers (class inst)) {})]
       (try
         (let [initial-state-p (init inst initial-state)
               pid (spawn (event-handler-actor initial-state-p inst))
               prot-handlers-p (assoc prot-handlers handler-id pid)
               handlers-p (assoc handlers (class inst) prot-handlers-p)]
           (swap! state (fn [old-state] (assoc old-state :handlers handlers-p))))
        (catch Exception ex (do
                              (log :error (str "Exception initializing handler " (.getMessage ex) "\r\n" (.getStackTrace ex)))
                              [:exception (str "Exceptiona adding handler " (.getMessage ex))]))))))

(defn- do-stop
  ([pids]
     (doseq [pid pids]
       (send! pid [:terminate]))))

(defn- do-delete-handler
  ([state handler-protocol]
     (let [handlers (:handlers @state)
           prot (if (coll? handler-protocol) (second handler-protocol) handler-protocol)
           handler-id (if (coll? handler-protocol) (first handler-protocol) nil)
           inst (if (string? prot) (eval-class prot) (eval-class prot))
           prot-handlers (or (get handlers (class inst)) {})]
       (try
         (if (nil? handler-id)
           (let [pids (vals prot-handlers)]
             (do-stop pids)
             (let [handlers-p (dissoc handlers (class inst))]
               (swap! state (fn [old-state] (assoc old-state :handlers handlers-p)))))
           (let [pid (get prot-handlers handler-id)]
             (do-stop [pid])
             (let [prot-handlers-p (dissoc prot-handlers handler-id)
                   handlers-p (assoc handlers (class inst) prot-handlers-p)]
               (swap! state (fn [old-state] (assoc old-state :handlers handlers-p))))))
        (catch Exception ex [:exception (str "Exceptiona adding handler " (.getMessage ex))])))))

(defn- do-notify
  ([pids event]
     (doseq [pid pids]
       (send! pid [:event event]))))


(deftype EventManagerImpl [state] jobim.behaviours.events.EventManager
  (add-handler [this handler-protocol initial-state]
               (do-add-handler state handler-protocol initial-state :not-evented))
  (notify [this event]
          (do-notify  (->> @state :handlers vals (map vals) (apply concat)) event))
  (delete-handler [this handler-protocol]
                  (do-delete-handler state handler-protocol))
  (stop [this]
        (do-stop  (->> @state :handlers vals (map vals) (apply concat)))))

(defn event-manager-actor
  ([implementation]
     (fn []
       (log :info (str "*** event-manager " (self) " starting! "))
       (loop [msg (receive)]
         (log :debug (str "*** event-manager " (self) " got something " msg))
         (cond-match
          [[:add-handler ?handler-protocol ?initial-state] msg] (do (add-handler implementation handler-protocol initial-state)
                                                                    (recur (receive)))

          [[:delete-handler ?handler-protocol] msg]             (do (delete-handler implementation handler-protocol)
                                                                    (recur (receive)))

          [[:notify ?evt] msg]                                  (do (notify implementation evt)
                                                                    (recur (receive)))

          [:stop msg]                                           (stop implementation)

          [:terminate msg]                                      (stop implementation)

          [_ msg]                                               (recur (receive)))))))

;; utility functions

(defn start-event-manager
  ([] (let [evt-manager (jobim.behaviours.events.EventManagerImpl.
                         (atom {:handlers {}}))]
        (spawn (event-manager-actor evt-manager))))
  ([name]
     (let [pid (start-event-manager)]
       (register-name name pid)
       pid)))

(defmacro def-event-handler
  ([name & defs]
     `(deftype ~name [] jobim.behaviours.events.EventHandler
        ~@defs)))

(defn add-handler
  ([pid handler-protocol-or-identifier initial-value]
     (send! pid [:add-handler handler-protocol-or-identifier initial-value])
     :ok))

(defn delete-handler
  ([pid handler-protocol-or-identifier]
     (send! pid [:delete-handler handler-protocol-or-identifier])
     :ok))

(defn notify
  ([pid event]
     (send! pid [:notify event])))

(defn stop
  ([pid]
     (send! pid :stop)))
