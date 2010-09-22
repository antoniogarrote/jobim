(ns jobim.examples.events
  (:use [jobim]
        [jobim.behaviours.events]))

(def-event-handler ConsoleLogger
  (init [this initial-state] initial-state)
  (handle-event [this event state]
                (println (str state " " event)))
  (terminate [this state] (println "exiting...")))


(defn start-example-event-manager
  ([] (start-event-manager "example-manager")))

(defn add-example-handler
  ([name] (add-handler (resolve-name name)
             [:console jobim.examples.events.ConsoleLogger]
             "got a message! :")))


;(notify (resolve-name "example-manager") "hey!")
