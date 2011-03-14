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
  ([name] (add-handler (resolve-name "example-manager")
             [name jobim.examples.events.ConsoleLogger]
             "got a message! :")))


;; Example code

; (start-example-event-manager)
; (add-example-handler :console)
; (add-example-handler :console2)
; (notify (resolve-name "example-manager") "hey!")
; (delete-handler (resolve-name "example-manager") [:console2 ConsoleLogger])
; (notify (resolve-name "example-manager") "hey!")
; (delete-handler (resolve-name "example-manager") ConsoleLogger)
