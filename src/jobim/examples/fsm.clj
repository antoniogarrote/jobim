(ns jobim.examples.fsm
  (:use [jobim]
        [jobim.behaviours.fsm]
        [matchure]))

(defn- partial-match?
  ([code so-far]
     (if (empty? so-far) true
         (if (= (first code) (first so-far))
           (recur (rest code) (rest so-far))
           false))))

(defn- handle-button
  ([current-state current-data message]
     (let [[_ code] message
           so-far (conj (:so-far current-data) code)]
       (if (= (:code current-data) so-far)
         (action-next-state :open (assoc current-data :so-far so-far))
         (if (partial-match? (:code current-data) so-far)
           (action-next-state :locked (assoc current-data :so-far so-far))
           (action-next-state :locked (assoc current-data :so-far [])))))))

(defn- handle-lock
  ([current-state current-data message]
     (action-next-state :locked (assoc current-data :so-far []))))

;; FSM Lock type

(def-fsm Lock
  (init [this code] [:locked {:so-far [] :code code}])
  (next-transition [this state-name state-data message]
                   (let [[topic _] message]
                     (condp = [state-name topic]
                       [:locked :button] handle-button
                       [:open   :lock]  handle-lock
                       action-ignore)))
  (handle-info [this current-state current-data message]
               (do
                 (cond-match
                  [[?from :state] message] (send! from current-state))
                 (action-next-state current-state current-data))))


;; public interface

(defn make-lock
  ([combination]
     (start (jobim.examples.fsm.Lock.) combination)))

(defn make-lock-evented
  ([combination]
     (start-evented (jobim.examples.fsm.Lock.) combination)))

(defn push-button
  ([fsm number]
     (send-event! fsm [:button number])))

(defn lock
  ([fsm]
     (send-event! fsm [:lock :ignore])))

(defn state
  ([fsm]
     (send! fsm [(self) :state])
     (receive)))
