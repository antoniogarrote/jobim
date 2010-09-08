(ns jobim.events
  (:use [clojure.contrib.logging :only [log]])
  (:import java.util.concurrent.LinkedBlockingQueue))

;; Global registry of events and events handlers
(def *events-handlers* (ref {}))


;; Global events queue
(def *events-queue* (LinkedBlockingQueue.))


;;; multiplexer
;; - Mantains a registry of events + handlers
;; - Runs an event loop
;; - executes handlers

(defn- match-handlers
  ([evt]
     (let [key (:key evt)
           handlers (get @*events-handlers* key)]
       (if (or (nil? handlers) (empty? handlers)) nil
           handlers))))

(defn- multiplexer-thread
  "Single multiplexer logic"
  ([]
     (loop [evt (.take *events-queue*)]
       (if-let [handlers (match-handlers evt)]
         (doseq [handler handlers]
           (try (apply handler [(assoc evt :handler handler)])
                (catch Exception ex
                  (log :error (str "Error applying evented actor: " (.getMessage ex) " " (vec (.getStackTrace ex)))))))
         (.put *events-queue* evt))
       (recur (.take *events-queue*)))))

(defn run-multiplexer
  "Starts processing incoming events for all the handlers"
  ([num-threads]
     (doseq [i (range 0 num-threads)]
       (future (multiplexer-thread)))
     :ok))


;;; Reactor

;; - register events + handlers as lambda + closures

(defn publish
  "Sends a new data to the events queue"
  ([key data]
     (.put *events-queue* {:key key :data data})))

(defn listen
  "Starts listening for events"
  ([key handler]
     (if (get @*events-handlers* key)
       (dosync (alter *events-handlers* (fn [table] (assoc table key
                                                           (conj (get table key) handler)))))
       (dosync (alter *events-handlers* (fn [table] (assoc table key [handler])))))))

(defn unlisten
  "Stops listening for events"
  ([key handler]
     (when (get @*events-handlers* key)
       (dosync (alter *events-handlers*
                      (fn [table] (let [old-handlers (get table key)
                                        new-handlers (filter #(not= %1 handler) old-handlers)]
                                    (assoc table key new-handlers))))))))
