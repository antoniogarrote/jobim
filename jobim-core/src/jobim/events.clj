(ns jobim.events
  (:use [lamina.core])
  (:use [clojure.contrib.logging :only [log]]))


(defonce *reactor-thread* (channel))
(defonce *events-channels* (atom {}))


;;; Reactor
(defn- reactor-thread
  ([reactor-queue]
     (future (loop [[key data] (wait-for-message reactor-queue)]
               (let [ch (get @*events-channels* key)]
                 (when (not (nil? ch))
                   (enqueue ch {:key key :data data}))
                 (recur (wait-for-message reactor-queue)))))))

(defn run-multiplexer
  ([num-threads]
     (let [reactor-queues (take  num-threads (repeatedly channel))] ;; create queues
       ;; create reactor threads
       (doseq [channel reactor-queues]
         (reactor-thread channel))
       ;; multiplexer thread
       (future
        (doseq [ch (cycle reactor-queues)]
          (let [item (wait-for-message *reactor-thread*)]
            (enqueue ch item)))))))

;; - register events + handlers as lambda + closures

(defn publish
  "Sends a new data to the events queue"
  ([key data]
     (enqueue *reactor-thread* [key data])))

(defn listen
  "Starts listening for events"
  ([key handler]
     (swap! *events-channels* (fn [chs] (let [ch (get chs key)]
                                         (if (nil? ch)
                                           (assoc chs key (let [ch (channel)]
                                                            (receive-all ch handler) ch))
                                           (assoc chs key (do (receive-all ch handler) ch))))))
     :ok))


(defn listen-once
  "Starts listening for a single event occurrence"
  ([key handler]
     (swap! *events-channels* (fn [chs] (let [ch (get chs key)]
                                         (if (nil? ch)
                                           (assoc chs key (let [ch (channel)]
                                                            (receive ch handler) ch))
                                           (assoc chs key (do (receive ch handler) ch))))))
     :ok))

(defn unlisten
  "Stops listening for events"
  ([key handler]
     (let [ch (get @*events-channels* key)]
       (when (not (nil? ch))
         (cancel-callback ch handler))
       :ok)))
