(ns jobim.events2
  (:use [lamina.core])
  (:use [clojure.contrib.logging :only [log]]))


(defonce *events-channels* (atom {}))
(defonce *named-callbacks* (atom {}))
(defonce *named-callbacks-counter* (atom 0))

;;; Reactor
(defn reactor-thread
  ([reactor-queue]
     (future (loop [[key data] (wait-for-message reactor-queue)]
               (let [ch (get @*events-channels* key)]
                 (when (not (nil? ch))
                   (enqueue ch {:key key :data data}))
                 (recur (wait-for-message reactor-queue)))))))

(defn run-multiplexer
  ([num-threads]
     (let [reactor-queues (map (fn [_] (channel)) )])
     (future (loop [[key data] (wait-for-message *reactor-thread*)]
               (let [ch (get @*events-channels* key)]
                 (when (not (nil? ch))
                   (enqueue ch {:key key :data data}))
                 (recur (wait-for-message *reactor-thread*)))))))

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
                                                            (receive ch handler) ch))
                                           (assoc chs key (do (receive ch handler) ch))))))
     :ok))

(defn listen-once
  "Starts listening for a single event occurrence"
  ([key handler]
     (let [autodelete-id (swap! *named-callbacks-counter* inc)
           channel  (do (swap! *events-channels* (fn [chs] (let [ch (get chs key)]
                                                            (if (nil? ch)
                                                              (assoc chs key (let [ch (channel)] ch))
                                                              chs))))
                        (get @*events-channels* key))
           autodelete-handler (fn [v] (let [[self ch] (get @*named-callbacks* autodelete-id)]
                                       (cancel-callback ch self)
                                       (swap! *named-callbacks* (fn [clbs] (dissoc clbs autodelete-id)))
                                       (handler v)))]
       (swap! *named-callbacks* (fn [clbs] (assoc clbs autodelete-id [autodelete-handler channel])))
       (listen key autodelete-handler)
       :ok)))

(defn unlisten
  "Stops listening for events"
  ([key handler]
     (let [ch (get @*events-channels* key)]
       (when (not (nil? ch))
         (cancel-callback ch handler))
       :ok)))
