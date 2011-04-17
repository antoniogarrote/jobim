(ns jobim.events
  (:use [lamina.core])
  (:use [clojure.contrib.logging :only [log]])
  (:import [java.util LinkedList HashMap])
  (:import [java.util.concurrent LinkedBlockingQueue]))


(defonce ^LinkedBlockingQueue *reactor-thread* (LinkedBlockingQueue.))
(defonce ^HashMap *events-handlers* (HashMap.))
(defonce ^HashMap *events-queues* (HashMap.))


;;; Reactor
(defn- do-handler
  ([handler data]
     (try
       (handler data)
       (catch Exception ex
         (do (println (str "*** Exception handling event in reactor thread: " (.getMessage ex)  " " (vec (.getStackTrace ex)))))))))

(defn- process-listen
  ([{:keys [key handler]}]
     (do (.put *events-handlers* key {:handler handler :kind :persistent})
         (let [^LinkedList evts-queue (get *events-queues* key)]
           (if (nil? evts-queue)
             (.put *events-queues* key (LinkedList.))
             (when (not (.isEmpty evts-queue))
               (let [data (.removeFirst evts-queue)]
                 (do-handler handler {:key key :data data}))))))))

(defn- process-finish
  ([{:keys [key]}]
     (.remove *events-handlers* key)
     (.remove *events-queues* key)))

(defn- process-listen-once
  ([{:keys [key handler]}]
     (let [_ (when (nil? (get *events-queues* key))
               (.put *events-queues* key (LinkedList.)))
           ^LinkedList queue (get *events-queues* key)]
       (if (.isEmpty queue)
         (.put *events-handlers* key {:handler (fn [data]
                                                 (.remove *events-handlers* key)
                                                 (.remove *events-queues* key)
                                                 (do-handler handler data))
                                      :kind :ephemeral})
         (let [evt (.removeFirst queue)]
           (.remove *events-handlers* key)
           (.remove *events-queues* key)
           (do-handler handler {:key key :data evt}))))))

(defn- process-publish
  ([key data]
     
     (let [^LinkedList queue (get *events-queues* key)
           handler (get *events-handlers* key)]
       
       (when (and (nil? queue) (nil? handler))
         (let [queue (LinkedList.)]
           (.add queue data)
           (.put *events-queues* key queue)))
         
       (when (and (nil? queue) (not (nil? handler)))
         (let [{:keys [handler kind]} handler]
           (if (= :persistent kind)
             (do-handler handler {:key key :data data})
             (do (.remove *events-handlers* key)
                 (do-handler handler {:key key :data data})))))

       (when (and (not (nil? queue)) (nil? handler))
         (.add queue data))

       (when (and (not (nil? queue)) (not (nil? handler)))
         (let [{:keys [handler kind]} handler]
           (if (= :persistent kind)
             (do-handler handler {:key key :data data})
             (do (.remove *events-handlers* key)
                 (.remove *events-queues* key)
                 (do-handler handler {:key key :data data}))))))))

(defn- do-reactor-thread
  ([key data]
     (condp = key
         :listen (process-listen data)
         :finish (process-finish data)
         :listen-once (process-listen-once data)
         (process-publish key data))))

(defn- reactor-thread
  ([^LinkedBlockingQueue reactor-queue]
     (.start (Thread. #(do
                         (.setName (Thread/currentThread) (str "Jobim Reactor Thread - " (.getId (Thread/currentThread))))
                         (loop [[key data] (.take reactor-queue)]
                           (do
                             (do-reactor-thread key data)
                             (recur (.take reactor-queue)))))))))

(defn run-multiplexer
  ([num-threads]
     (let [reactor-queues (take  num-threads (repeatedly #(LinkedBlockingQueue.)))] ;; create queues
       ;; create reactor threads
       (doseq [channel reactor-queues]
         (reactor-thread channel))
       ;; multiplexer thread
       (.start (Thread.
                #(do
                   (.setName (Thread/currentThread) "Jobim Multiplexer Thread")
                   (doseq [^LinkedBlockingQueue ch (cycle reactor-queues)]
                     (let [item (.take *reactor-thread*)]
                       ;(.put ch item)
                       ; Not using reactor threads
                       (do-reactor-thread (first item) (second item))
                       ))))))))

;; - register events + handlers as lambda + closures

(defn publish
  "Sends a new data to the events queue"
  ([key data]
     (.put *reactor-thread* [key data])))

(defn listen
  "Starts listening for events"
  ([key handler]
     (publish :listen {:key key :handler handler})
     :ok))

(defn finish
  "Closes a channel and removes it from the channels map"
  ([key]
     (publish :finish {:key key })
     :ok))


(defn listen-once
  "Starts listening for a single event occurrence"
  ([key handler]
     (publish :listen-once {:key key :handler handler})
     :ok))
