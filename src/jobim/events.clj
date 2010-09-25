(ns jobim.events
  (:use [clojure.contrib.logging :only [log]]))

;; Global registry of events and events handlers
(def *events-handlers* (atom {}))

;; Notification queues
(def *notification-queues* (ref [[] []]))

;; Main notification queue
(def *main-notification-queue* (java.util.concurrent.LinkedBlockingQueue.))

;; Global events queue
(def *events-queue* (atom []))

;; Global lock
(def *events-queue-lock* (java.util.concurrent.locks.ReentrantLock.))

(defn- with-queue-critical-section
  ([f]
     (do
       (.lock *events-queue-lock*)
         (try (f)
              (finally
               (.unlock *events-queue-lock*))))))

(defn- register-multiplexer-queue
  ([]
     (let [q (java.util.concurrent.LinkedBlockingQueue.)]
       (dosync (alter *notification-queues*
                      (fn [[rem next]]
                        [(conj rem q) next])))
       q)))

(defn- next-queue
  ([]
     (dosync (let [[rem next] @*notification-queues*
                   q (if (empty? rem) (first next) (first rem))
                   next-p (if (empty? rem) [q] (conj next q))
                   rem-p (if (empty? rem) (rest next) (rest rem))]
               (alter *notification-queues* (fn [[rem next]]
                                              [rem-p next-p]))
               q))))

;;; multiplexer
;; - Mantains a registry of events + handlers
;; - Runs an event loop
;; - executes handlers

(defn remove-fire-once-handlers
  ([events-handlers key]
     (let [handlers (get @*events-handlers* key)
           handlers-p (filter (fn [h]  (not (:once (meta h)))) handlers)]
       (swap! *events-handlers* (fn [ehs] (assoc ehs key handlers-p))))))

(defn- match-handlers
  ([evt]
     (let [key (:key evt)
           handlers (get @*events-handlers* key)]
       (if (or (nil? handlers) (empty? handlers)) nil
           (do (remove-fire-once-handlers *events-handlers* key)
               handlers)))))


(defn- find-event-queue
  ([]
     (let [result (find-event-queue @*events-queue* [])]
       (if (nil? result)
         nil
         (let [[to-return queue-p] result]
           (swap! *events-queue* (fn [old] (vec queue-p)))
           to-return))))
  ([remaining acum]
     (if (empty? remaining) nil
         (let [fst (first remaining)
               handlers (match-handlers fst)]
           (if (nil? handlers)
             (recur (rest remaining) (conj acum fst))
             [[fst handlers] (concat acum (rest remaining))])))))


(defn- collector-thread
  ([]
     (loop [notif (.take *main-notification-queue*)]
       (let [q (next-queue)]
         (.put q notif)
         (recur (.take *main-notification-queue*))))))

(defn- multiplexer-thread
  "Single multiplexer logic"
  ([]
     (let [notif-queue (register-multiplexer-queue)]
       (loop [_ (.take notif-queue)
              result (with-queue-critical-section find-event-queue)]
         (do
           (when (not (nil? result))
             (let [[evt handlers] result]
               (doseq [handler handlers]
                 (try
                     (apply handler [(assoc evt :handler handler)])
                   (catch Exception ex
                     (log :error (str "Error applying evented actor: " (.getMessage ex) " " (vec (.getStackTrace ex)))))))))
           (recur (.take notif-queue)
                  (with-queue-critical-section find-event-queue)))))))

(defn run-multiplexer
  "Starts processing incoming events for all the handlers"
  ([num-threads]
     (doseq [i (range 0 num-threads)]
       (future (multiplexer-thread)))
     (.start (Thread. #(collector-thread)))
     :ok))


;;; Reactor

;; - register events + handlers as lambda + closures

(defn publish
  "Sends a new data to the events queue"
  ([key data]
     (with-queue-critical-section
       (fn [] (swap! *events-queue* (fn [old-queue]
                                      (vec (conj old-queue {:key key :data data}))))))
     (.put *main-notification-queue* :published)))

(defn listen
  "Starts listening for events"
  ([key handler]
     (if (get @*events-handlers* key)
       (with-queue-critical-section #(swap! *events-handlers*
                                            (fn [table] (assoc table key
                                                               (conj (get table key) handler)))))
       (with-queue-critical-section #(swap! *events-handlers*
                                            (fn [table] (assoc table key [handler])))))
     (.put *main-notification-queue* :listening)))

(defn listen-once
  "Starts listening for a single event occurrence"
  ([key handler]
     (if (get @*events-handlers* key)
       (with-queue-critical-section #(swap! *events-handlers*
                                            (fn [table] (assoc table key
                                                               (conj (get table key) (with-meta handler {:once true}))))))
       (with-queue-critical-section #(swap! *events-handlers*
                                            (fn [table] (assoc table key [(with-meta handler {:once true})])))))
     (.put *main-notification-queue* :listening)))

(defn unlisten
  "Stops listening for events"
  ([key handler]
     (when (get @*events-handlers* key)
       (with-queue-critical-section (swap! *events-handlers*
                                           (fn [table] (let [old-handlers (get table key)
                                                             new-handlers (filter #(not= %1 handler) old-handlers)]
                                                         (assoc table key new-handlers))))))))
