(ns jobim.behaviours.tcp
  (:use [jobim]
        [jobim.utils]
        [clojure.contrib.logging :only [log]]
        [matchure])
  (:require [lamina.core :as acore]
            [aleph.tcp :as atcp]
            [gloss.core :as gloss]))


;; Utility functions

(defn send-to-aleph-bytes
  ([channel data]
     (let [to-send (vec data)]
       (acore/enqueue channel to-send))))

(defn send-to-aleph-string
  ([channel data]
     (acore/enqueue channel data)))



(defprotocol TCP
  (init-server [this port opts] "Inits a new TCP server socket actor")
  (init-client [this host port opts] "Inits a new TCP client socket actor")
  (send-tcp-inner! [this counter msg] "Sends an array of bytes through the action")
  (set-active [this boolean] "Sets the active option of this TCP actor")
  (set-controlling-actor [this pid] "Sets the PID of the active actor for this actor")
  (receive-tcp [this] "Blocks waiting for the next message if the TCP actor is not active"))


;; TCP Server impl.

(defn server-tcp-handler
  ([state controlling-pid this]
     (let [connection-counter (ref 0)]
       (fn [channel connection-info]
         (log :debug (str "*** server connection info: " connection-info))
         (let [old-channels (:channels @state)
               old-counter @connection-counter]
           (dosync (swap! state (fn [old-state] (assoc old-state :channels
                                                       (assoc old-channels @connection-counter channel))))
                   (alter connection-counter (fn [old] (inc old))))
           (try
             (acore/receive-all channel
                               (fn [data]
                                 (let [msg [:tcp {:topic :tcp-data
                                                  :counter old-counter
                                                  :dst this}
                                            (condp = (:frame-kind @state)
                                               :string  data
                                              (into-array Byte/TYPE data))]]
                                   (if (:active @state)
                                     (send! (:pid @state) msg)
                                     (.put (:queue @state) msg)))))
            (catch Exception ex [:tcp :close (.getMessage ex)])))))))

(deftype TCPServerImpl [state] jobim.behaviours.tcp.TCP
  (init-server [this port opts]
               (let [active (or (:active opts) true)
                     frame-kind (or (:frame opts) :string)
                     frame (condp = frame-kind
                               :bytes (gloss/repeated :byte)
                               :string (gloss/string :utf-8)
                               frame-kind)]
                 (swap! state (fn [old-state] (-> old-state
                                                 (assoc :active active)
                                                 (assoc :pid (self))
                                                 (assoc :frame-kind frame-kind))))
                 (atcp/start-tcp-server (server-tcp-handler state (:pid @state) this)
                                        {:port port
                                         :frame frame})))
  (init-client [this host port opts]
               (throw (Exception. "Impossible to init a client from a TCP server implementation")))
  (send-tcp-inner! [this counter msg](try (condp = (:frame-kind @state)
                                              :string (send-to-aleph-string (get (:channels @state) counter) msg)
                                              (send-to-aleph-bytes (get (:channels @state) counter) msg))
                                          (catch Exception ex (do (log :error (str "ERROR!!" (.getMessage ex)))
                                                                  (let [old-chns (:channels @state)]
                                                                    (swap! state (fn [old-state]
                                                                                   (assoc old-state :channels
                                                                                          (dissoc old-chns counter)))))))))
  (set-active [this boolean] (swap! state (fn [old-state] (assoc old-state :active boolean))))
  (set-controlling-actor [this pid] (swap! state (fn [old-state] (assoc old-state :pid pid))))
  (receive-tcp [this] (if (:active @state)
                        (throw (Exception. "The TCP actor is configured into active mode, no passive receive can be invoked"))
                        (.take (:queue @state)))))

;; TCP Client impl.

(defn client-tcp-handler
  ([state]
     (fn [data]
       (let [msg (condp = (:frame-kind @state)
                     :string data
                     (into-array Byte/TYPE data))]
         (log :debug (str "*** received data in the client " msg))
         (if (:active @state)
           (send! (:pid @state) [:tcp (:channel @state) msg])
           (.put (:queue @state) [:tcp (:channel @state) msg]))))))


(deftype TCPClientImpl [state] jobim.behaviours.tcp.TCP
  (init-server [this port opts] (throw (Exception. "Impossible to init a server from a TCP client implementation")))
  (init-client [this host port opts]
               (let [active (or (:active opts) true)
                     frame-kind (or (:frame opts) :string)
                     frame (condp = frame-kind
                               :bytes (gloss/repeated :byte)
                               :string (gloss/string :utf-8)
                               frame-kind)]
                 (swap! state (fn [old-state] (-> old-state
                                                 (assoc :active active)
                                                 (assoc :pid (self))
                                                 (assoc :frame-kind frame-kind)
                                                 (assoc :channel (acore/wait-for-result (atcp/tcp-client
                                                                                         {:host host :port port
                                                                                          :frame frame}))))))
                 (acore/receive-all (:channel @state) (client-tcp-handler state))))
  (send-tcp-inner! [this counter msg] (if (= (:frame-kind @state) :bytes)
                                        (send-to-aleph-bytes (:channel @state) msg)
                                        (send-to-aleph-string (:channel @state) msg)))
  (set-active [this boolean] (swap! state (fn [old-state] (assoc old-state :active boolean))))
  (set-controlling-actor [this pid] (swap! state (fn [old-state] (assoc old-state :pid pid))))
  (receive-tcp [this] (if (:active @state)
                        (throw (Exception. "The TCP actor is configured into active mode, no passive receive can be invoked"))
                        (.take (:queue @state)))))


(defn send-tcp!
  ([dst data]
     (if (and (map? dst) (= (:topic dst) :tcp-data))
       (send-tcp-inner! (:dst dst) (:counter dst) data)
       (send-tcp-inner! dst 0 data))))

;; utility functions

(defn start-client
  ([host port & opts]
     (let [client (jobim.behaviours.tcp.TCPClientImpl. (atom {:active true
                                                              :queue (java.util.concurrent.LinkedBlockingQueue.)
                                                              :pid (self)}))]
       (init-client client host port (first opts)) client)))

(defn start-server
  ([port & opts]
     (let [server (jobim.behaviours.tcp.TCPServerImpl.
                   (atom {:active true
                          :queue (java.util.concurrent.LinkedBlockingQueue.)
                          :pid (self)}))]
       (init-server server port (first opts)) server)))
