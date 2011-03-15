(ns jobim.services.messaging.tcp
  (:use [jobim.definitions])
  (:use [jobim.core :only [node msg-destiny-node]])
  (:use [jobim.utils :only [hostaddress]])
  (:use [clojure.contrib.logging :only [log]])
  (:require [lamina.core :as acore]
            [aleph.tcp :as atcp]
            [gloss.core :as gloss]))

(defonce *nodes-messaging-tcp-znode* "/jobim/messaging/tcp")

(defn tcp-node
  ([node-name]
     (str *nodes-messaging-tcp-znode* "/" node-name)))

(defn- check-default-znodes
  "Creates the default nodes for the TCP messaging system"
  ([coordination-service node host port]
     (do
       (when (false? (exists? coordination-service *nodes-messaging-tcp-znode*))
         (create-persistent coordination-service *nodes-messaging-tcp-znode*))
       (let [tcp-info-node (tcp-node node)]
         (when (exists? coordination-service tcp-info-node)
           (delete coordination-service tcp-info-node))
         (create coordination-service tcp-info-node (str host ":" port))))))

(defn node-data-to-host-port
  ([node-data]
     (vec (.split node-data ":"))))

(defn send-to-aleph
  ([channel data]
     (let [to-send (vec data)]
       (acore/enqueue channel to-send))))

(deftype TcpMessagingService [queue port coordination-service serialization-service] MessagingService
  (connect-messaging [this]
           (check-default-znodes coordination-service (node) (hostaddress) port))
  (publish [this msg]
           (let [node (msg-destiny-node msg)
                 [host port] (node-data-to-host-port (String. (get-data coordination-service (tcp-node node))))]
             (let [channel (acore/wait-for-result (atcp/tcp-client {:host host
                                                                    :port (if (string? port) (Integer/parseInt port) port)
                                                                    :frame (gloss/repeated :byte)}))]
               (send-to-aleph channel (encode serialization-service msg))
               (acore/close channel))))
  (set-messages-queue [this new-queue]
                      (swap! queue (fn [q] new-queue))))

(defn- server-tcp-handler
  ([queue serialization-service]
     (fn [channel connection-info]
       (acore/receive-all channel
                          (fn [data]
                            (when (not (empty? data))
                              (acore/enqueue @queue (decode serialization-service (into-array Byte/TYPE data)))))))))

(defmethod make-messaging-service :tcp
  ([kind configuration coordination-service serialization-service]
     (let [port (if (string? (:port configuration)) (Integer/parseInt (:port configuration)) (:port configuration))
           queue (atom nil)]
       (atcp/start-tcp-server (server-tcp-handler queue serialization-service) {:port port
                                                                                :frame (gloss/repeated :byte)})
       (TcpMessagingService. queue  port coordination-service serialization-service))))
