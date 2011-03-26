(ns jobim.services.messaging.rabbitmq
  (:use [jobim])
  (:use [jobim.core :only [msg-destiny-node *node-id*
                           *serialization-service*]])
  (:use [jobim.definitions])
  (:require [lamina.core :as acore]
            [jobim-rabbitmq.core :as rabbit]))

;; this must be extracted from jobim-core
;(defonce *node-messaging-rabbitmq-znode* "/jobim/messaging/rabbitmq")

(defn- node-channel-id
  ([node-id] (str "node-channel-" node-id)))

(defn- node-exchange-id
  ([node-id] (str "node-exchange-" node-id)))

(defn- node-queue-id
  ([node-id] (str "node-queue-" node-id)))

;; RabbitMQ implementation of the messaging service
(deftype RabbitMQService [*rabbit-server*] MessagingService
  (connect-messaging [this] )

  (publish [this msg]
           (let [node (msg-destiny-node msg)]
             (rabbit/publish *rabbit-server*
                             (node-channel-id @*node-id*)
                             (node-exchange-id node)
                             "msg"
                             (encode *serialization-service* msg))))

  (set-messages-queue [this queue] (rabbit/make-consumer *rabbit-server*
                                                         (node-channel-id @*node-id*)
                                                         (node-queue-id @*node-id*)
                                                         (fn [msg] (acore/enqueue queue (decode *serialization-service* msg))))))

(defmethod make-messaging-service :rabbitmq
  ([kind configuration coordination-service serialization-service]
     (let [rabbit-server (apply rabbit/connect [configuration])]
       (rabbit/make-channel rabbit-server (node-channel-id @*node-id*))
       (rabbit/declare-exchange rabbit-server (node-channel-id @*node-id*) (node-exchange-id @*node-id*))
       (rabbit/make-queue rabbit-server (node-channel-id @*node-id*) (node-queue-id @*node-id*) (node-exchange-id @*node-id*) "msg")
       (jobim.services.messaging.rabbitmq.RabbitMQService. rabbit-server))))
