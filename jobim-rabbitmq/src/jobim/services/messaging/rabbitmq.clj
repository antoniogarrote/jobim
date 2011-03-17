(ns jobim.services.messaging.rabbitmq
  (:use [jobim])
  (:use [jobim.definitions]))

(defonce *node-messaging-rabbitmq-znode* "/jobim/messaging/rabbitmq")

;; RabbitMQ implementation of the messaging service
(deftype RabbitMQService [*rabbit-server*] MessagingService

  (publish [this msg]
           (let [node (msg-destiny-node msg)]
             (rabbit/publish *rabbit-server*
                             (node-channel-id @*node-id*)
                             (node-exchange-id node)
                             "msg"
                             (default-encode msg))))

  (set-messages-queue [this queue] (rabbit/make-consumer *rabbit-server*
                                                         (node-channel-id @*node-id*)
                                                         (node-queue-id @*node-id*)
                                                         (fn [msg] (.put queue (default-decode msg))))))

(defmethod make-messaging-service :rabbitmq
  ([kind configuration]
     (let [rabbit-server (apply rabbit/connect [configuration])]
       (rabbit/make-channel rabbit-server (node-channel-id @*node-id*))
       (rabbit/declare-exchange rabbit-server (node-channel-id @*node-id*) (node-exchange-id @*node-id*))
       (rabbit/make-queue rabbit-server (node-channel-id @*node-id*) (node-queue-id @*node-id*) (node-exchange-id @*node-id*) "msg")
       (let [ms (jobim.core.RabbitMQService. rabbit-server)]
         (alter-var-root #'*messaging-service* (fn [_] ms))
         ms))))
