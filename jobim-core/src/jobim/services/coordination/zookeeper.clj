(ns jobim.services.coordination.zookeeper
  (:use [jobim.definitions]
        [clojure.contrib.logging :only [log]])
  (:require [jobim.zookeeper :as zk]))

(defonce *node-app-znode* "/jobim")
(defonce *node-nodes-znode* "/jobim/nodes")
(defonce *node-processes-znode* "/jobim/processes")
(defonce *node-messaging-znode* "/jobim/messaging")
(defonce *node-messaging-rabbitmq-znode* "/jobim/messaging/rabbitmq")
(defonce *node-messaging-zeromq-znode* "/jobim/messaging/zeromq")
(defonce *node-names-znode* "/jobim/names")
(defonce *node-links-znode* "/jobim/links")

(defn check-default-znodes
  "Creates the default znodes for the distributed application to run"
  ([] (do
        (when (nil? (zk/exists? *node-app-znode*))
          (zk/create *node-app-znode* "/" {:world [:all]} :persistent))
        (when (nil? (zk/exists? *node-nodes-znode*))
          (zk/create *node-nodes-znode* "/" {:world [:all]} :persistent))
        (when (nil? (zk/exists? *node-names-znode*))
          (zk/create *node-names-znode* "/" {:world [:all]} :persistent))
        (when (nil? (zk/exists? *node-links-znode*))
          (zk/create *node-links-znode* "/" {:world [:all]} :persistent))
        (when (nil? (zk/exists? *node-processes-znode*))
          (zk/create *node-processes-znode* "/" {:world [:all]} :persistent))
        (when (nil? (zk/exists? *node-messaging-znode*))
          (zk/create *node-messaging-znode* "/" {:world [:all]} :persistent))
        (when (nil? (zk/exists? *node-messaging-rabbitmq-znode*))
          (zk/create *node-messaging-rabbitmq-znode* "/" {:world [:all]} :persistent))
        (when (nil? (zk/exists? *node-messaging-zeromq-znode*))
          (zk/create *node-messaging-zeromq-znode* "/" {:world [:all]} :persistent)))))

(deftype ZooKeeperCoordinationService [args] jobim.definitions.CoordinationService
  ;; connection
  (connect-coordination [this]
           (apply zk/connect args)
           (check-default-znodes))
  ;; data nodes
  (exists? [this node-path]
           (if (nil? (zk/exists? node-path))
             false true))
  (delete [this node-path]
          (when (zk/exists? node-path)
            (let [version (:version (second (zk/get-data node-path)))]
              (zk/delete node-path version))))
  (create [this node-path data]
          (zk/create node-path {:world [:all]} :ephemeral)
          (zk/set-data node-path data 0))
  (create-persistent [this node-path]
                     (zk/create node-path {:world [:all]} :persistent))
  (get-data [this node-path]
            (first (zk/get-data node-path)))
  (set-data [this node-path value]
            (let [version (:version (zk/exists? node-path))]
              (zk/set-data node-path value version)))
  ;; Groups
  (join-group [this group-name group-id value]
              (zk/join-group group-name group-id value))
  (watch-group [this group-name callback]
               (zk/watch-group group-name
                               (fn [evt] (try (let [node (first (:member evt))
                                                   kind (:kind evt)]
                                               (callback kind node))
                                             (catch Exception ex
                                               (log :error (str "Error watching nodes "
                                                                (.getMessage ex) " "
                                                                (vec (.getStackTrace ex)))))))))
  (get-children [this group-name]
                (zk/get-children group-name))

  ;; 2-phase commit
  (make-2-phase-commit [this tx-name participants])
  (commit [this tx-name participant]
          (zk/commit tx-name participant))
  (rollback [this tx-name participant]))

(defmethod make-coordination-service :zookeeper
  ([kind configuration]
     (ZooKeeperCoordinationService. configuration)))
