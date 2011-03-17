(ns jobim.services.messaging.zeromq
  (:use [jobim.core])
  (:use [jobim.definitions]))

(defonce *node-messaging-zeromq-znode* "/jobim/messaging/zeromq")

;; ZeroMQ implementation of the messaging service
;;(declare zk-zeromq-node)

(defn node-to-zeromq-socket
  ([node node-map]
     (if (nil? (get @node-map node))
       (let [result (zk/get-data (zk-zeromq-node node))]
         (if (nil? result)
           (throw (Exception. (str "The node " node " is not registered as a ZeroMQ node")))
           (let [ctx (zmq/make-context 10)
                 socket (zmq/make-socket ctx zmq/+downstream+)
                 protocol-string (String. (first result))]
             (zmq/connect socket protocol-string)
             (log :debug (str "*** created downstream socket: " socket " protocol: " protocol-string))
             (let [ag-socket (agent socket)]
             (swap! node-map (fn [table] (assoc table node ag-socket))))
             ag-socket))))
       (get @node-map node))))

(deftype ZeroMQService [*zeromq*] MessagingService
  (publish [this msg]
           (let [node (msg-destiny-node msg)
                 socket (node-to-zeromq-socket node (:node-map *zeromq*))]
             (log :debug (str "*** publishing to socket " @socket " and node " node " msg " msg))
             (let [result (do (send-off socket (fn [sock] (do (zmq/send- sock (default-encode msg)) sock))))]
               (await socket)
               (log :debug (str "*** publish result: " result))
               :ok)))
  (set-messages-queue [this queue]
                      (future (loop [msg (zmq/recv (:socket *zeromq*))]
                                (log :debug (str "*** retrieved msg " (default-decode msg)))
                                (.put queue (default-decode msg))
                                (recur (zmq/recv (:socket *zeromq*)))))))


Constructors for the Messaging services


(defonce *zmq-default-io-threads* (num-processors))

(defmethod make-messaging-service :zeromq
  ([kind configuration]
     ;; we delete the old node if it exists
     (when (zk/exists? (zk-zeromq-node @*node-id*))
       (let [version (:version (second (zk/get-data (zk-zeromq-node @*node-id*))))]
         (zk/delete (zk-zeromq-node @*node-id* version))))
     ;; we register the new node
     (zk/create (zk-zeromq-node @*node-id*) (:protocol-and-port configuration) {:world [:all]} :ephemeral)
     ;; we establish the connection
     (let [ctx (zmq/make-context 10)
           socket (zmq/make-socket ctx zmq/+upstream+)
           downstream-socket (zmq/make-socket ctx zmq/+downstream+)
           _ (zmq/connect downstream-socket (:protocol-and-port configuration))
           ms (jobim.core.ZeroMQService. {:context ctx :socket socket :node-map (ref {@*node-id* (agent downstream-socket)})
                                          :io-threads (or (:io-threads configuration) *zmq-default-io-threads*)})]
       (zmq/bind socket (:protocol-and-port configuration))
       (alter-var-root #'*messaging-service* (fn [_] ms))
       ms)))
