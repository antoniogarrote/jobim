(ns jobim.core
  (:require [jobim.rabbit :as rabbit]
            [jobim.zookeeper :as zk]
            [jobim.utils])
  (:use [jobim.utils]
        [clojure.contrib.logging :only [log]]
        [clojure.contrib.json]))

(defonce *messaging-service* nil)
(defonce *node-app-znode* "/jobim")
(defonce *node-nodes-znode* "/jobim/nodes")
(defonce *node-processes-znode* "/jobim/processes")
(defonce *node-messaging-znode* "/jobim/messaging")
(defonce *node-messaging-rabbitmq-znode* "/jobim/messaging/rabbitmq")
(defonce *node-messaging-zeromq-znode* "/jobim/messaging/zeromq")
(defonce *node-names-znode* "/jobim/names")
(defonce *node-links-znode* "/jobim/links")
(defonce *pid* nil)
(defonce *mbox* nil)
(def *process-table* (ref {}))
(def *evented-table* (ref {}))
(def *process-count* (ref 0))
(def *nodes-table* (ref {}))
(def *rpc-table* (ref {}))
(def *rpc-count* (ref 0))
(def *links-table* (ref {}))
(def *node-id* (ref nil))

;; Messaging layer

(declare pid-to-node-id)
(declare default-encode)

(defn- node-channel-id
  ([node-id] (str "node-channel-" node-id)))

(defn- node-exchange-id
  ([node-id] (str "node-exchange-" node-id)))

(defn- node-queue-id
  ([node-id] (str "node-queue-" node-id)))


(defn msg-destiny-node
  "Returns the node identifier where the message must be sent"
  ([msg]
     (if (nil? (:node msg))
       (if (nil? (:to msg))
         (throw (Exception. "Message without destinatary PID or node"))
         (pid-to-node-id (:to msg)))
       (:node msg))))

;; Generic messaging protocol
(defprotocol MessagingService
  "Abstraction for different messaging systems"
  (publish [this msg]
           "Sends the provided message")
  (set-messages-queue [this queue]
                      "Accepts an instance of a Queue where incoming messages will be stored"))

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
                                                         (fn [msg] (.put queue msg)))))


;; Constructors for the Messaging services

(defmulti make-messaging-service
  "Multimethod used to build the different messaging services"
  (fn [kind configuration] kind))

(defmethod make-messaging-service :rabbitmq
  ([kind configuration]
     (let [rabbit-server (apply rabbit/connect configuration)]
       (rabbit/make-channel rabbit-server (node-channel-id @*node-id*))
       (rabbit/declare-exchange rabbit-server (node-channel-id @*node-id*) (node-exchange-id @*node-id*))
       (rabbit/make-queue rabbit-server (node-channel-id @*node-id*) (node-queue-id @*node-id*) (node-exchange-id @*node-id*) "msg")
       (let [ms (jobim.core.RabbitMQService. rabbit-server)]
         (alter-var-root #'*messaging-service* (fn [_] ms))
         ms))))

;; protocol

(defn protocol-process-msg
  ([from to msg]
     {:type :msg
      :topic :process
      :to   to
      :from from
      :content msg}))

(defn protocol-rpc
  ([node from function args should-return internal-id]
     {:type :msg
      :topic :rpc
      :from from
      :node node
      :content {:function      function
                :args          args
                :should-return should-return
                :internal-id    internal-id}}))

(defn protocol-link-new
  ([from to tx-name]
     {:type :msg
      :topic :link-new
      :from from
      :to   to
      :content {:tx-name tx-name}}))

(defn protocol-link-broken
  ([from to cause]
     {:type  :signal
      :topic :link-broken
      :from from
      :to   to
      :content {:cause cause}}))

(defn protocol-answer
  ([node value internal-id]
     {:type :msg
      :topic :rpc-response
      :node  node
      :content {:value value
                :internal-id internal-id}}))

(defn admin-msg
  ([command args from to]
     {:type  :msg
      :topic :admin
      :from  from
      :to    to
      :content { :command command
                 :args    args}}))



;; utility functions

(defn json-encode
  ([msg]
     (.getBytes (json-str msg))))

(defn json-decode
  ([msg]
     (read-json (if (string? msg) msg (String. msg)))))

(defn java-encode
  ([obj] (let [bos (java.io.ByteArrayOutputStream.)
               oos (java.io.ObjectOutputStream. bos)]
           (.writeObject oos obj)
           (.close oos)
           (.toByteArray bos))))

(defn java-decode
  ([bytes] (let [bis (java.io.ByteArrayInputStream. (if (string? bytes) (.getBytes bytes) bytes))
                  ois (java.io.ObjectInputStream. bis)
                  obj (.readObject ois)]
              (.close ois)
              obj)))

;; Default encode and decode functions

(defonce default-encode java-encode)
(defonce default-decode java-decode)

;; ZooKeeper paths

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

(defn zk-process-path
  ([pid] (str *node-processes-znode* "/" pid)))

(defn zk-link-tx-path
  ([tx-name] (str *node-links-znode* "/" tx-name)))

(defn zk-zeromq-node
  ([node] (str *node-messaging-zeromq-znode* "/" node)))

(defn add-link
  ([tx-name self-pid remote-pid]
     (dosync (alter *links-table* (fn [table]
                                    (let [old-list (get table self-pid)
                                          old-list (if (nil? old-list) [] old-list)]
                                      (assoc table self-pid (conj old-list remote-pid))))))))

(defn remove-link
  ([self-pid remote-pid]
     (dosync (alter *links-table* (fn [table]
                                    (let [old-list (get table self-pid)]
                                      (if (nil? old-list) table
                                          (let [new-list (filter #(not (= %1 remote-pid)) old-list)]
                                            (assoc table self-pid new-list)))))))))

(defn remove-links
  ([self-pid]
     (dosync (alter *links-table* (fn [table]
                                    (let [old-list (get table self-pid)]
                                      (if (nil? old-list) table
                                          (dissoc table self-pid))))))))

;; core functions for nodes

(declare pid-to-node-id)
(defn process-rpc
  "Process an incoming RPC request"
  ([msg]
     (let [from (get msg :from)
           content (get msg :content)
           function (get content :function)
           args     (get content :args)
           should-return (get content :should-return)
           internal-id (get content :internal-id)]
       (try
        (let [f (eval-fn function)
              result (apply f args)]
          (when should-return
            (let [node (pid-to-node-id from)
                  resp (protocol-answer node result internal-id)]
              (publish *messaging-service* resp))))
        (catch Exception ex
          (do
            (log :error (str "Error invoking RPC call" (.getMessage ex) " " (vec (.getStackTrace ex))))))))))


(declare exists-pid?)
(defn handle-link-request
  ([msg] (let [from (:from msg)
               to (:to msg)
               tx-name (:tx-name (:content msg))]
           (when (exists-pid? to)
             ;; @todo add timeout here
             (let [result (zk/commit (zk-link-tx-path tx-name) to)]
               (when (= result "commit")
                 (add-link tx-name to from)))))))

(declare pid-to-mbox)
(declare rpc-id-to-promise)
(declare remove-rpc-promise)
(defn dispatch-signal
  ([msg]
     (condp = (keyword (:topic msg))
       :link-broken (if-let [mbox (pid-to-mbox (:to msg))]
                      (do
                        (remove-link (:to msg) (:from msg))
                        (.put mbox {:signal :link-broken
                                    :from (:from msg)
                                    :cause (:cause (:content msg))}))))))

(declare pid-to-process-number)
(declare clean-process)
(declare notify-links)
(defn reduce-evented-proces
  ([msg]
     (let [pid (:to msg)
           content (:content msg)]
       (try
        (binding [*pid* pid
                  *mbox* (pid-to-mbox pid)]
          (let [evented-actor (get @*evented-table* (pid-to-process-number pid))
                r-loop (:loop evented-actor)
                cont (:continuation evented-actor)
                next-desc (apply cont [content])]
            ;; react -> evented
            (if (:evented (meta next-desc))
              ;; react -> loop
              (if (:loop next-desc)
                ;; exception!
                ;; @todo This should be supported
                (throw (Exception. "unsupported nested loop"))
                ;; react -> react : queue
                (dosync (alter *evented-table* (fn [table] (assoc table (pid-to-process-number pid) {:continuation (:react next-desc) :loop r-loop})))))
              ;; react -> not evented
              (if (nil? r-loop)
                ;; no loop finished
                (clean-process pid)
                ;; we hve a loop -> recur
                (let [next-desc (apply r-loop [])]
                  ;; react -> loop -> evented?
                  (if (:evented (meta next-desc))
                    ;; react -> loop -> evented
                    (if (:loop next-desc)
                      ;; react -> loop -> loop : exception !
                      (throw (Exception. "unsupported nested loop"))
                      ;; react -> loop -> react : enqueue
                      (dosync (alter *evented-table* (fn [table] (assoc table (pid-to-process-number pid) {:continuation (:react next-desc) :loop r-loop})))))
                    ;; react -> loop -> not evented : finished
                    (clean-process pid)))))))
        (catch Exception ex
          (log :error (str "*** process " pid " died with message : " (.getMessage ex) " " (vec (.getStackTrace ex))))
          (notify-links pid (str (class ex) ":" (.getMessage ex)))
          (remove-links pid)
          (clean-process pid))))))

(declare evented-process?)
(defn dispatch-msg
  ([msg]
     (condp = (keyword (:topic msg))
       :process (if-let [mbox (pid-to-mbox (:to msg))]
                  (if (evented-process? (:to msg))
                    (future (reduce-evented-proces msg))
                    (.put mbox (:content msg))))
       :link-new (future (handle-link-request msg))
       :rpc     (future (process-rpc msg))
       :rpc-response (future (try
                              (let [prom (rpc-id-to-promise (:internal-id (:content msg)))
                                    val (:value (:content msg))]
                                (deliver prom val)
                                (remove-rpc-promise (:internal-id (:content msg))))
                              (catch Exception ex (log :error (str "*** Error processing rpc-response: " (.getMessage ex)))))))))

(defn node-dispatcher-thread
  "The main thread receiving events and messages"
  ([name id queue]
     (loop [should-continue true
            queue queue]
       (let [msg (.take queue)]
         (try
          (do
            (let [msg (default-decode msg)]
              (condp = (keyword (:type msg))
                :msg (dispatch-msg msg)
                :signal (dispatch-signal msg)
                (log :error (str "*** " name " , " (java.util.Date.) " uknown message type for : " msg)))))
          (catch Exception ex (log :error (str "***  " name " , " (java.util.Date.) " error processing message : " msg " --> " (.getMessage ex)))))
         (recur true queue)))))


(declare bootstrap-node)
(defn- bootstrap-from-file
  ([file-path]
     (let [config (eval (read-string (slurp file-path)))]
       (bootstrap-node (:node-name config) (:rabbit-options config) (:zookeeper-options config)))))

(declare purge-links)
(declare resolve-node-name)
(declare nodes)
(defn bootstrap-node
  "Adds a new node to the distributed application"
  ([file-path] (bootstrap-from-file file-path))
  ([name rabbit-args zookeeper-args]
     (let [id (random-uuid)]
       ;; store node configuration
       (dosync (alter *node-id* (fn [_] id)))
       ;; initializing the messaging service
       (make-messaging-service :rabbitmq rabbit-args)
       ;; connecting to ZooKeeper
       (apply zk/connect zookeeper-args)
       ;; check application standard znodes
       (check-default-znodes)
       ;; connect messages
       (let [dispatcher-queue (java.util.concurrent.LinkedBlockingQueue.)]
         (set-messages-queue *messaging-service* dispatcher-queue)
         (dosync (alter *nodes-table* (fn [_] (nodes))))
         (zk/watch-group *node-nodes-znode* (fn [evt] (try (let [node (first (:members evt))]
                                                             (if (= (:kind evt) :member-left)
                                                               (let [node-id (get @*nodes-table* node)]
                                                                 (when (not (nil? node-id))
                                                                   (dosync (alter *nodes-table* (fn [table] (dissoc table node))))
                                                                   (future (purge-links node-id))))
                                                               (future (dosync (alter *nodes-table* (fn [table] (assoc table node (resolve-node-name node))))))))
                                                           (catch Exception ex (log :error (str  "Error watching nodes" (.getMessage ex) " " (vec (.getStackTrace ex))))))))
         ;; starting main thread
         (.start (Thread. (fn [] (node-dispatcher-thread name id  dispatcher-queue))))
         ;; register zookeeper group
         (zk/join-group *node-nodes-znode* name id)))))

;; library functions

(defn nodes
  "Returns all the available nodes and their identifiers"
  ([] (let [children (zk/get-children *node-nodes-znode*)]
        (reduce (fn [m c] (let [[data stats] (zk/get-data (str *node-nodes-znode* "/" c))]
                            (assoc m c (String. data))))
                {}
                children))))

(defn- next-process-id
  ([] (let [rpid (dosync (alter *process-count* (fn [old] (inc old))))
            lpid (deref *node-id*)]
        (str lpid "." rpid))))

(defn- next-rpc-id
  ([] (let [rpc (dosync (alter *rpc-count* (fn [old] (inc old))))]
        rpc)))

(defn pid-to-process-number
  ([pid] (last (vec (.split pid "\\.")))))

(defn pid-to-node-id
  ([pid] (first (vec (.split pid "\\.")))))

(defn evented-process?
  ([pid]
     (not (nil? (get @*evented-table* (pid-to-process-number pid))))))


(defn- register-local-mailbox
  ([pid]
     (let [process-number (pid-to-process-number pid)
           q (java.util.concurrent.LinkedBlockingQueue.)]
       (dosync (alter *process-table* (fn [table] (assoc table process-number {:mbox q
                                                                               :dictionary {}}))))
       q)))

(defn- register-rpc-promise
  ([rpc-id]
     (let [p (promise)]
       (dosync (alter *rpc-table* (fn [table] (assoc table rpc-id p))))
       p)))

(defn- rpc-id-to-promise
  ([rpc-id]
     (get @*rpc-table* rpc-id)))

(defn- remove-rpc-promise
  ([rpc-id]
     (dosync (alter *rpc-table* (fn [table] (dissoc table rpc-id))))))

(defn- exists-pid?
  ([pid] (not (nil? (get @*process-table* (pid-to-process-number pid))))))

(defn pid-to-mbox
  ([pid] (let [rpid (pid-to-process-number pid)]
           (:mbox (get @*process-table* rpid)))))

(defn dictionary-write
  ([pid key value]
     (let [process-number (pid-to-process-number pid)
           dictionary (:dictionary  (get @*process-table* process-number))
           mbox (pid-to-mbox pid)]
       (dosync (alter *process-table* (fn [table] (assoc table process-number {:mbox mbox
                                                                               :dictionary (assoc dictionary key value)})))))))
(defn dictionary-get
  ([pid key]
     (let [process-number (pid-to-process-number pid)
           dictionary (:dictionary (get @*process-table* process-number))]
       (get dictionary key))))

(defn clean-process
  ([pid]
     (let [version (:version (second (zk/get-data (zk-process-path pid))))
           registered (dictionary-get pid :registered-name)]
       (zk/delete (zk-process-path pid) version)
       (when (not (nil? registered))
         (let [version (:version (second (zk/get-data (str *node-names-znode* "/" registered))))]
           (zk/delete (str *node-names-znode* "/" registered) version)))
       (when (evented-process? pid)
         (dosync (alter *process-table* (fn [table] (dissoc table (pid-to-process-number pid))))))
       (dosync (alter *process-table* (fn [table] (dissoc table (pid-to-process-number pid))))))))

(declare send!)
(defn notify-links
  ([pid cause]
     (if-let [pids (get @*links-table* pid)]
       (doseq [linked pids]
         (send! linked (protocol-link-broken pid linked cause))))))

(defn purge-links
  ([node]
     (try
     (let [pids (keys @*links-table*)]
       (doseq [pid pids]
         (let [linked (get @*links-table* pid)
               broken-links (filter #(= node (pid-to-node-id %1)) linked)]
           (when (> (count broken-links) 0)
             (doseq [broken-pid broken-links]
               (send! pid (protocol-link-broken broken-pid pid "node down")))))))
     (catch Exception ex
       (log :error (str "*** Exception " (.getMessage ex)))))))


(declare self)
(defn remote-send
  ([node pid msg]
     (if (zk/exists? (zk-process-path pid))
       (let [msg (if (and (= (map? msg))
                          (= :signal (keyword (:type msg))))
                   msg
                   (protocol-process-msg (self) pid msg))]
         (publish *messaging-service* msg))
       (throw (Exception. (str "Non existent remote process " pid))))))

(defn admin-send
  ([node pid msg]
     (if (zk/exists? (zk-process-path pid))
       (do
         (publish *messaging-service* msg))
       (throw (Exception. (str "Non existent remote process " pid))))))

(defn spawn
  "Creates a new local process"
  ([]
     (let [pid (next-process-id)
           queue (register-local-mailbox pid)]
       (zk/create (zk-process-path pid) {:world [:all]} :ephemeral)
       (zk/set-data (zk-process-path pid) "running" 0)
       pid))
  ([f]
     (let [pid (spawn)
           mbox (pid-to-mbox pid)
           f (if (string? f) (eval-fn f) f)]
       (future
        (binding [*pid* pid
                  *mbox* mbox]
          (try
           (let [result (f)]
             (clean-process *pid*))
           (catch Exception ex
             (log :error (str "*** process " pid " died with message : " (.getMessage ex) " " (vec (.getStackTrace ex))))
             (notify-links *pid* (str (class ex) ":" (.getMessage ex)))
             (remove-links *pid*)
             (clean-process *pid*)))))
         pid)))

(defn spawn-in-repl
  "Creates a new process attached to the running shell"
  ([] (let [pid (spawn)]
        (alter-var-root #'*pid* (fn [_] pid))
        (alter-var-root #'*mbox* (fn [_] (pid-to-mbox pid)))
        pid)))

(defn self
  "Returns the pid of the current process"
  ([] *pid*))

(defn send!
  "Sends a message to a local/remote process"
  ([pid msg]
     (let [node (pid-to-node-id pid)]
       (if (= node @*node-id*)
         (if (and (= (map? msg))
                  (= :signal (keyword (:type msg))))
           (dispatch-signal msg)
           (if (evented-process? pid)
             (do (future (reduce-evented-proces (protocol-process-msg (self) pid msg)))
                 :ok)
             (let [mbox (pid-to-mbox pid)]
               (.put mbox msg)
               :ok)))
         (remote-send node pid msg)))))

(defn receive
  "Blocks until a new message has been received"
  ([] (let [msg (.take *mbox*)]
        msg)))

(defn register-name
  "Associates a name that can be retrieved from any node to a PID"
  [name pid]
  (if (zk/exists? (str *node-names-znode* "/" name))
    (throw (Exception. (str "Already existent remote process " pid)))
    (do (zk/create (str *node-names-znode* "/" name) pid {:world [:all]} :ephemeral)
        (dictionary-write pid :registered-name name)
        :ok)))

(defn registered-names
  "The list of globally registered names"
  ([]
     (let [children (zk/get-children *node-names-znode*)]
       (reduce (fn [m c] (let [[data stats] (zk/get-data (str *node-names-znode* "/" c))]
                           (assoc m c (String. data))))
               {}
               children))))

(defn resolve-name
  "Wraps a globally registered name"
  ([name]
     (if (zk/exists? (str *node-names-znode* "/" name))
       (String. (first (zk/get-data (str *node-names-znode* "/" name)))))))

(defn resolve-node-name
  "Returns the identifier for a provided node name"
  ([node-name]
     (let [stat (zk/exists? (str *node-nodes-znode* "/" node-name))]
       (if stat
         (let [[data stats] (zk/get-data (str *node-nodes-znode* "/" node-name))]
           (String. data))
         (throw (Exception. (str "Unknown node " node-name)))))))

(defn rpc-call
  "Executes a non blocking RPC call"
  ([node function args]
     (let [msg (protocol-rpc node (self) function args false 0)]
       (publish *messaging-service* msg))))

(defn rpc-blocking-call
  "Executes a blocking RPC call"
  ([node function args]
     (let [rpc-id (next-rpc-id)
           prom (register-rpc-promise rpc-id)
           msg (protocol-rpc node (self) function args true rpc-id)]
       (publish *messaging-service* msg)
       @prom)))

(defn link
  "Links this process with the process identified by the provided PID. Links are bidirectional"
  ([pid]
     (let [pid-number  (apply + (map #(int %1) (vec pid)))
           self-number (apply + (map #(int %1) (vec (self))))
           tx-name (if (< pid-number self-number) (str pid "-" (self)) (str (self) "-" pid))
           ;; we create a transaction for committing on the link
           _2pc (zk/make-2-phase-commit (zk-link-tx-path tx-name) [pid (self)])]
       ;; we send the link request
       (admin-send (pid-to-node-id pid) pid (protocol-link-new (self) pid tx-name))
       ;; we commit and wait for a result
       ;; @todo add timeout!
       (let [result (zk/commit (zk-link-tx-path tx-name) (self))]
         (if (= result "commit")
           (add-link tx-name (self) pid)
           (throw (Exception. (str "Error linking processes " (self) " - " pid))))))))

(defn react
  ([f]
     (with-meta {:react f} {:evented true})))

(defn spawn-evented
  ([f]
     (let [pid (spawn)
           mbox (pid-to-mbox pid)]
       (try
        (let [desc (if (string? f) (apply (eval-fn f) [])
                       (if (:evented (meta f))
                         f
                         (apply f [])))]
          (if (nil? (:evented (meta desc)))
            ;; we run the function and it returns something that is not evented
            ;; we rturn :finished since execution has finished
            (do
              (clean-process pid)
              :finished)
            ;; we have some evented functionality, it can be a loop or a react call
            (let [r-loop (:loop desc)] ;; we check if we need to store the loop function
              ;; If this is a loop we run the loop
              (loop [c (if (:loop desc) (apply (:loop desc) []) desc)]
                (if (:evented (meta c))
                  (if (:loop c)
                    ;; infinite loop
                    (do (clean-process pid)
                        (throw (Exception. "Unsupported nested loop")))
                    ;; we have a queued event
                    (dosync (alter *evented-table* (fn [table] (assoc table (pid-to-process-number pid) {:continuation (:react c) :loop r-loop})))
                            pid))
                  ;; execution has finished
                  (do
                    (clean-process pid)
                    :finished))))))
        (catch Exception ex
          (log :error (str "*** Exception executing evented actor " pid " : " (.getMessage ex)))
          (.printStackTrace ex)
          :exception)))))
