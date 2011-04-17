(ns jobim.core
  (:require [jobim.events :as jevts])
  (:require [lamina.core :as lam])
  (:use [jobim.utils]
        [jobim.definitions]
        [clojure.contrib.logging :only [log]])
  ;(:import [java.util.concurrent LinkedBlockingQueue])
  (:import [java.util LinkedList]))

(defonce *messaging-service* nil)
(defonce *coordination-service* nil)
(defonce *serialization-service* nil)

(defonce *pid* nil)
(defonce *mbox* nil)
(defonce *mbox-data* nil)
(def *process-table* (atom {}))
(def *evented-table* (atom {}))
(def *process-count* (atom 0))
(def *nodes-table* (atom {}))
(def *rpc-table* (atom {}))
(def *rpc-count* (atom 0))
(def *links-table* (atom {}))
(def *node-id* (atom nil))
(def *local-name-register* (atom {}))

;; Coordination paths

(defonce *node-app-znode* "/jobim")
(defonce *node-nodes-znode* "/jobim/nodes")
(defonce *node-processes-znode* "/jobim/processes")
(defonce *node-messaging-znode* "/jobim/messaging")
(defonce *node-names-znode* "/jobim/names")
(defonce *node-links-znode* "/jobim/links")

(defn zk-link-tx-path
  "Transforms a tx-name into a ZK node path"
  ([tx-name] (str *node-links-znode* "/" tx-name)))


;; Messaging layer

(declare pid-to-node-id)

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

;; ;; ZooKeeper paths

(defn zk-process-path
  ([pid] (str *node-processes-znode* "/" pid)))

;; (defn zk-zeromq-node
;;   ([node] (str *node-messaging-zeromq-znode* "/" node)))

(defn add-link
  ([tx-name self-pid remote-pid]
     (swap! *links-table* (fn [table]
                                    (let [old-list (get table self-pid)
                                          old-list (if (nil? old-list) [] old-list)]
                                      (assoc table self-pid (conj old-list remote-pid)))))))

(defn remove-link
  ([self-pid remote-pid]
     (swap! *links-table* (fn [table]
                                    (let [old-list (get table self-pid)]
                                      (if (nil? old-list) table
                                          (let [new-list (filter #(not (= %1 remote-pid)) old-list)]
                                            (assoc table self-pid new-list))))))))

(defn remove-links
  ([self-pid]
     (swap! *links-table* (fn [table]
                                    (let [old-list (get table self-pid)]
                                      (if (nil? old-list) table
                                          (dissoc table self-pid)))))))

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
        (let [f (eval-ns-fn function)
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
             (let [result (commit *coordination-service* (zk-link-tx-path tx-name) to)]
               (when (= result "commit")
                 (add-link tx-name to from)))))))

(declare pid-to-mbox)
(declare pid-to-mbox-data)
(declare rpc-id-to-promise)
(declare remove-rpc-promise)
(declare critical-message-dispatch)
(declare evented-process?)
(declare send-to-evented)
(defn dispatch-signal
  ([msg]
     (condp = (keyword (:topic msg))
         :link-broken (let [signal-msg {:signal :link-broken
                                        :from (:from msg)
                                        :cause (:cause (:content msg))}]
                        (if (evented-process? (:to msg))
                          (do
                            (remove-link (:to msg) (:from msg))
                            (send-to-evented (:to msg)
                                             signal-msg))
                          (if-let [mbox (pid-to-mbox (:to msg))]
                            (do
                              (remove-link (:to msg) (:from msg))
                              (critical-message-dispatch (:to msg)
                                                         signal-msg))))))))

(defn dispatch-msg
  ([msg]
     (condp = (keyword (:topic msg))
       :process  (if (evented-process? (:to msg))
                   (send-to-evented (:to msg) msg)
                   (if-let [mbox (pid-to-mbox (:to msg))]
                     (critical-message-dispatch (:to msg) (:content msg))))
       :link-new (future (handle-link-request msg))
       :rpc     (future (process-rpc msg))
       :rpc-response (future (try
                               (let [prom (rpc-id-to-promise (:internal-id (:content msg)))
                                     val (:value (:content msg))]
                                 (when (not (nil? prom))
                                   (do
                                     (deliver prom val)
                                     (remove-rpc-promise (:internal-id (:content msg))))))
                              (catch Exception ex (log :error (str "*** Error processing rpc-response: " (.getMessage ex)))))))))

(defn node-dispatcher-thread
  "The main thread receiving events and messages"
  ([name id queue]
     (lam/receive-all queue
                      (fn [msg]
                        (try
                          (do
                            (condp = (keyword (:type msg))
                                :msg (dispatch-msg msg)
                                :signal (dispatch-signal msg)
                                (log :error (str "*** " name " , " (java.util.Date.) " uknown message type for : " msg))))
                          (catch Exception ex (log :error (str "***  " name " , " (java.util.Date.) " error processing message : " msg " --> " (.getMessage ex)))))))))

(declare bootstrap-node)
(defn- bootstrap-from-file
  ([file-path]
     (let [config (eval (read-string (slurp file-path)))]
       (bootstrap-node (:node-name config)
                       (:coordination-type config) (:coordination-args config)
                       (:messaging-type config) (:messaging-args config)
                       (:serialization-type config) (:serialization-args config)))))

(defn print-node-info
  "Prints the configuration information for a node"
  ([name id
    coordination-type coordination-args
    messaging-type messaging-args
    serialization-type serialization-args]
     (println (str "\n ** Jobim node started ** \n\n"
                   "\n - node-name: " name
                   "\n - id: " id
                   "\n - messaging-service: " messaging-type
                   "\n - messaging-args: " messaging-args
                   "\n - coordination-service: " coordination-type
                   "\n - coordination-args: " coordination-args
                   "\n - serialization-service: " serialization-type
                   "\n - serialization-args: " serialization-args
                   "\n\n"))))

(declare purge-links)
(declare resolve-node-name)
(declare nodes)
(defn bootstrap-node
  "Adds a new node to the distributed application"
  ([file-path] (bootstrap-from-file file-path))
  ([name coordination-type coordination-args messaging-type messaging-args serialization-type serialization-args]
     (let [id (random-uuid)
           name (clojure.core/name name)]
       ;; Print node information
       (print-node-info name id
                        coordination-type coordination-args
                        messaging-type messaging-args
                        serialization-type serialization-args)
       ;; store node configuration
       (swap! *node-id* (fn [_] id))
       ;; Launch evented processing of events
       (jevts/run-multiplexer 1 ;;(num-processors)
                              )
       ;; binding of core services
       (alter-var-root #'*serialization-service* (fn [_] (make-serialization-service serialization-type serialization-args)))
       (alter-var-root #'*coordination-service* (fn [_] (make-coordination-service coordination-type coordination-args)))
       (alter-var-root #'*messaging-service* (fn [_] (make-messaging-service messaging-type messaging-args *coordination-service* *serialization-service*)))

       ;; initialization of core services
       (connect-coordination *coordination-service*)
       (connect-messaging *messaging-service*)

       ;; connect messages
       (let [dispatcher-queue (lam/channel)]
         (set-messages-queue *messaging-service* dispatcher-queue)
         (swap! *nodes-table* (fn [_] (nodes)))
         (watch-group *coordination-service* *node-nodes-znode*
                      (fn [kind node] (try
                                       (if (= kind :member-left)
                                         (let [node-id (get @*nodes-table* node)]
                                           (when (not (nil? node-id))
                                             (swap! *nodes-table* (fn [table] (dissoc table node)))
                                             (future (purge-links node-id))))
                                         (future (swap! *nodes-table* (fn [table] (assoc table node (resolve-node-name node))))))
                                       (catch Exception ex (log :error (str  "Error watching nodes" (.getMessage ex) " " (vec (.getStackTrace ex))))))))
         ;; starting main thread
         (node-dispatcher-thread name id  dispatcher-queue)
         ;; register zookeeper group
         (join-group *coordination-service* *node-nodes-znode* name id))
       :ok)))

;; ;; library functions

(defn node
  "Returns the identifier of the current node"
  ([] @*node-id*))

(defn nodes
  "Returns all the available nodes and their identifiers"
  ([] (let [children (get-children *coordination-service* *node-nodes-znode*)]
        (reduce (fn [m c] (let [data (get-data *coordination-service* (str *node-nodes-znode* "/" c))]
                           (assoc m c (String. data))))
                {}
                children))))

(defn- next-process-id
  ([] (let [rpid (swap! *process-count* (fn [old] (inc old)))
            lpid (deref *node-id*)]
        (str lpid "." rpid))))

(defn- next-rpc-id
  ([] (let [rpc (swap! *rpc-count* (fn [old] (inc old)))]
        rpc)))

(defn pid-to-process-number
  ([^String pid] (last (vec (.split pid "\\.")))))

(defn pid-to-node-id
  ([^String pid] (first (vec (.split pid "\\.")))))

(defn evented-process?
  ([pid]
     (not (nil? (get @*evented-table* (pid-to-process-number pid))))))


(defn- register-local-mailbox
  ([pid is-evented]
     (let [process-number (pid-to-process-number pid)
           q (if (not is-evented) (lam/channel) nil)
           s (LinkedList.)
           mbox-data {:save-queue s}]
       (swap! *process-table* (fn [table] (assoc table process-number {:mbox q
                                                                      :dictionary {}
                                                                      :mbox-data mbox-data})))
       q)))

(defn- register-rpc-promise
  ([rpc-id]
     (let [p (promise)]
       (swap! *rpc-table* (fn [table] (assoc table rpc-id p)))
       p)))

(defn- rpc-id-to-promise
  ([rpc-id]
     (get @*rpc-table* rpc-id)))

(defn- remove-rpc-promise
  ([rpc-id]
     (swap! *rpc-table* (fn [table] (dissoc table rpc-id)))))

(defn- exists-pid?
  ([pid] (not (nil? (get @*process-table* (pid-to-process-number pid))))))

(defn pid-to-mbox
  ([pid] (let [rpid (pid-to-process-number pid)]
           (:mbox (get @*process-table* rpid)))))

(defn pid-to-mbox-data
  ([pid] (let [rpid (pid-to-process-number pid)]
           (:mbox-data (get @*process-table* rpid)))))

(defn dictionary-write
  ([pid key value]
     (let []
       
       (swap! *process-table* (fn [table] (let [process-number (pid-to-process-number pid)
                                               ptable (get table process-number)
                                               dictionary (:dictionary  ptable)
                                               dictionary (assoc dictionary key value)
                                               ptable (assoc ptable :dictionary dictionary)]
                                           (assoc table process-number ptable)))))))
(defn dictionary-get
  ([pid key]
     (let [process-number (pid-to-process-number pid)
           dictionary (:dictionary (get @*process-table* process-number))]
       (get dictionary key))))

(defn clean-process
  "Clean all data associated to this process locally or remotely"
  ([pid]
     (let [registered (dictionary-get pid :registered-name)
           registered-local (dictionary-get pid :registered-name-local)]
       (delete *coordination-service* (zk-process-path pid))
       (when (not (nil? registered))
         (delete *coordination-service* (str *node-names-znode* "/" registered)))
       (when (not (nil? registered-local))
         (swap! *local-name-register*
                        (fn [table] (dissoc table registered-local))))
       (when (evented-process? pid)
         (swap! *evented-table* (fn [table] (dissoc table (pid-to-process-number pid)))))
       (swap! *process-table* (fn [table] (dissoc table (pid-to-process-number pid)))))))

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
               (doseq [broken-pid broken-links]
                 (send! pid (protocol-link-broken broken-pid pid "node down"))))))
       (catch Exception ex
         (log :error (str "*** Exception " (.getMessage ex)))))))


(declare self)
(defn remote-send
  ([node pid msg]
     (if (exists? *coordination-service* (zk-process-path pid))
       (let [msg (if (and (= (map? msg))
                          (= :signal (keyword (:type msg))))
                   msg
                   (protocol-process-msg (self) pid msg))]
         (publish *messaging-service* msg))
       (throw (Exception. (str "Non existent remote process " pid))))))

(defn process-info
  "Returns information about the process identified by the provided PID"
  ([pid]
     (let [node (pid-to-node-id pid)]
       (if (= node @*node-id*)
         ;; The process is in this node
         (let [id (pid-to-process-number pid)
               info (get @*process-table* id)]
           (if (nil? info) nil {:pid pid :node node :alive true}))
         ;; The process is in a different node
         (if (exists? *coordination-service* (zk-process-path pid))
           {:pid pid :node node :alive true}
           nil)))))

(defn admin-send
  ([node pid msg]
     (if (exists? *coordination-service* (zk-process-path pid))
       (do
         (publish *messaging-service* msg))
       (throw (Exception. (str "Non existent remote process " pid))))))

(defn- initialize-actor
  ([is-evented]
     (let [^String pid (next-process-id)]
       (register-local-mailbox pid is-evented)
       (create *coordination-service* (zk-process-path pid) " ")
       (set-data *coordination-service* (zk-process-path pid) "running")
       pid)))

(defn spawn
  "Creates a new local process"
  ([]
     (initialize-actor false))
  ([f]
     (let [pid (spawn)
           mbox (pid-to-mbox pid)
           mbox-data (pid-to-mbox-data pid)
           f (if (string? f) (eval-ns-fn f) f)]
       (future
        (binding [*pid* pid
                  *mbox* mbox
                  *mbox-data* mbox-data]
          (try
           (let [result (f)]
             (clean-process *pid*))
           (catch Exception ex
             (log :error (str "*** process " pid " died with message : " (.getMessage ex) " " (vec (.getStackTrace ex))))
             (notify-links *pid* (str (class ex) ":" (.getMessage ex)))
             (remove-links *pid*)
             (clean-process *pid*)))))
       pid))
  ([f args]
     (spawn (fn [] (apply f args)))))

(defn spawn-in-repl
  "Creates a new process attached to the running shell"
  ([] (let [pid (spawn)]
        (alter-var-root #'*pid* (fn [_] pid))
        (alter-var-root #'*mbox* (fn [_] (pid-to-mbox pid)))
        (alter-var-root #'*mbox-data* (fn [_] (pid-to-mbox-data pid)))
        pid)))

(defn self
  "Returns the pid of the current process"
  ([] (if (nil? *pid*)
        (throw (Exception. "The current thread doest not have an associated PID"))
        *pid*)))

(defn send!
  "Sends a message to a local/remote process"
  ([pid msg]
     (let [node (pid-to-node-id pid)]
       (if (= node @*node-id*)
         (if (and (= (map? msg))
                  (= :signal (keyword (:type msg))))
           (dispatch-signal msg)
           (if (evented-process? pid)
             (do (send-to-evented pid (protocol-process-msg (self) pid msg))
                 :ok)
             (let [mbox (pid-to-mbox pid)
                   mbox-data (pid-to-mbox-data pid)]
               (critical-message-dispatch pid msg)
               :ok)))
         (remote-send node pid msg)))))

(declare critical-message-reception)
(declare critical-message-selective-reception)
(defn receive
  "Blocks until a new message has been received"
  ([] (critical-message-reception))
  ([filter]
     (critical-message-selective-reception filter)))

(defn- register-name-global
  "Associates a name that can be retrieved from any node and transformed into a PID"
  [name pid]
  (if (exists? *coordination-service* (str *node-names-znode* "/" name))
    (throw (Exception. (str "Already existent remote process " pid)))
    (do (create *coordination-service* (str *node-names-znode* "/" name) pid)
        (dictionary-write pid :registered-name name)
        :ok)))

(defn- register-name-local
  "Associates a name that can be translated locally to a PID"
  [name pid]
  (do
    (if (get @*local-name-register* name)
      (throw (Exception. (str "Already existent local process " (get @*local-name-register* name))))
      (swap! *local-name-register* (fn [table] (assoc table name pid))))
    (dictionary-write pid :registered-name-local name))
  :ok)

(defn register-name
  "Associates a name that can be translated locally or globally into a PID"
  ([name-or-map pid]
     (let [name (if (string? name-or-map) {:global name-or-map} name-or-map)]
       (if (:global name)
         (register-name-global (:global name) pid)
         (register-name-local (:local name) pid)))))

(defn registered-names
  "The list of globally registered names"
  ([]
     (let [children (get-children *coordination-service* *node-names-znode*)]
       (reduce (fn [m c] (let [data (get-data *coordination-service* (str *node-names-znode* "/" c))]
                          (assoc m c (String. data))))
               {}
               children))))

(defn resolve-name-global
  "Wraps a globally registered name"
  ([name]
     (if (exists? *coordination-service* (str *node-names-znode* "/" name))
       (String. (get-data *coordination-service* (str *node-names-znode* "/" name))))))

(defn resolve-name-local
  "Wraps a locally registered name"
  ([name]
     (get @*local-name-register* name)))

(defn resolve-name
  "Wraps a name locally or globally"
  ([name-or-map]
     (let [name (if (string? name-or-map) {:global name-or-map} name-or-map)]
       (if (:global name)
         (resolve-name-global (:global name))
         (resolve-name-local (:local name))))))

(defn resolve-node-name
  "Returns the identifier for a provided node name"
  ([node-name]
     (if (exists? *coordination-service*(str *node-nodes-znode* "/" node-name))
       (let [data (get-data *coordination-service* (str *node-nodes-znode* "/" node-name))]
         (String. data))
       (throw (Exception. (str "Unknown node " node-name))))))

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
           _2pc (make-2-phase-commit *coordination-service* (zk-link-tx-path tx-name) [pid (self)])]
       ;; we send the link request
       (admin-send (pid-to-node-id pid) pid (protocol-link-new (self) pid tx-name))
       ;; we commit and wait for a result
       ;; @todo add timeout!
       (let [result (commit *coordination-service* (zk-link-tx-path tx-name) (self))]
         (if (= result "commit")
           (add-link tx-name (self) pid)
           (throw (Exception. (str "Error linking processes " (self) " - " pid))))))))

;; Message routing

(declare react-to-pid)
(defn evented-data-handler
  "Builds an evented actor message handler for a certain callback"
  ([f]
     (fn [data]
       (let [pid (react-to-pid (:key data))
             mbox (pid-to-mbox pid)
             mbox-data (pid-to-mbox-data pid)]
         (binding [*pid* pid
                   *mbox* mbox
                   *mbox-data* mbox-data]
           (let [result (f (:content (:data data)))]
             (when (not= result :evented-loop-continue)
               (try (clean-process pid)
                    (catch Exception ex
                      (log :error (str "Error cleaning evented process "
                                       pid " : " (.getMessage ex) " "
                                       (vec (.getStackTrace ex)))))))))))))


(defn critical-message-dispatch
  ([pid msg]
     "Sends a message to an actor identified by PID"
     (let [mbox (pid-to-mbox pid)]
       (lam/enqueue mbox msg))))


(defn- update-save-queue
  ([pid new-save-queue]
     (swap! *process-table* (fn [t] (let [mbox-data (pid-to-mbox-data pid)
                                         mbox-data-p (assoc mbox-data :save-queue  new-save-queue)
                                         old-process-data (get t (pid-to-process-number pid))]
                                     (assoc t (pid-to-process-number pid)
                                            (assoc old-process-data :mbox-data mbox-data-p)))))))
  
(defn- do-save-queue
  "Tries to find a matching event in the saved queue for the handler of an evented actor"
  ([mbox-filter ^LinkedList save-queue]
     ;; Look for a potential match or the filter in the save queue
     (loop [msg (.poll save-queue)
            new-save-queue (LinkedList.)
            matching-msg nil]
       ;; End of the saved queue
       (if (nil? msg)
         [matching-msg new-save-queue]   
       ;; another item in the saved queue
       (if (and (nil? matching-msg) (mbox-filter msg))
         ;; we found a mathching message
         (recur (.poll save-queue)
                new-save-queue
                msg)
         ;; no matching message
         (recur (.poll save-queue)
                (do (.add new-save-queue msg) new-save-queue)
                matching-msg))))))

(defn critical-message-selective-reception
  ([mbox-filter]
     (let [save-queue (:save-queue (pid-to-mbox-data *pid*))
           [matching-msg ^LinkedList new-save-queue] (do-save-queue mbox-filter save-queue)
           _ (update-save-queue *pid* new-save-queue)]
       (if (nil? matching-msg)
         (loop [msg (lam/wait-for-message *mbox*)]
           (if (mbox-filter msg)
             msg
             (do (.add new-save-queue msg)
                 (recur (lam/wait-for-message *mbox*)))))
         matching-msg))))

(defn critical-message-selective-reception-evented
  ([f mbox-filter]
     (let [save-queue (:save-queue (pid-to-mbox-data *pid*))
           [matching-msg new-save-queue] (do-save-queue mbox-filter save-queue)
           _ (update-save-queue *pid* new-save-queue)]
       (if (nil? matching-msg)
         (let [handler-id (str *pid* ":message")
               data-handler (evented-data-handler f)
               callee (atom nil)
               callback (fn [data]
                          (let [pid (react-to-pid (:key data))
                                msg (:content (:data data))]
                            (if (mbox-filter msg)
                              (data-handler data)
                              (let [^LinkedList this-save-queue (:save-queue (pid-to-mbox-data pid))]
                                (.add this-save-queue msg)
                                (jevts/listen-once handler-id @callee)))))]
           (swap! callee (fn [_] callback))
           (jevts/listen-once handler-id callback))
         (f matching-msg)))))

(defn- default-filter
  ([x] true))

(defn critical-message-reception-evented
  ([f] (critical-message-selective-reception-evented f default-filter)))

(defn critical-message-reception
  ([] (critical-message-selective-reception default-filter)))


;; Evented actors

(def *react-loop-id* nil)

(defn react-to-pid
  ([^String evt-key] (first (vec (.split evt-key ":")))))

(defn react
  ([f] (do (critical-message-reception-evented f)
           :evented-loop-continue))
  ([filter f]
     (do (critical-message-selective-reception-evented f filter)
         :evented-loop-continue)))

(defn react-break
  ([]
     (let [pid *pid*
           old-map (get @*evented-table* (pid-to-process-number pid))
           mbox (:mbox old-map)
           env (:env old-map)
           react-loop-id (get env :loop)]
       (try (do
              (clean-process pid)
              ;(println (str "FINISHING LOOP " react-loop-id))
              (jevts/finish react-loop-id))
            (catch Exception ex
              (log :error (str "Error cleaning evented process "
                               pid " : " (.getMessage ex) " "
                               (vec (.getStackTrace ex)))))))))

(defn react-loop
  ([vals f]
     (let [react-loop-id (str (self) ":react-loop-" (random-uuid))]
       (jevts/listen react-loop-id
                     (fn [data] (let [;_ (println (str "*** REACT LOOP"))
                                     pid (react-to-pid (:key data))
                                     old-map (get @*evented-table* (pid-to-process-number pid))
                                     mbox (:mbox old-map)
                                     env (:env old-map)
                                     envp (assoc env :loop react-loop-id)
                                     _ (swap! *evented-table*
                                                      (fn [table] (assoc table (pid-to-process-number pid)
                                                                        (assoc old-map :env envp))))]
                                 (binding [*pid* pid
                                           *mbox* mbox]
                                   (let [result (apply f (:data data))]
                                     (when (not= result :evented-loop-continue)
                                       (react-break)))))))
       (apply jevts/publish [react-loop-id vals])
       :evented-loop-continue)))

(defn react-future
  ([action handler]
     (let [future-msg (str *pid* ":future:" (random-uuid))]
       (jevts/listen-once future-msg
                     (fn [data]
                       (let [pid (react-to-pid (:key data))
                             mbox (pid-to-mbox pid)
                             mbox-data (pid-to-mbox-data pid)]
                         (binding [*pid* pid
                                   *mbox* mbox
                                   *mbox-data* mbox-data]
                           (let [result (handler (:data data))]
                             (when (not= result :evented-loop-continue)
                               (try (clean-process pid)
                                    (jevts/finish future-msg)
                                    (catch Exception ex
                                      (log :error (str "Error cleaning evented process "
                                                       pid " : " (.getMessage ex) " "
                                                       (vec (.getStackTrace ex))))))))))))
       (future (let [result (try  (apply action [])
                                  (catch Exception ex ex))]
                 (apply jevts/publish [future-msg result])))
       :evented-loop-continue)))

(defn react-recur
  ([& vals] (let [old-map (get @*evented-table* (pid-to-process-number *pid*))
                  env (:env old-map)
                  loop (:loop env)]
            (apply jevts/publish [loop vals])
            :evented-loop-continue)))

(declare critical-message-dispatch)
(defn send-to-evented
  ([pid msg]
     (apply jevts/publish [(str pid ":message") msg])))

(defn spawn-evented
  ([actor-desc]
     (try
      (let [actor-desc (if (string? actor-desc) (eval-ns-fn actor-desc) actor-desc)
            pid (initialize-actor true)
            mbox (pid-to-mbox pid)
            mbox-data (pid-to-mbox-data pid)]
        (swap! *evented-table*
                       (fn [table] (assoc table
                                     (pid-to-process-number pid)
                                     mbox-data)))
        (binding [*pid* pid
                  *mbox* mbox
                  *mbox-data* mbox-data]
          (let [result (apply actor-desc [])]
            (when (not= result :evented-loop-continue)
              (try (clean-process pid)
                   (catch Exception ex
                     (log :error (str "Error cleaning evented process "
                                      pid " : " (.getMessage ex) " "
                                      (vec (.getStackTrace ex)))))))))
        pid)
      (catch Exception ex
        (log :error (str "Exception spawning evented " (.getMessage ex) " " (vec (.getStackTrace ex))))))))
