(ns jobim
  (:require [jobim.core :as core]))

(defn nodes
  "Returns all the available nodes and their identifiers"
  ([] (apply core/nodes [])))

(defn spawn
  "Creates a new local process"
  ([]
     (apply core/spawn []))
  ([f]
     (apply core/spawn [f]))
  ([f args]
     (apply core/spawn [f args])))

(defn spawn-evented
  "Spawns an evented process"
  ([f]
     (apply core/spawn-evented [f])))

(defn spawn-in-repl
  "Creates a new process attached to the running shell"
  ([] (apply core/spawn-in-repl [])))

(defn process-info
  "Returns information about the process identified by the provided PID"
  ([pid]
     (apply core/process-info [pid])))

(defn self
  "Returns the pid of the current process"
  ([] (apply core/self [])))

(defn send!
  "Sends a message to a local/remote process"
  ([pid msg]
     (apply core/send! [pid msg])))

(defn receive
  "Blocks until a new message has been received
   a predicate can be passed as an argument for selective reception
   of messages."
  ([] (apply core/receive []))
  ([filter] (apply core/receive [filter])))

(defn register-name
  "Associates a name that can be retrieved from any node to a PID"
  [name pid]
  (apply core/register-name [name pid]))

(defn registered-names
  "The list of globally registered names"
  ([]
     (apply core/registered-names [])))

(defn resolve-name
  "Wraps a globally registered name"
  ([name]
     (apply core/resolve-name [name])))

(defn resolve-node-name
  "Returns the identifier for a provided node name"
  ([node-name]
     (apply core/resolve-node-name [node-name])))

(defn rpc-call
  "Executes a non blocking RPC call"
  ([node function args]
     (apply core/rpc-call [node function args])))

(defn rpc-blocking-call
  "Executes a blocking RPC call"
  ([node function args]
     (apply core/rpc-blocking-call [node function args])))

(defn link
  "Links this process with the process identified by the provided PID. Links are bidirectional"
  ([pid]
     (apply core/link [pid])))

(defmacro react
  "Receives a message in an evented actor"
  ([vals-bindings & body]
     `(apply core/react [(fn [~@vals-bindings] ~@body)])))

(defmacro react-selective
  "Receives a message in an evented actor using selective reception
   of messages."
  ([filter vals-bindings & body]
     `(apply core/react [~filter (fn [~@vals-bindings] ~@body)])))

(defmacro react-loop
  "Creates an evented loop in an actor description"
  ([vals-vars & body]
    (let [vars-b (map #(nth vals-vars %1) (filter odd? (range 0 (count vals-vars))))
          vals-b (map #(nth vals-vars %1) (filter even? (range 0 (count vals-vars))))]
      (if (empty? vars-b)
        `(core/react-loop [:not-argument-react-loop] (fn [_#] (do ~@body)))
        `(core/react-loop [~@vars-b] (fn [~@vals-b] (do ~@body)))))))

(defn react-future
  "Handles some blocking operation in an evented actor"
  ([action handler]
     (apply core/react-future [action handler])))

(defn react-recur
  "Recurs in react-loop"
  ([] (apply core/react-recur [:not-argument-react-loop]))
  ([& vals]
     (apply core/react-recur vals)))


(defn bootstrap-node
  "Adds a new node to the Jobim cluster"
  ([file]
     (let [config (eval (read-string (slurp file)))
           node-name (or (:node-name config)
                         (str (.getHostName (java.net.InetAddress/getLocalHost)) "-"
                              (.replace (java.util.UUID/randomUUID) "-" "")))
           coordination-type (symbol (str "jobim.services.coordination." (name (or (:coordination-type config) :localnode))))
           messaging-type (symbol (str "jobim.services.messaging." (name (or (:messaging-type config) :localnode))))
           serialization-type (symbol (str "jobim.services.serialization." (name (or (:serialization-type config) :java))))]
       (use 'jobim.definitions)
       (use coordination-type)
       (use messaging-type)
       (use serialization-type)
       (apply core/bootstrap-node [file])
       (spawn-in-repl)))
  ([name
    coordination-type coordination-args
    messaging-type messaging-args
    serialization-type serialization-args]
     (apply core/bootstrap-node [name
                                 coordination-type coordination-args
                                 messaging-type messaging-args
                                 serialization-type serialization-args])
     (spawn-in-repl)))

(defn bootstrap-local
  "Bootstrap a node without distribution support"
  ([name]
     (bootstrap-node name :localnode {} :localnode {} :java {})))

