(ns jobim
  (:require [jobim.core :as core]))

(defn bootstrap-node
  "Adds a new node to the distributed application"
  ([file-path] (apply core/bootstrap-node [file-path]))
  ([name rabbit-args zookeeper-args] (apply core/bootstrap-node [name rabbit-args zookeeper-args])))

(defn nodes
  "Returns all the available nodes and their identifiers"
  ([] (apply core/nodes [])))

(defn spawn
  "Creates a new local process"
  ([]
     (apply core/spawn []))
  ([f]
     (apply core/spawn [f])))

(defn spawn-evented
  "Spawns an evented process"
  ([f]
     (apply core/spawn-evented [f])))

(defn spawn-in-repl
  "Creates a new process attached to the running shell"
  ([] (apply core/spawn-in-repl [])))

(defn self
  "Returns the pid of the current process"
  ([] (apply core/self [])))

(defn send!
  "Sends a message to a local/remote process"
  ([pid msg]
     (apply core/send! [pid msg])))

(defn receive
  "Blocks until a new message has been received"
  ([] (apply core/receive [])))

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

(defn react
  "Receives a message in an evented actor"
  ([f]
     (apply core/react [f])))

(defmacro react-loop
  "An infinite execution loop for an evented actor"
  ([& body]
     `(let [c# (fn []
                ~@body)]
        (with-meta {:loop c#} {:evented true}))))
