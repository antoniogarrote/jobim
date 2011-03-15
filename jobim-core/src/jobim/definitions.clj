(ns jobim.definitions)

;; Generic messaging protocol
(defprotocol MessagingService
  "Abstraction for different messaging systems"
  (connect-messaging [this]
           "Connects to the messaging service")
  (publish [this msg]
           "Sends the provided message")
  (set-messages-queue [this queue]
                      "Accepts an instance of a Queue where incoming messages will be stored"))

;; Constructors for the Messaging services
(defmulti make-messaging-service
  "Multimethod used to build the different messaging services"
  (fn [kind configuration coordination-service serialization-service] kind))

;; Generic cordination protocol
(defprotocol CoordinationService
  "Abstraction for different messaging systems"
  ;; connection
  (connect-coordination [this]
           "Connects to the coordination service using the provided configuration")
  ;; data nodes
  (exists? [this node-path]
           "Returns a true/false if there is a coordinated node for that node-path")
  (delete [this node-path]
          "Deletes the coordinated node")
  (create [this node-path data]
          "Creates a new coordinated node path with the provided value")

  (create-persistent [this node-path]
                     "Creates a new coordinated node path with the provided value that can persist between restarts")
  (get-data [this node-path]
            "Returns the data associated to the provided node-path")
  (set-data [this nodepath value]
            "Sets a value associated to the provided node path")
  ;; Groups
  (join-group [this group-name group-id value]
              "Joins a group with a certain group ID and a value for that ID")
  (watch-group [this group-name callback]
               "Start watching join/leave events in the group")
  (get-children [this group-name]
                "Returns a collection with all the children nodes in this group")
  ;; 2-phase commit
  (make-2-phase-commit [this name participants]
          "Creates a global transaction")
  (commit [this tx-name participant]
          "Commits the state of a transaction. Blocks until all other participants have commited/rollback")
  (rollback [this tx-name participant]
            "Rollbacks the transaction"))

;; Constructors for the coordination services

(defmulti make-coordination-service
  "Multimethod used to build the different coordination services"
  (fn [kind configuration] kind))


;; Generic encoding/decoding protocol

(defprotocol SerializationService
  "Abstraction for different messaging systems"
  (encode [this msg]
           "Encodes the message to be transmitted over the communication layer")
  (decode [this encoded-message]
          "Decodes a message received from the communication layer"))

(defmulti make-serialization-service
  "Multimethod used to build the different coordination services"
  (fn [kind configuration] kind))
