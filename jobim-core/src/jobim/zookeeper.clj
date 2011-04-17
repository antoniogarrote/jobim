(ns jobim.zookeeper
  (:use [clojure.contrib.logging :only [log]])
  (:import [org.apache.zookeeper
            ZooKeeper
            Watcher]
           [org.apache.zookeeper.data
            Id
            ACL]))

(defonce *zk* nil)

(defn state-to-keyword
  ([state]
     (condp = (.getIntValue state)
       (.getIntValue org.apache.zookeeper.Watcher$Event$KeeperState/Disconnected)    :disconnected
       (.getIntValue org.apache.zookeeper.Watcher$Event$KeeperState/Expired)         :expired
       (.getIntValue org.apache.zookeeper.Watcher$Event$KeeperState/NoSyncConnected) :no-sync-connected
       (.getIntValue org.apache.zookeeper.Watcher$Event$KeeperState/SyncConnected)   :sync-connected
       :unknown)))

(defn event-type-to-keyword
  ([type]
     (condp = (.getIntValue type)
       (.getIntValue org.apache.zookeeper.Watcher$Event$EventType/NodeChildrenChanged) :node-children-changed
       (.getIntValue org.apache.zookeeper.Watcher$Event$EventType/NodeCreated)         :node-created
       (.getIntValue org.apache.zookeeper.Watcher$Event$EventType/NodeDataChanged)     :node-data-changed
       (.getIntValue org.apache.zookeeper.Watcher$Event$EventType/NodeDeleted)         :node-deleted
       (.getIntValue org.apache.zookeeper.Watcher$Event$EventType/None)                :none
       (throw (Exception. (str "Unknown event type: " type))))))

(defn event-to-map
  ([evt] {:path (.getPath evt) :type (event-type-to-keyword (.getType evt)) :state (state-to-keyword (.getState evt))}))

(defn- watcher
  "Creates a new watcher with the provided function"
  ([f] (proxy [Watcher] []
         (process [event] (f (event-to-map event))))))

(defn- stat-callback
  "Creates a new callback"
  ([f] (proxy [org.apache.zookeeper.AsyncCallback$StatCallback] []
         (processResult [rc path ctx stat]
                        (f rc path stat)))))

(defn- data-callback
  "Creates a new callback"
  ([f] (proxy [org.apache.zookeeper.AsyncCallback$DataCallback] []
         (processResult [rc path ctx data stat]
                        (f rc path data stat)))))

(defn make
  "Connects to a zookeeper cluster and returns a connection object"
  ([servers opts]
     (let [servers-str (if (string? servers) servers (apply str servers))
           session-timeout (:timeout opts)
           prom (promise)
           watcher-fn (fn [e] (when (= (:state e) :sync-connected) (deliver prom :connected)))
           session-id (:id opts)
           password (:password opts)
           connection (atom nil)]
       (let [zk (if (nil? password)
                  (ZooKeeper. servers-str session-timeout (watcher watcher-fn))
                  (ZooKeeper. servers-str session-timeout (watcher watcher-fn (long session-id) (.getBytes password))))]
         @prom
         (swap! connection (fn [_] zk))
         {:servers servers
          :session-timeout session-timeout
          :session-id session-id
          :password password
          :connection @connection}))))

(defn connect
  "Connects to a zookeeper cluster"
  ([servers opts]
     (let [zk (make servers opts)]
       (alter-var-root #'*zk* (fn [_] zk))
       *zk*)))

(defn state
  "Get the state of the connection"
  ([] (.getState (:connection *zk*))))

(defn close
  ([]
     (do (.close (:connection *zk*))
         (alter-var-root #'*zk* {}))))

(defn- permission-sym
  ([p] (condp = p
         :all    org.apache.zookeeper.ZooDefs$Perms/ALL
         :admin  org.apache.zookeeper.ZooDefs$Perms/ADMIN
         :create org.apache.zookeeper.ZooDefs$Perms/CREATE
         :delete org.apache.zookeeper.ZooDefs$Perms/DELETE
         :read   org.apache.zookeeper.ZooDefs$Perms/READ
         :write  org.apache.zookeeper.ZooDefs$Perms/WRITE
         (throw (Exception. (str "Unknown zookeeper permission: " p))))))


(defn- world-scheme
  ([] (let [id (Id.)]
        (.setScheme id "world")
        (.setId id "anyone")
        id)))

(defn- auth-scheme
  ([] org.apache.zookeeper.ZooDefs$Ids/AUTH_IDS))

(defn- digest-scheme
  ([username-password] (let [id (Id.)]
                         (.setScheme id "digest")
                         (.setId id username-password)
                         id)))

(defn- ip-scheme
  ([ip] (let [id (Id.)]
          (.setScheme id "ip")
          (.setId id ip)
          id)))

(defn- process-acl-map-pre
  ([acl-map]
     (reduce (fn [ac [k perms]] (assoc ac k (map #(permission-sym %1) perms))) {} acl-map)))

(defn- make-acl
  ([scheme perms]
     (if (coll? scheme)
       (let [[s d] scheme]
         (condp = s
           :digest (map (fn [p] (ACL. p (digest-scheme d))) perms)
           :ip     (map (fn [p] (ACL. p (ip-scheme d))) perms)
           (throw (Exception. "Uknown ACL scheme: " s))))
       (condp = scheme
         :world (map (fn [p] (ACL. p (world-scheme))) perms)
         :auth (map (fn [p] (ACL. p (auth-scheme))) perms)
         (throw (Exception. "Uknown ACL scheme: " scheme))))))

(defn- process-acl-map
  ([acl-map]
     (let [acl-map (process-acl-map-pre acl-map)
           acl-map (reduce (fn [ac [scheme perms]]
                             (let [next-acl-list (make-acl scheme perms)]
                               (concat ac next-acl-list))) [] acl-map)]
       (vec acl-map))))

(defn- process-create-mode
  ([create-mode]
     (condp = create-mode
       :ephemeral org.apache.zookeeper.CreateMode/EPHEMERAL
       :ephemeral-sequential org.apache.zookeeper.CreateMode/EPHEMERAL_SEQUENTIAL
       :persistent org.apache.zookeeper.CreateMode/PERSISTENT
       :persistent-sequential org.apache.zookeeper.CreateMode/PERSISTENT_SEQUENTIAL
       (throw (Exception. (str "Unknown create mode " create-mode))))))

(defn- stat-to-map
  "Creates a map with the data in a stat object"
  ([stat] {:aversion (.getAversion stat)
           :ctime (.getCtime stat)
           :cversion (.getCversion stat)
           :czxid (.getCzxid stat)
           :data-length (.getDataLength stat)
           :ephemeral-owner (.getEphemeralOwner stat)
           :mtime (.getMtime stat)
           :mzxid (.getMzxid stat)
           :num-children (.getNumChildren stat)
           :pzxid (.getPzxid stat)
           :version (.getVersion stat)}))

(defn- map-to-stat
  "Creates a new stat object from a stat map"
  ([m]
     (org.apache.zookeeper.data.Stat.
      (:czxid m)
      (:mzxid m)
      (:ctime m)
      (:mtime m)
      (:version m)
      (:cversion m)
      (:aversion m)
      (:ephemeral-owner m)
      (:data-length m)
      (:num-children m)
      (:pzxid m))))

(defn create
  "Creates a new znode in a zookeeper server"
  ([path acl-map create-mode]
     (create path " " acl-map create-mode))
  ([path data acl-map create-mode]
     (let [data (if (string? data) (.getBytes data) data)]
       (.create (:connection *zk*) path data (process-acl-map acl-map) (process-create-mode create-mode)))))

(defn exists?
  "Checks the stats for the znode"
  ([path & opts]
     (let [should-watch-or-fn (if (empty? opts) false (first opts))]
       (if (fn? should-watch-or-fn)
         (let [f should-watch-or-fn]
           (.exists (:connection *zk*) path (watcher f)))
         (let [result (.exists (:connection *zk*) path should-watch-or-fn)]
           (if (nil? result) result (stat-to-map result)))))))


(defn get-data
  "Retrieve the data from the provided znode path"
  ([path]
     (let [to-return (promise)]
       (.getData (:connection *zk*) path false (data-callback (fn [rc path data stat]
                                                                (deliver to-return [data (stat-to-map stat)]))) {})
       @to-return))

  ([path stat]
     (.getData (:connection *zk*) path false (map-to-stat stat)))

  ([path stat f]
     (if (fn? f)
       (.getData (:connection *zk*) path (watcher f) (map-to-stat stat))
       (let [should-watch f]
         (.getData (:connection *zk*) path should-watch (map-to-stat stat))))))

(defn- maybe-version
  ([mv] (if (map? mv) (:version mv) mv)))

(defn set-data
  "Sets the value of a znode"
  ([path data version]
     (let [data (if (string? data) (.getBytes data) data)]
       (stat-to-map (.setData (:connection *zk*) path data (maybe-version version))))))

(defn delete
  "Deletes a znode"
  ([path version]
     (.delete (:connection *zk*) path (maybe-version version))))


(defn get-children
  "Returns all the children from a znode"
  ([path]
     (vec (.getChildren (:connection *zk*) path false)))
  ([path f]
     (vec (.getChildren (:connection *zk*) path (watcher f)))))

(defmacro with-zookeeper
  ([zk & body]
     `(binding [*zk* ~zk]
        ~@body)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; High level operations
;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Writing of maps

(defn set-map
  "Writes a map under a znode as pairs of children znode key -> value.
   The function writes nested maps recursively"

  ([path m acl-map create-mode]
     (when-not (exists? path)
       (throw (Exception. "Root node doest not exists")))
     (set-map (map (fn [[k v]] [(str path "/" (if (keyword? k) (name k) k)) v] ) m) acl-map create-mode))

  ([children acl-map create-mode]
     (if-not (empty? children)
       (let [[path v] (first children)]
         ; If the children is a map we add the pairs to the list and recur
         (if (map? v)
           (do
             ; Before recuring, we create the znode where the inner map will be stored
             (when-not (exists? path)
               (create path "" acl-map create-mode))
             (recur (concat (rest children) (map (fn [[k v]] [(str path "/" (if (keyword? k) (name k) k)) v]) v))
                    acl-map create-mode))
           ; If it is a plain value we just insert the value
           (do
             (when-not (exists? path)
               (create path v acl-map create-mode) )
             (recur (rest children) acl-map create-mode)))))))

;;; Group membership

(defn watch-group
  "Watches changes of members in a group"
  ([group-path callback]
     (let [initial-children (get-children group-path)]
       (get-children group-path
                     (fn [evt]
                       (try
                        (let [new-children (get-children group-path)
                              diff (if (> (count new-children) (count initial-children))
                                     (vec (clojure.set/difference (set new-children) (set initial-children)))
                                     (vec (clojure.set/difference (set initial-children) (set new-children))))
                              kind (if (> (count new-children) (count initial-children)) :member-joined :member-left)
                              result (callback {:kind kind :members diff})]
                          (when (not= result :cancel)
                            (watch-group group-path callback)))
                        (catch Exception ex (log :error (str "error watching group: " (.getMessage ex))))))))))

(defn join-group
  "Joins a group with the provided name"
  ([group-path member-name]
     (join-group group-path member-name " "))
  ([group-path member-name value]
     (let [stat (exists? (str group-path "/" member-name))]
       (if stat
         :already-member
         (do (create (str group-path "/" member-name) value {:world [:read]} :ephemeral) :ok)))))

(defn leave-group
  "Makes member with name member-name leave the provided group"
  ([group-path member-name]
     (let [stat (exists? (str group-path "/" member-name))]
       (if stat
         (do (delete (str group-path "/" member-name) (:version stat)) :ok)
         :not-member))))


;;; 2 Phase Commit

(defn make-2-phase-commit
  "Creates a new transaction with name tx-name and some participants"
  ([tx-name tx-participants]
     (do
       ;; Cleaning old transaction
       (doseq [tx-part (if (exists? tx-name) (get-children tx-name) [])]
         (let [stats (exists? (str tx-name "/" tx-part))]
           (when stats (delete (str tx-name "/" tx-part) (:version stats)))))
       (let [stats (exists? tx-name)] (when stats (delete tx-name (:version stats))))
       ;; transaction znode
       (create tx-name {:world [:all]} :persistent)
       ;; create commit/rollback handlers
       (doseq [tx-part tx-participants]
         (let [tx-part-name (str tx-name "/" tx-part)]
           (create tx-part-name "pending" {:world [:all]} :ephemeral))))))

(defn- check-transaction-part
  ([part prom]
     ;; we get current status for the participant
     (let [[data stats] (get-data part)
           result (String. data)]
       (condp = result
         ;; If it has voted commit/rollback, we return the state without setting
         ;; a callback
         "commit"   (deliver prom "commit")
         "rollback" (deliver prom "rollback")
         ;; If the status is pending we set up a callback and wait until it votes
         "pending"  (let [d (get-data part stats (fn [e]
                                                   (try
                                                    ;; data change, lets change
                                                    ;; the current status and we
                                                    ;; dliver it
                                                    (let [s (exists? part)]
                                                      (if (nil? s) (deliver prom "rollback")
                                                          (if (= "commit" (String. (get-data part s)))
                                                            (deliver prom "commit")
                                                            (deliver prom "rollback"))))
                                                    ;; There could be a previous
                                                    ;; delivery, so we catch the
                                                    ;; exception in case we are
                                                    ;; doing doulbe delivery
                                                   (catch Exception ex
                                                     (do (log :error (str "Error checking transaction: " (.getMessage ex))))))))]
                      ;; We check the status when setting up the watcher in case
                      ;; it has change since the last check
                      (condp = (String. d)
                        "pending" ""
                        (try (deliver prom (String. d))
                             ;; Callback could have already deliver, we catch
                             ;; double delivery
                             (catch Exception ex (log :error  (str "Error checking transaction: " (.getMessage ex)))))))
         ;; Unknown state -> rolling back
         (deliver prom "rollback")))))

(defn commit
  "Commits in the 2-phase-commit protocol and returns :commited or :rollback depending on the other participants"
  ([tx-name tx-commit]
     ;; throw exception if there is no transaction running
     (when (nil? (exists? tx-name)) (throw (Exception. "Non existant transaction")))

     (let [tx-participants (filter #(not= %1 tx-commit) (get-children tx-name)) ;; Discover participants in the transaction
           proms (map (fn [_] (promise)) tx-participants)]
       ;; Check the status of other participants and set a callback that will
       ;; deliver commit/rollback in the provided promise when the participant
       ;; has voted
       (doseq [tx-part tx-participants
               prom proms]
         (apply check-transaction-part [(str tx-name "/" tx-part) prom]))
       ;; We set ourt state
       (set-data (str tx-name "/" tx-commit) "commit" 0)
       ;; We wait until all the participants have voted commit or one of them
       ;; has voted rollback
       (loop [should-continue true
              proms proms]
         (if (not (empty? proms))
           (let [prom (first proms)
                 result @prom]
             (if (= result "commit")
               (recur true (rest proms))
               "rollback"))
           "commit")))))

(defn rollback
  "Rollbacks in the 2-phase-commit protocol"
  ([tx-name tx-rollback]
     ;; Throw exception if there is no running transaction
     (when (nil? (exists? tx-name)) (throw (Exception. "Non existant transaction")))
     ;; We set vote rollback and returns rollback
     (let [tx-part  (str tx-name "/" tx-rollback)
           stats (exists? tx-part)]
       (set-data tx-part "rollback" (:version stats))
       "rollback")))
