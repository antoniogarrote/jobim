(ns jobim.services.coordination.localnode
  (use [jobim.definitions]))

(defn- do-set-data
  ([cells ^String node-path ^String value]
     (swap! cells (fn [cs] (let [cell (get cs node-path)]
                                   (if (not (nil? cell))
                                     (assoc cs node-path value)
                                     cs))))))

(defn- do-create
  ([cells groups ^String node-path ^String data]
     (swap! cells (fn [cs]
                    (let [pair (first (filter (fn [[^String name callbacks]] (= 0 (.indexOf node-path name))) @groups))]
                      (when (not (nil? pair))
                        (let [[^String group callbacks] pair
                              val (aget (.split node-path (str group "/")) 1)]
                          (doseq [cb callbacks]
                            (cb :member-enter val))))
                      (assoc cs node-path data))))))

(deftype LocalNodeCoordinationService [tx-map cells groups] jobim.definitions.CoordinationService
  ;; connection
  (connect-coordination [this] :ignore)
  ;; data nodes
  (exists? [this node-path]
           (not (nil? (get @cells node-path))))
  (delete [this node-path]
          (swap! cells (fn [cs] (let [cell (get cs node-path)]
                                 (if (not (nil? cell))
                                   (do
                                     (let [pair (first (filter (fn [[name callbacks]] (= 0 (.indexOf node-path name))) @groups))]
                                       (when (not (nil? pair))
                                         (let [[group callbacks] cell
                                               val (aget (.split node-path group) 1)]
                                           (doseq [cb callbacks]
                                             (cb :member-left val)))))
                                     (dissoc cs node-path))
                                   cs)))))
  (create [this node-path data]
          (do-create cells groups node-path data))
  (create-persistent [this node-path]
                     (create this node-path ""))
  (get-data [this node-path]
            (get @cells node-path))
  (set-data [this node-path value]
            (do-set-data cells node-path value))
  ;; groups
  (join-group [this group-name group-id value]
              (create this (str group-name "/" group-id) value))

  (watch-group [this group-name callback]
               (swap! groups (fn [gs] (let [cs (get gs group-name)]
                                       (if (nil? cs)
                                         (assoc gs group-name [callback])
                                         (assoc gs group-name (conj cs callback)))))))
  (get-children [this group-name]
                (reduce (fn [ac [node-path [v cs]]]
                          (let [parts (.split node-path (str group-name "/"))]
                            (if (= (alength parts) 2)
                              (conj ac (aget parts 1))
                              ac)))
                        []
                        @cells))
  ;; 2pc protocol
  (make-2-phase-commit [this tx-name participants]
                       (swap! tx-map (fn [tx-m]
                                       (let [tx-m (if (get tx-m tx-name)
                                                    (dissoc tx-m tx-name)
                                                    tx-m)]
                                         (assoc tx-m tx-name {:state "incomplete"
                                                              :promises []
                                                              :votes    (count participants)})))))
  (commit [this tx-name participant]
          (let [state (:state (get @tx-map tx-name))]
            (if (= "incomplete" state)
              (let [p (promise)]
                (swap! tx-map (fn [tx-m]
                                (let [tx-info (get tx-m tx-name)]
                                  (if (= (:state tx-info) "incomplete")
                                    (let [votes (dec (:votes tx-info))
                                          promises (conj (:promises tx-info) p)]
                                      (if (= 0 votes)
                                        (do (doseq [p promises]
                                              (deliver p "commit"))
                                            (assoc tx-m tx-name {:state "commit"
                                                                 :votes 0
                                                                 :promises promises}))
                                        (assoc tx-m tx-name {:state "incomplete"
                                                             :votes votes
                                                             :promises promises})))
                                    tx-m))))
                @p)
              state)))
  (rollback [this tx-name participant]
            (do (swap! tx-map (fn [tx-m]
                                (let [tx-info (get tx-m tx-name)
                                      state (:state tx-info)
                                      votes (:votes tx-info)
                                      promises (:promises tx-info)]
                                  (if (= "incomplete" state)
                                    (do (doseq [p promises]
                                          (deliver p "rollback"))
                                        {:state "rollback"
                                         :votes votes
                                         :promises promises})
                                    ;; It has to be already cancelled
                                    (do
                                      (assert (= state "rollback"))
                                      tx-m)))))
                "rollback")))

(defmethod make-coordination-service :localnode
  ([kind configuration]
     (LocalNodeCoordinationService. (atom {}) (atom {}) (atom {}))))
