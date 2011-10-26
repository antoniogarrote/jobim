(ns loadtest
  (:use [jobim]
        [jobim.behaviours.server])
  (:use [clojure.contrib.io :only [writer]]))

(def *nodes* ["osx"])
(def *prob-new-cell* 0.3)
(def *first-cell* "cell0")

(defn make-cell
  ([cell-name]
     (start cell-name (loadtest.Cell.) [cell-name])))

(defn make-cell-remote
  ([server-name cel-name]
     (rpc-blocking-call (resolve-node-name server-name)
                        "loadtest/make-cell" [cel-name])))

(defn link-cell
  ([state pid]
     (let [tmp  @(:next state)]
       ;(println (str (self) " -> " pid " -> " tmp))
       (send-call! pid {:key :set-next :value tmp}))
       (swap! (:next state) (fn [_] pid))))

(defn make-and-link
  ([state server]
     (let [cell-id (str (:id state) "." (swap! (:counter state) inc))
           new-cell (make-cell-remote server cell-id)]
       (link-cell state new-cell))))

(defn run-token
  ([initial-node]
     (send-cast! (resolve-name initial-node) 0)))

(defn add-link?
  ([] (let [prob (rand 1)]
        (< prob *prob-new-cell*))))

(defn random-server
  ([] (nth *nodes*
           (int (Math/floor (rand (count *nodes*)))))))

(defn timestamp
  ([] (.getTime (java.util.Date.))))

(defn write-token-timestamp
  ([state token]
     (.write (:out state) (str "\r\n" (timestamp) "," token))
     (.flush (:out state))))

(defn first-cell?
  ([state] (= (:id state) *first-cell*)))

(def-server Cell

  (init [this [id]]
        (if (= id *first-cell*)
          {:id id :next (atom (self)) :counter (atom 0) :out (writer "out.txt")}
          {:id id :next (atom (self)) :counter (atom 0)}))

  (handle-call [this request from state]
               (condp = (:key request)
                   ;; we are requested to set the next pointer
                   :set-next (reply :ok (do (swap! (:next state) (fn [_] (:value request))) state))
                   ;; returns the state of the cell for debug purposes
                   :state    (reply [(:id state) @(:next state)] state)
                   ;; test method creates a new link
                   :link     (let [pid (make-and-link state (random-server))]
                               (reply pid state))
                   ;; stop the server, breaking the linked list
                   :exit     (stop :ok)))

  (handle-cast [this token state]
               (Thread/sleep  3000)
               (println (str "** Cell " (:id state) " value token: " token))
               (when (add-link?)
                 (make-and-link state (random-server)))
               (if (first-cell? state)
                 (do
                   (write-token-timestamp state token)
                   (send-cast! @(:next state) 0))
                 (send-cast! @(:next state) (inc token)))
               (noreply state)))
(defn state?
  ([name] (send-call! (resolve-name name) {:key :state})))
