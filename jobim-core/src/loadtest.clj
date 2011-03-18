(ns loadtest
  (:use [jobim]
        [jobim.behaviours.server]))

(def *nodes* ["osx" "debian"])

(def-server Cell

  (init [this [counter prev next]]
        {:counter counter :newborn true :prev prev :next next})

  (handle-call [this request from state]
               (condp = (:key request)
                 ;; we are requested to set the previous pointer
                 :set-prev (reply :ok (assoc state :prev (:value request)))
                 ;; we are requested to set the next pointer
                 :set-next (reply :ok (assoc state :next (:value request)))
                 ;; returns the state of the cell for debug purposes
                 :state (reply state state)
                 ;; stop the server, breaking the linked list
                 :exit     (stop :ok)))

  (handle-cast [this token state]
               (Thread/sleep  3000)
               (println (str "** Cell " (:counter state) " value token: " token))
               (send-cast! (:next state) (inc token))
               (noreply state)))

(defn make-cell
  ([cell-name c]
     (start (name cell-name) (loadtest.Cell.) [c nil nil])))

(defn link-cells
  ([x y]
     (let [pidx (resolve-name (name x))
           pidy (resolve-name (name y))]
       (send-call! pidx {:key :set-next :value pidy})
       (send-call! pidy {:key :set-prev :value pidx}))))

(defn run-token
  ([initial-node]
     (send-cast! (resolve-name (name initial-node)) 0)))
