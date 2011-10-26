(ns jobim.examples.barber
  (:use [jobim])
  (:use [jobim.behaviours.events]))

;; Message logger

(def-event-handler BarberLogger
  (init [this initial-state] initial-state)
  (handle-event [this event state]
                (println (str "*** " event)))
  (terminate [this state] (println ("BarberLogger exiting"))))

(defn notify-shop [msg]
  (notify (resolve-name "barber-manager") msg))

;; Barber Shop
(defn do-shop
  "Shop actor"
  ([max-seats max-clients]
     (let [seats (atom [])
           max-clients (atom max-clients)]
       (loop [[msg pid] (receive)]
         (condp = msg
             ;; allocate a seat for the client
             :get-seat (if (< (count @seats) max-seats)
                         (do (swap! seats conj pid)
                             (send! pid :seated))
                         (send! pid :no-space))
             ;; barber asks for a client
             :next-client (let [pid (first @seats)
                                seats (swap! seats rest)]
                            (send! (resolve-name "the-barber") pid))
             ;; A client is leaving the shop
             :exiting (swap! max-clients dec))
         (if (> @max-clients 0)
           (recur (receive))
           (notify-shop "Closing barber shop"))))))

(defn shop
  ([max-seats max-clients]
     (let [seats (atom [])
           pid (spawn #(do-shop max-seats max-clients))]
       (register-name "barber-shop" pid))))

;; Customer
(defn customer
  "Customer actor"
  ([id]
     (spawn
      ;; enters the shop and ask for a seat
      #(loop []
         (println (str "Customer id " id "..."))
         (let [seated (do (send! (resolve-name "barber-shop") [:get-seat (self)])
                          (receive))]
           ;; we have a seat, wait to have a hair cut
           (if (= seated :seated)
             (let [_ (send! (resolve-name "the-barber") :hey) ;; Awaking the barber
                   msg (receive)]
               ;; barber starting to do his thing
               (when (= msg :cutting)
                 (notify-shop (str "Customer " id " getting his hair cut")))
               ;; barber finished
               (let [msg (receive)]
                 (when (= msg :finished)
                   (notify-shop (str "Customer " id " got his hair cut")))
                 ;; leaving the shop
                 (send! (resolve-name "barber-shop") [:exiting (self)])
                 (notify-shop (str "Customer " id " leaving the shop"))))
             ;; No space available in the shop, exiting
             (do
               (notify-shop (str "Customer " id " could not get his hair cut"))
               (Thread/sleep (rand 5000))
               (recur))))))))

;; Barber

(defn barber
  "Barber actor"
  ([]
     (let [pid (spawn
                #(loop [msg (receive)] ;; wait for a customer
                   (when (= msg :hey)  ;; awaken!
                     ;; ask for the customer to come in
                     (send! (resolve-name "barber-shop") [:next-client (self)])
                     (let [client-pid (receive (fn [msg] (not (= :hey msg))))] ;; ignore other customers trying to awake us
                       ;; starting to cut
                       (send! client-pid :cutting)
                       (Thread/sleep (rand 4000))
                       ;; finished cutting
                       (send! client-pid :finished)))
                   (recur (receive))))]
       (register-name "the-barber" pid))))


(defn run-barber-shop
  "Runs the barber shop problem in a single node"
  ([num-seats num-clients]
     (start-event-manager "barber-manager")
     (add-handler (resolve-name "barber-manager")
                  [name jobim.examples.barber.BarberLogger]
                  nil)
     (shop num-seats num-clients)
     (barber)
     (doseq [i (range 0 num-clients)]
       (customer i))))

(defn run-barber-shop-distributed
  "Runs the barber shop problem distributed in a set of nodes"
  ([num-seats num-clients nodes]
     (start-event-manager "barber-manager")
     (add-handler (resolve-name "barber-manager")
                  [name jobim.examples.barber.BarberLogger]
                  nil)
     (shop num-seats num-clients)
     (barber)
     (loop [ids (range 0 num-clients)
            nodes (cycle nodes)]
       (if (empty? ids)
         (println (str "*** All clients spawned"))
         (do (rpc-blocking-call (resolve-node-name (first nodes))
                                "jobim.examples.barber/customer"
                                [(first ids)])
             (recur (rest ids)
                    (rest nodes)))))))
