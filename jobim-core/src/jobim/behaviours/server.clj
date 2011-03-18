(ns jobim.behaviours.server
  (:use [jobim]
        [jobim.utils]
        [matchure]))

;; Server protocol

(defprotocol Server
  (init [this initial-value] "Invoked when the server starts execution")
  (handle-call [this request from state] "Invoked when a new synchronous request arrives at the server")
  (handle-cast [this request state] "Invoked when a new asynchronous request arrives at the server")
  (handle-info [this request state] "Handles messages distinct to events")
  (terminate [this state] "Clean up code invoked when the server is going to be terminated externally"))


;; Server implementation callback modeul responses API

(defn reply
  "Returns the data to be sent to the client and the new state of the server"
  ([reply-data new-state]
     [:reply reply-data new-state]))

(defn noreply
  "Does not send any data back to the client and pass the new state of the server"
  ([new-state]
     [:noreply new-state]))

(defn stop
  "Stops the server maybe sending some data back to the client"
  ([reply-data]
     [:stop reply-data])
  ([] [:stop :ok]))


(defmacro def-server
  ([name & defs]
     `(deftype ~name [] jobim.behaviours.server.Server
        ~@defs)))

;; Server actors

(defn- do-server-call
  ([server from request state]
     (let [result (handle-call server request from state)]
       (cond-match
        [[:noreply ?new-state] result]           [:recur new-state]
        [[:reply ?reply-data ?new-state] result] (do (send! from [(self) reply-data])
                                                     [:recur new-state])
        [[:stop ?reply-data] result]             (do (send! from [(self) reply-data])
                                                     :norecur)
        [_ result]                               (throw (Exception. (str "Unknown result from handle-call: " result)))))))

(defn- do-server-cast
  ([server request state]
     (let [result (handle-cast server request state)]
       (cond-match
        [[:noreply ?new-state] result] [:recur new-state]
        [[:stop ?reply-data] result]   :norecur
        [_ result]                     (throw (Exception. (str "Unknown result from handle-cast: " result)))))))

(defn- do-handle-info
  ([server request state]
     (let [result (handle-info server request state)]
       (cond-match
        [[:noreply ?new-state] result] [:recur new-state]
        [[:stop _] result]             :norecur
        [_ result]                     (throw (Exception. (str "Unknown result from handle-info: " result)))))))

(defn- server-actor
  "Defines the loop of a server actor"
  ([initial-msg server]
     (loop [msg (receive)
            state (init server initial-msg)]
       (let [result (cond-match
                     ;; Call
                     [[:server-call ?pid ?from ?request] msg]  (do-server-call server from request state)
                     ;; Cast
                     [[:server-cast ?request] msg]             (do-server-cast server request state)
                     ;; Terminate                               
                     [:terminate msg]                          (do (terminate server state) :norecur)
                     ;; Info                                    
                     [_ msg]                                   (do-handle-info server msg state))]
         (when (not= result :norecur)
           (recur (receive) (second result)))))))

(defn- server-evented-actor
  "Defiles the evented loop of a server actor"
  ([initial-msg server]
     (react-loop [state (init server initial-msg)]
       (react [msg]
         (let [result (cond-match
                       ;; Call
                       [[:server-call ?pid ?from ?request] msg]  (do-server-call server from request state)
                       ;; Cast
                       [[:server-cast ?request] msg]             (do-server-cast server request state)
                       ;; Terminate                               
                       [:terminate msg]                          (do (terminate server state) :norecur)
                       ;; Info                                    
                       [_ msg]                                   (do-handle-info server msg state))]
           (when (not= result :norecur)
             (react-recur (second result))))))))

;; Clients API


(defn start
  "Starts a new Server behaviour"
  ([server initial-msg]
     (let [server (eval-class server)
             pid (spawn #(server-actor initial-msg server))]
       pid))
  ([name server initial-msg]
     (let [pid (start server initial-msg)]
       (register-name name pid) pid)))


(defn start-evented
  "Starts a new evented server behaviour"
  ([server initial-msg]
     (let [server (eval-class server)
           pid (spawn-evented #(server-evented-actor initial-msg server))]
       pid))
  ([name server initial-msg]
     (let [pid (start-evented server initial-msg)]
       (register-name name pid))))

(defn send-call!
  "Sends a synchronous request to a server"
  ([pid msg]
     (send! pid [:server-call pid (self) msg])
     (second (receive #(= (first %) pid))))
  ([pid msg f]
     (send! pid [:server-call pid (self) msg])
     (react [m] #(= (first %) pid) (f (second m)))))

(defn send-cast!
  "Sends an asynchronous request to a server"
  ([pid msg]
     (send! pid [:server-cast msg])))
