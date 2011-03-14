(ns jobim.examples.server
  (:use [jobim]
        [jobim.behaviours.server]
        [matchure]))

(defn- alloc-server
  ([channels c]
     (if (empty? channels)
       [(str "channel-" (inc c)) channels (inc c)]
       [(first channels) (rest channels) c])))

(def-server ChannelsManager
  (init [this _] [[] 0])

  (handle-call [this request from [channels c]]
               (if (= request :alloc)
                 (let [[ch rest-channels next-c] (alloc-server channels c)]
                   (reply ch [rest-channels next-c]))
                 (reply :error channels)))

  (handle-cast [this request [channels c]]
               (cond-match
                [[:free ?ch] request] (noreply [(conj channels ch) c])
                [_ request]           (noreply [channels c])))

  (handle-info [this request [channels c]]
               (do (cond-match
                    [[:available ?from] request] (do (send! from (count channels))
                                                     (noreply [channels c]))))))

;; public interface

(defn make-channel-manager
  "Makes a new channel manager"
  ([] (start (jobim.examples.server.ChannelsManager.) nil)))

(defn make-channel-manager-evented
  "Makes an evented channel manager"
  ([] (start-evented (jobim.examples.server.ChannelsManager.) nil)))

(defn alloc
  "Allocs a new channel"
  ([channels-manager] (send-call! channels-manager :alloc)))

(defn free
  "Frees an allocated channel"
  ([channels-manager ch]
     (send-cast! channels-manager [:free ch])))

(defn available
  "Returns the number of free channels"
  ([channels-manager]
     (send! channels-manager [:available (self)])
     (receive)))
