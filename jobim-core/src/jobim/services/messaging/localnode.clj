(ns jobim.services.messaging.localnode
  (:use [jobim.definitions])
  (:use [lamina.core]))

(deftype LocalNodeMessagingService [queue coordination-service serialization-service] MessagingService
  (connect-messaging [this] :ignore)
  (publish [this msg]
           ;(.put @queue msg)
           (enqueue @queue msg))
  (set-messages-queue [this new-queue]
                      (swap! queue (fn [q] new-queue))))

(defmethod make-messaging-service :local
  ([kind configuration coordination-service serialization-service]
     (LocalNodeMessagingService. (atom nil) coordination-service serialization-service)))
