(ns jobim.services.serialization.json
  (use [jobim.definitions]
       [clojure.contrib.json]))


(defn json-encode
  ([msg]
     (.getBytes (json-str msg))))

(defn json-decode
  ([msg]
     (read-json (if (string? msg) msg (String. msg)))))


(deftype JsonSerializationService [] jobim.definitions.SerializationService
  (encode [this msg] (json-encode msg))
  (decode [this encoded-msg] (json-decode encoded-msg)))


(defmethod make-serialization-service :json
  ([kind configuration]
     (JsonSerializationService.)))
