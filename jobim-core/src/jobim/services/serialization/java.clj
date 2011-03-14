(ns jobim.services.serialization.java
  (use [jobim.definitions]))

(defn java-encode
  ([obj] (let [bos (java.io.ByteArrayOutputStream.)
               oos (java.io.ObjectOutputStream. bos)]
           (.writeObject oos obj)
           (.close oos)
           (.toByteArray bos))))

(defn java-decode
  ([bytes] (let [bis (java.io.ByteArrayInputStream. (if (string? bytes) (.getBytes bytes) bytes))
                 ois (java.io.ObjectInputStream. bis)
                 obj (.readObject ois)]
              (.close ois)
              obj)))

(deftype JavaSerializationService [] jobim.definitions.SerializationService
  (encode [this msg] (java-encode msg))
  (decode [this encoded-msg] (java-decode encoded-msg)))


(defmethod make-serialization-service :java
  ([kind configuration]
     (JavaSerializationService.)))
