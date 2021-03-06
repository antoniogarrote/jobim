(ns jobim.services.serialization.java
  (use [jobim.definitions]))

(defn java-encode
  ([^java.lang.Object obj] (let [^java.io.ByteArrayOutputStream bos (java.io.ByteArrayOutputStream.)
                                 ^java.io.ObjectOutputStream oos (java.io.ObjectOutputStream. bos)]
                             (.writeObject oos obj)
                             (.close oos)
                             (.toByteArray bos))))

(defn java-decode
  ([^java.lang.Object  bytes] (let [^java.io.ByteArrayOutputStream bis (java.io.ByteArrayInputStream. (if (string? bytes)
                                                                                                        (let [^String str bytes] (.getBytes str))
                                                                                                        (let [^bytes bs bytes] bs)))
                                    ^java.io.ObjectInputStream ois (java.io.ObjectInputStream. bis)
                                    ^java.lang.Object obj (.readObject ois)]
                                (.close ois)
                               obj)))

(deftype JavaSerializationService [] jobim.definitions.SerializationService
  (encode [this msg] (java-encode msg))
  (decode [this encoded-msg] (java-decode encoded-msg)))


(defmethod make-serialization-service :java
  ([kind configuration]
     (JavaSerializationService.)))
