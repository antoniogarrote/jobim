(ns jobim.services.serialization.jboss
  (use [jobim.definitions]))

(defn java-encode
  ([^java.lang.Object obj] (let [^java.io.ByteArrayOutputStream bos (java.io.ByteArrayOutputStream.)
                                 ^org.jboss.serial.io.JBossObjectOutputStream oos (org.jboss.serial.io.JBossObjectOutputStream. bos)]
                             (.writeObject oos obj)
                             (.close oos)
                             (.toByteArray bos))))

(defn java-decode
  ([^java.lang.Object  bytes] (let [^java.io.ByteArrayOutputStream bis (java.io.ByteArrayInputStream. (if (string? bytes)
                                                                                                        (let [^String str bytes] (.getBytes str))
                                                                                                        (let [^bytes bs bytes] bs)))
                                    ^org.jboss.serial.io.JBossObjectInputStream ois (org.jboss.serial.io.JBossObjectInputStream. bis)
                                    ^java.lang.Object obj (.readObject ois)]
                                (.close ois)
                               obj)))

(deftype JBossSerializationService [] jobim.definitions.SerializationService
  (encode [this msg] (java-encode msg))
  (decode [this encoded-msg] (java-decode encoded-msg)))


(defmethod make-serialization-service :jboss
  ([kind configuration]
     (JBossSerializationService.)))
