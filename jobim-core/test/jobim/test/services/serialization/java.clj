(ns jobim.test.services.serialization.java
  (:use [jobim.definitions])
  (:use [jobim.services.serialization.java] :reload)
  (:use [clojure.test]))

(deftest java-encoding-decoding-test
  (let [serv (make-serialization-service :java {})
        orig (java.util.Date.)]
    (is (= (str orig) (str (decode serv (encode serv orig)))))))
