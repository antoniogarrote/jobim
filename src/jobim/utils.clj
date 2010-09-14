(ns jobim.utils)

(defn localhost
  ([]
     (str (java.net.InetAddress/getLocalHost))))

(defn show-java-methods
  ([obj]
     (let [ms (.. obj getClass getDeclaredMethods)
           max (alength ms)]
       (loop [count 0]
         (when (< count max)
           (let [m (aget ms count)
                 params (.getParameterTypes m)
                 params-max (alength params)
                 return-type (.getReturnType m)
                 to-show (str (loop [acum (str (.getName m) "(")
                                     params-count 0]
                                (if (< params-count params-max)
                                  (recur (str acum " " (aget params params-count))
                                         (+ params-count 1))
                                  acum))
                              " ) : " return-type)]
             (println (str to-show))
             (recur (+ 1 count))))))))

(defn collect-java-implemented-interfaces
  ([obj]
     (let [is (.. obj getClass getInterfaces)
           max (alength is)]
       (loop [count 0
              acum []]
         (if (< count max)
           (recur (+ count 1)
                  (conj acum (aget is count)))
           acum)))))

(defn eval-ns-fn
  ([ns-fn]
     (if (= -1 (.indexOf ns-fn "/"))
       (eval (read-string ns-fn))
       (let [parts (vec (.split ns-fn "/"))
             ns-part (first parts)
             fn-part (second parts)]
;         (eval (read-string (str "(use ['" ns-part " :only '(" fn-part ")])")))))))
         (eval (read-string (str "(do (use '" ns-part ")" ns-fn ")")))))))

(defn eval-fn
  ([ns-fn]
     (eval (read-string ns-fn))))

(defn random-uuid
  ([] (.replace (str (java.util.UUID/randomUUID)) "-" "")))
