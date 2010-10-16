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

(defn eval-class
  ([class-or-string]
     (let [cls (if (string? class-or-string) (Class/forName class-or-string) class-or-string)]
       (if (instance? Class cls)
         (.newInstance (aget (.getConstructors cls) 0) (make-array java.lang.Object 0))
         cls))))
;     (if (string? class-or-string)
;       (eval (read-string (str "(" class-or-string ".)")))
;       class-or-string)))

(defmacro instantiate
  ([class]
     `(new ~class))
  ([class & args]
     `(new ~class ~@args)))

(defn num-processors
  ([] (.. (Runtime/getRuntime) availableProcessors)))

(defn in?
  "Checks if an element is included in a collection"
  ([coll v] (not (nil? (some #(= v %) coll))))
  ([coll pred v] (not (nil? (some #(pred v %) coll)))))

; java.util.TimerTask

(defn make-timer
  ([] (java.util.Timer.)))

(defn make-timer-task
  ([f]
     (proxy [java.util.TimerTask] []
       (run [] (f)))))

(defn with-timer
  ([t period f]
     (.schedule t (make-timer-task f) (long 0) (long period)) t)
  ([period f]
     (let [t (make-timer)]
       (with-timer t period f))))

(defn cancel-timer
  ([t] (.cancel t)))
