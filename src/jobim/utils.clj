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

(defmacro require-apply
  ([function-path args]
     `(let [[namespace-symbol# function-symbol#] (vec (->> (.split ~function-path "/") vec (map symbol)))
            alias# (gensym (str namespace-symbol#))
            alias-path# (symbol (str alias# "/" function-symbol#))]
        (do (require [namespace-symbol# :as alias#])
            (eval (read-string (str "(apply " alias-path# " " ~args ")")))))))

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
  ([timer period f]
     (.schedule timer (make-timer-task f) (long period) (long period)) timer)
  ([period f]
     (let [timer (make-timer)]
       (with-timer timer period f))))

(defn cancel-timer
  ([t] (.cancel t)))


;; apply with timeout

(defn testable-promise
  "Alpha - subject to change.
Returns a promise object that can be read with deref/@, and set
once only, with deliver. Calls to deref/@ prior to delivery will
block. All subsequent derefs will return the same delivered value
without blocking."
  {:added "1.1"}
  []
  (let [d (java.util.concurrent.CountDownLatch. 1)
        v (atom nil)]
    ^{:delivered?
      (fn []
        (locking d
          (zero? (.getCount d))))}
    (reify
     clojure.lang.IDeref
      (deref [_] (.await d) @v)
     clojure.lang.IFn
      (invoke [this x]
        (locking d
          (if (pos? (.getCount d))
            (do (reset! v x)
                (.countDown d)
                this)
            (throw (IllegalStateException. "Multiple deliver calls to a promise"))))))))

(defn testable-promise-delivered?
  "Alpha - subject to change.
Returns true if promise has been delivered, else false"
  [p]
  (if-let [f (:delivered? (meta p))]
    (f)))

(defn testable-promise-deliver
  [p v] (.invoke p v))

(defn apply-with-timeout
  ([f args timeout]
     (let [to-return (testable-promise)
           timer (make-timer)]
       (future (let [from-function (try {:success (apply f args)}
                                        (catch Exception ex
                                          {:exception ex}))]
                 (when-not (testable-promise-delivered? to-return)
                   (testable-promise-deliver to-return from-function))))
       (with-timer timer timeout
         #(do
            (when-not (testable-promise-delivered? to-return)
              (testable-promise-deliver to-return :timeout))
            (cancel-timer timer)))
       (let [result @to-return]
         (if (= :timeout result) :timeout
             (if (nil? (:exception result))
               (:success result)
               (throw (:exception result))))))))
