(ns jobim.main
  (:use [jobim]
        [clojure.main :only [repl]])
  (:gen-class :main true))

(defn cmd-param-to-keyword
  "Transforms a command line argument (-something) into a keyword (:something)"
  ([atom]
     (if (keyword? atom)
       atom
       (if (.startsWith atom "-") (keyword (.substring atom 1)) atom))))

(defn cmd-params-to-hash
  ([args]
     (apply hash-map (map cmd-param-to-keyword args))))

(defn show-help []
  (println "syntax: java -cp app.jar jobim.main node-configuration-file.clj [benchmark (evented | not-evented) remote-node-name"))

(use 'jobim.examples.actors)
(defn -main
  [& args]
  (bootstrap-node (first args))
  (if (= (second args) "benchmark")
    (let [sink (spawn jobim.examples.actors/sink)]
      (spawn-in-repl)
      (loop [c 1]
        (time
         (do (println (str (self) " --- " c))
             (let [remote-pid (if (= (nth args 2) "evented")
                                (rpc-blocking-call (resolve-node-name (nth args 3)) "jobim/spawn-evented" ["jobim.examples.actors/ping-benchmarking-evented"])
                                (rpc-blocking-call (resolve-node-name (nth args 3)) "jobim/spawn" ["jobim.examples.actors/ping-benchmarking"]))]
               (send! remote-pid [sink c]))))
        (recur (inc c))))
    (repl)))
