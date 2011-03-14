(defproject jobim-core "0.0.8-SNAPSHOT"
  :description "Actors library for Clojure"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 [org.apache.zookeeper/zookeeper "3.3.3" :exclusions [log4j/log4j]]
                 [matchure "0.9.1"]
                 [aleph "0.1.5-SNAPSHOT"]]
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]
                     [cdt "1.2"]]
  :aot :all)
