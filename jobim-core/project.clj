(defproject jobim-core "0.1.3-SNAPSHOT"
  :description "Actors library for Clojure"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 [org.apache.zookeeper/zookeeper "3.3.2" :exclusions [log4j/log4j]]
                 [jboss/jboss-serialization "1.0.3.GA"]
                 [trove/trove "1.0.2"]
                 [matchure "0.9.1"]
                 [aleph "0.1.5-SNAPSHOT"]]
  :repositories {"apache" "https://repository.apache.org/content/groups/public"
                 "repository.jboss.org" "http://repository.jboss.org/nexus/content/groups/public"}
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]
                     [cdt "1.2"]]
;  :jvm-opts ["-Xmn3072m -Xmx3072m -Xincgc"]
;  :jvm-opts ["-Xmn3072m -Xmx3072m"]
  :aot :all)
