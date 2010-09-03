(defproject jobim "0.0.4-SNAPSHOT"
  :description "Actors library for Clojure built on top of Zookeeper and RabbitMQ"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 [matchure "0.9.1"]
                 [com.rabbitmq/amqp-client "2.0.0"]
                 [org.apache.zookeeper/zookeeper "3.3.1"]]
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]]
  :aot :all)
