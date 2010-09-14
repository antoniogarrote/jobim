(defproject jobim "0.0.6-SNAPSHOT"
  :description "Actors library for Clojure built on top of Zookeeper and RabbitMQ"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 [matchure "0.9.1"]
                 [com.rabbitmq/amqp-client "2.0.0"]
                 [org.clojars.mikejs/clojure-zmq "2.0.7-SNAPSHOT"]
                 [org.apache.zookeeper/zookeeper "3.3.1"]]
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]
                     [native-deps "1.0.4"]]
  :native-dependencies [[org.clojars.mikejs/jzmq-native-deps "2.0.7-SNAPSHOT"]
                        [org.clojars.mikejs/jzmq-macosx-native-deps "2.0.7-SNAPSHOT"]]
  :aot :all)
