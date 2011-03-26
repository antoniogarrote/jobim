(defproject jobim-rabbitmq "0.1.1-SNAPSHOT"
  :description "Communication layer plugin for the Jobim actors library using RabbitMQ"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [commons-cli/commons-cli "1.1"]
                 [commons-io/commons-io "1.2"]
                 [jobim-rabbitmq-deps "2.4.1-SNAPSHOT"]
                 [lamina "0.4.0-SNAPSHOT"]
                 [jobim-core "0.1.0-SNAPSHOT"]]
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]]
  :aot :all)
