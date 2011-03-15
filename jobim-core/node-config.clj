;; Default configuration for a node

{:node-name "osx"
 :coordination-type :zookeeper
 :coordination-args ["localhost:2181" {:timeout 3000}]
 :messaging-type :tcp
 :messaging-args {:port "7777"}
 :serialization-type :java
 :serialization-args {}}
