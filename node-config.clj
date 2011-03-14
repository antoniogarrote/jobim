;; Default configuration for a node

;{:node-name "osx"
; :messaging-type :zeromq
; :messaging-options {:protocol-and-port "tcp://192.168.1.36:5555"}
; :zookeeper-options ["localhost:2181" {:timeout 5000}]}


{:node-name "osx"
 :messaging-type :rabbitmq
 :messaging-options {:host "localhost"}
 :zookeeper-options ["localhost:2181" {:timeout 3000}]}
