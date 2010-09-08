;; Default configuration for a node


;{:node-name "remote-test"
; :messaging-type :zeromq
; :messaging-options {:protocol-and-port "tcp://192.168.1.35:5555"}
; :zookeeper-options ["localhost:2181" {:timeout 3000}]}

{:node-name "remote-test"
 :messaging-type :rabbitmq
 :messaging-options {:host "172.21.1.237"}
 :zookeeper-options ["172.21.1.237:2181" {:timeout 3000}]}
