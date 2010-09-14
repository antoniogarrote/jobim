;; Default configuration for a node


{:node-name "remote-test"
 :messaging-type :zeromq
 :messaging-options {:protocol-and-port "tcp://192.168.1.36:5555"}
 :zookeeper-options ["localhost:2181" {:timeout 3000}]}

;{:node-name "remote-test"
; :messaging-type :rabbitmq
; :messaging-options {:host "192.168.1.36"}
; :zookeeper-options ["192.168.1.36:2181" {:timeout 3000}]}
