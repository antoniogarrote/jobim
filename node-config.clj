;; Default configuration for a node

{:node-name "osx"
 :messaging-type :zeromq
 :messaging-options {:protocol-and-port "tcp://192.168.1.35:5555"}
 :zookeeper-options ["localhost:2181" {:timeout 3000}]}


; {:node-name "linux"
;  :rabbit-options [:host "192.168.1.35"]
;  :zookeeper-options ["192.168.1.35:2181" {:timeout 3000}]}
