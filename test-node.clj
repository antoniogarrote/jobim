;; Default configuration for a node

;{:node-name "linux"
; :rabbit-options [:host "172.21.1.238"]
; :zookeeper-options ["172.21.1.238:2181" {:timeout 3000}]}


{:node-name "remote-test"
 :rabbit-options [:host "192.168.1.35"]
 :zookeeper-options ["192.168.1.35:2181" {:timeout 3000}]}
