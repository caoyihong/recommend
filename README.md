## hadoop

    sbin/start-dfs.sh
    
    

## spark

    ./sbin/start-master.sh
    ./sbin/start-slave.sh spark://localhost:7077


## kafka

启动zk
    
    bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

    
启动单机服务
    
    bin/kafka-server-start.sh config/server.properties
    
创建一个主题

    bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test
    
查看主题列表
    
    bin/kafka-topics.sh --list --bootstrap-server localhost:9092