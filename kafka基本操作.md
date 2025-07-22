# 1.创建topic
```bash
root@worker02:/home/jeff/kafka_2.13-3.9.1# bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test
Created topic test.
root@worker02:/home/jeff/kafka_2.13-3.9.1# bin/kafka-topics.sh --list --bootstrap-server localhost:9092
test
```
- replication-factor 制定了副本数量
- partitions 指定分区个数
