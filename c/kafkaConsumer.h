#include <string>
#include "rdkafkacpp.h"
#include <vector>
#include <iostream>


class ConsumerEventCb : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event &event) override {
        switch (event.type()) {
        case RdKafka::Event::EVENT_ERROR:
            std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " << event.str() << std::endl;
            break;
        case RdKafka::Event::EVENT_STATS:
            std::cout << "STATS: " << event.str() << std::endl;
            break;
        case RdKafka::Event::EVENT_LOG:
            std::cout << "LOG-" << event.severity() << "-" << event.fac() << ": " << event.str() << std::endl;
            break;
        default:
            std::cout << "EVENT " << event.type() << ": " << event.str() << std::endl;
            break;
        }
    }
};

class ConsumerRebalanceCb : public RdKafka::RebalanceCb {
private:
    static void printTopicPartition(const std::vector<RdKafka::TopicPartition *> &partitions){
        for (unsigned int i = 0; i < partitions.size(); i++){
            std::cerr << partitions[i]->topic() << "["
                      << partitions[i]->partition() << "], ";
        }
        std::cerr << "\n";
    }

public:
    void rebalance_cb(RdKafka::KafkaConsumer *consumer, RdKafka::ErrorCode err,
                      std::vector<RdKafka::TopicPartition *> &partitions) {
        std::cerr << "RebalanceCb" << RdKafka::err2str(err) << ":";
        printTopicPartition(partitions);
        if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
            consumer->assign(partitions);
            partition_count = (int)partitions.size();
        } else {
            consumer->unassign();
            partition_count = 0;
        }
    }
private:
    int partition_count;
}; 

class KafkaConsumer {
public:
    KafkaConsumer(const std::string &brokers,
                  const std::string &groupID,
                  const std::vector<std::string> &topics,
                  int partition);
    ~KafkaConsumer();
    void pullMessage();
private:
    std::string m_brokers;
    std::string m_groupID;
    std::vector<std::string> m_topicvector;
    int m_partition;             
    
    RdKafka::Conf *m_topicConfig = nullptr; 
    RdKafka::Conf *m_config = nullptr;      
    RdKafka::KafkaConsumer *m_consumer = nullptr;
   
    ConsumerEventCb *m_event_cb = nullptr;         // 事件回调 (错误、日志等)
    ConsumerRebalanceCb *m_rebalance_cb = nullptr; // 再平衡回调 (当消费者组发生变化时)    
};
