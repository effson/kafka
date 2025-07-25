#include <string>
#include <vector>
#include <iostream>

class KafkaConsumer {
public:
    KafkaConsumer(const std::string &brokers,
                  const std::string &groupID,
                  const std::vector<std::string> &topics,
                  int partition);
    ~KafkaConsumer();

private:
    std::string m_brokers;
    std::string m_groupID;
    std::vector<std::string> m_topicvector;
    int m_partition;             
    
    RdKafka::Conf *m_config = nullptr;       
    RdKafka::Consumer *m_consumer = nullptr;
   
    ConsumerEventCb *m_event_cb = nullptr;         // 事件回调 (错误、日志等)
    ConsumerRebalanceCb *m_rebalance_cb = nullptr; // 再平衡回调 (当消费者组发生变化时)    
};
