#include "KafkaConsumer.h"

KafkaConsumer::KafkaConsumer(const std::string &brokers
                             const std::string &groupID
                             const std::vector<std::string> &topics,
                             int partition){
    m_brokers = broker;
    m_groupID = groupID;
    m_topicvector = topics;
    m_partition = partition;
                               
    std::string errorStr;
    RdKafka::Conf::ConfResult errCode;
    m_config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    
                               
}
