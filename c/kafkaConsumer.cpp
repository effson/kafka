#include "KafkaConsumer.h"

KafkaConsumer::KafkaConsumer(const std::string &brokers
                             const std::string &groupID
                             const std::vector<std::string> &topics,
                             int partition){
    m_brokers = broker;
    m_groupID = groupID;
    m_topicvector = topics;
    m_partition = partition;
                               
    std::string errStr;
    RdKafka::Conf::ConfResult errCode;
    m_config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    m_event_cb = new ConsumerEventCb;
    errCode = m_config->set("event_cb".m_event_cb, errStr);
    if (errCode != RdKafka::Conf::CONF_OK) {
      std::cout << "Fail to set conf.event_cb" << errStr << std::endl;
    }
                               
    m_rebalance_cb = new ConsumerRebalanceCb;
    errCode = m_config->set("rebalance_cb".m_rebalance_cb, errStr);
    if (errCode != RdKafka::Conf::CONF_OK) {
      std::cout << "Fail to set conf.rebalance_cb" << errStr << std::endl;
    }
                               
}
