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
    errCode = m_config->set("event_cb", m_event_cb, errStr);
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cout << "Fail to set conf.event_cb" << errStr << std::endl;
    }
                               
    m_rebalance_cb = new ConsumerRebalanceCb;
    errCode = m_config->set("rebalance_cb", m_rebalance_cb, errStr);
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cout << "Fail to set conf.rebalance_cb" << errStr << std::endl;
    }
                               
    errCode = m_config->set("enable_partition.eof", "false", errStr); 
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cout << "Fail to set conf.enable_partition.eof" << errStr << std::endl;
    }    

    errCode = m_config->set("group.id", m_groupID, errStr); 
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cout << "Fail to set conf.group.id" << errStr << std::endl;
    }    

    errCode = m_config->set("bootstrap.servers", m_brokers, errStr); 
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cout << "Fail to set conf.m_brokers" << errStr << std::endl;
    }  

    errCode = m_config->set("max.partition.fetch.bytes", "1024000", errStr); 
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cout << "Fail to set conf.max.partition.fetch.bytes" << errStr << std::endl;
    } 

    errCode = m_config->set("partition.assignment.strategy", "range", errStr); 
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cout << "Fail to set conf.partition.assignment.strategy" << errStr << std::endl;
    } 

    m_topicConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    if (m_consumer == NUL) {
        std::cout << "Create consumer " << m_consumer->name() << std::endl;
    }                               
}

void msg_consume(RdKafka::Message *msg, void *opaque) {
    switch(msg->err()) {
      case RdKafka::ERR__TIMED_OUT:
          std::cerr << "Consumer error: " << msg->errstr() << std::endl;
          break;
      case RdKafka::ERR_NO_ERROR:
          std::cout << "Message in->topic: " << msg->topic_name()
                    << ", partition:[" << msg->partition() << "] at offset "
                    << msg->offset() << "key: " << msg->key()
                    << "payload: " << (char *)msg->payload() << std::endl;
          break;
      default:
          std::cerr << "Consumer error: " << msg->errstr() << std::endl;
          break;
    }
}
