#include "KafkaConsumer.h"

KafkaConsumer::KafkaConsumer(const std::string &brokers,
                             const std::string &groupID,
                             const std::vector<std::string> &topics,
                             int partition){
    m_brokers = brokers;
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
    
    errCode = m_topicConfig->set("auto.offset.reset", "latest", errStr);
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cout << "Fail to set topicConfig.auto.offset.reset" << errStr << std::endl;
    }

    errCode = m_config->set("default_topic_conf", m_topicConfig, errStr);
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cout << "Fail to set topicConfig.default_topic_conf" << errStr << std::endl;
    }

    m_consumer = RdKafka::KafkaConsumer::create(m_config, errStr);
    if (m_consumer == NULL) {
        std::cout << "Fail to create Kafkaconsumer: " << errStr << std::endl;
    }

    std::cout << "Create KafkaConsumer " << m_consumer->name() << std::endl;
}

void msg_consume(RdKafka::Message *msg, void *opaque) {
    switch(msg->err()) {
    case RdKafka::ERR__TIMED_OUT:
        // std::cerr << "Consumer error: " << msg->errstr() << std::endl;
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

void KafkaConsumer::pullMessage() {
    RdKafka::ErrorCode errCode = m_consumer->subscribe(m_topicvector);
    if (errCode != RdKafka::ERR_NO_ERROR) {
        std::cout << "Subscribe failed: " << RdKafka::err2str(errCode)
                  << std::endl;
    }
    while (true) {
        RdKafka::Message * msg = m_consumer->consume(1000);
        msg_consume(msg, NULL);
        delete msg;
    }
}

KafkaConsumer::~KafkaConsumer() {
    m_consumer->close();
    delete m_config;
    delete m_topicConfig;
    delete m_consumer;
    delete m_event_cb;
    delete m_rebalance_cb;
}
