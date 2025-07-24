#include "KafkaProducer.h"

KafkaProducer::KafkaProducer(const std::string &brokers, const std::string &topic, int partition) {
    m_brokers = brokers;
    m_topicStr = topic;
    m_partition = partition;

    m_config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (m_config == NULL) {
        std::cout << "Create RdKafka conf failed." << std::endl;
    }

    m_topicConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    if (m_topicConfig == NULL) {
        std::cout << "Create RdKafka topic conf failed." << std::endl;
    }

    RdKafka::Conf::ConfResult errCode;
    m_dr_cb = new ProducerDeliveryRecordCb;
    std::string errstr;
    errCode = m_config->set("dr_cb", m_dr_cb, errstr);
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set conf " << errstr << std::endl;
    }

    m_event_cb = new ProducerEventCb;
    errCode = m_config->set("event_cb", m_event_cb, errstr);
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set conf " << errstr << std::endl;
    }   

    m_partitioner_cb = new HashPartitionerCb;
    errCode = m_topicConfig->set("partitioner_cb", m_partitioner_cb, errstr);
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set conf " << errstr << std::endl;
    }

    errCode = m_config->set("statistics.interval.ms", "10000", errstr);
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set conf " << errstr << std::endl;
    }

    errCode = m_config->set("message.max.bytes", "10240000", errstr);
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set conf " << errstr << std::endl;
    }

    errCode = m_config->set("bootstrap.servers", m_brokers, errstr);
    if (errCode != RdKafka::Conf::CONF_OK) {
        std::cerr << "Failed to set conf " << errstr << std::endl;
    }

    m_producer = RdKafka::Producer::create(m_config, errstr);
    if (m_producer == NULL) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
    }

    m_topic = RdKafka::Topic::create(m_producer, m_topicStr, m_topicConfig, errstr);
    if (m_topic == NULL) {
        std::cerr << "Failed to create topic: " << errstr << std::endl;
    }
}

void KafkaProducer::pushMessage(const std::string &str, const std::string &key) {
    int32_t len = str.length();
    void *payload = const_cast<void *>(static_cast<const void *>(str.data()));

    RdKafka::ErrorCode errCode = m_producer->produce(
        m_topic,
        RdKafka::Topic::PARTITION_UA, // Use unassigned partition
        RdKafka::Producer::RK_MSG_COPY, // Copy the payload
        payload, len, // Payload and its length
        &key, // Key for partitioning
        NULL // Headers (not used here)
    );
    m_producer->poll(0); // Poll to handle delivery reports
    if (errCode != RdKafka::ErrorCode::ERR_NO_ERROR) {
        std::cerr << "Produce failed: "
                  << RdKafka::err2str(errCode) << std::endl;
        if (errCode == RdKafka::ErrorCode::ERR__QUEUE_FULL) {
            m_producer->poll(100); // Wait for queue to drain
        }
    }
}

KafkaProducer::~KafkaProducer() {
    while (m_producer->outq_len() > 0) {
        std::cout << "Waiting for" << m_producer->outq_len() << std::endl;
        m_producer->flush(5000); // Poll to ensure all messages are sent
    }
    delete m_config;
    delete m_topicConfig;
    delete m_topic;
    delete m_producer;
    delete m_dr_cb;
    delete m_event_cb;
    delete m_partitioner_cb;
}
