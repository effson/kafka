#pragma once
#include "rdkafkacpp.h"
#include <iostream>
#include <cstring>


class ProducerDeliveryRecordCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message &message) {
        if (message.err()) {
            std::cerr << "Message delivery failed: " 
                      << message.errstr() << std::endl;
        } else {
            std::cout << "Message delivered to topic: " 
                      << message.topic_name() << "["
                      << message.partition() << "] at offset: " << message.offset() 
                      << std::endl;
        }
    }
};

class ProducerEventCb : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event &event) override {
        switch (event.type()) {
            case RdKafka::Event::EVENT_ERROR:
                std::cout << "RdKafka::Event::EVENT_ERROR: " 
                          << RdKafka::err2str(event.err()) << std::endl;
                break;
            case RdKafka::Event::EVENT_STATS:
                std::cout << "RdKafka::Event::EVENT_STATS: " << event.str() 
                          << std::endl;
                break;
            case RdKafka::Event::EVENT_LOG:
                std::cout << "RdKafka::Event::EVENT_LOG: " << event.fac() 
                          << std::endl;
                break;
            case RdKafka::Event::EVENT_THROTTLE:
                std::cout << "RdKafka::Event::EVENT_THROTTLE: " 
                          << event.broker_name() << std::endl;
                break;
        }
    }
};

class HashPartitionerCb : public RdKafka::PartitionerCb {
public:
    int32_t partitioner_cb(const RdKafka::Topic *topic, const std::string *key, 
                            int32_t partition_count, void *msg_opaque) {
        char msg[128] = {0};
        int32_t partition_id = generate_hash(key->c_str(), key->size()) % partition_count;

        sprintf(msg, "HashPartitionerCb:topic:[%s], key=[%s], partition_cnt=[%d],partition_id:[%d]",
                topic->name().c_str(), key->c_str(), partition_count, partition_id);

        return partition_id;
    }
private:
    static inline unsigned int generate_hash(const char *key, size_t key_len) {
        int32_t hash = 0;
        for (size_t i = 0; i < key_len; ++i) {
            hash = (hash * 31 + key[i]) % INT32_MAX; // Simple hash function
        }
        return hash;
    }
};

class KafkaProducer {
public:
    KafkaProducer(const std::string &brokers, const std::string &topic, int partition);
    ~KafkaProducer();

    void pushMessage(const std::string &str, const std::string &key);
private:
    std::string m_brokers;
    std::string m_topicStr;
    int m_partition;

    RdKafka::Conf *m_config = nullptr;
    RdKafka::Conf *m_topicConfig = nullptr;
    RdKafka::Producer *m_producer = nullptr;
    RdKafka::Topic *m_topic = nullptr;

    ProducerDeliveryRecordCb *m_dr_cb = nullptr;
    ProducerEventCb *m_event_cb = nullptr;
    HashPartitionerCb *m_partitioner_cb = nullptr;
};
