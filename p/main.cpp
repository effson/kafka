#include "KafkaProducer.h"
#include <iostream>
using namespace std;

int main(int argc, char *argv[]) {
    std::string brokers;
    std::string topic;

    if (argc != 3) {
        fprintf(stderr, "Usage: %s <brokers> <topic>\n", argv[0]);
        return 1;
    }

    brokers = argv[1];
    topic = argv[2];
    KafkaProducer producer(brokers, topic, 0);
    for (int i = 0; i < 10000; ++i) {
        char msg[64] = {0};
        sprintf(msg, "%s%4d", "Hello RdKafka sh kafka-topic.sh msg", i);
        char key[8] = {0};
        sprintf(key, "%d", i);
        producer.pushMessage(msg, key);
    }
    printf("wait destroyed\n");
    RdKafka::wait_destroyed(5000);
    printf("end\n");
}
