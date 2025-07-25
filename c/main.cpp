#include "kafkaConsumer.h"

int main(int argc, char *argv[]) {
    std::string brokers;
    std::string groupid
    char **temp_topics;
    std::vector<std::string> topics;
    int topic_cnt;

    if (argc < 4) {
        fprintf(stderr, "%% Usage: %s <broker><group.id> <topic1> <topic2> ..\n",
                argv[0]);
        return 1;
    }

    brokers = argv[1];
    groupid = argv[2];
    temp_topics = &srgv[3];
    topic_cnt = argc - 3;

    for (int i = 0; i < topic_cnt; i++) {
        topics.push_back(temp_topics[i]);
    }
}
