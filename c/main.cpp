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
}
