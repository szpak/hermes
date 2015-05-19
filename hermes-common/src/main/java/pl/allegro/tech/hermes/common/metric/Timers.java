package pl.allegro.tech.hermes.common.metric;

import static pl.allegro.tech.hermes.common.metric.PathsCompiler.$HOSTNAME;

public class Timers {

    public static final String  PRODUCER_BROKER_LATENCY = "producer." + $HOSTNAME + ".broker-latency",
            PRODUCER_PARSING_REQUEST = "producer." + $HOSTNAME + ".parsing-request",
            PRODUCER_ACK_ALL_LATENCY = "producer." + $HOSTNAME + ".ack-all.latency",
            PRODUCER_ACK_LEADER_LATENCY = "producer." + $HOSTNAME + ".ack-leader.latency",
            PRODUCER_TRACKER_COMMIT_LATENCY = "producer." + $HOSTNAME + ".tracker-commit-latency",
            PRODUCER_VALIDATION_LATENCY = "producer." + $HOSTNAME + ".validation-latency",
            CONSUMER_LATENCY = "consumer." + $HOSTNAME + ".latency",
            CONSUMER_READ_LATENCY = "consumer." + $HOSTNAME + ".read-latency",
            CONSUMER_TRACKER_COMMIT_LATENCY = "consumer." + $HOSTNAME + ".tracker-commit-latency";
}
