package pl.allegro.tech.hermes.common.metric;

import static pl.allegro.tech.hermes.common.metric.PathsCompiler.$GROUP;
import static pl.allegro.tech.hermes.common.metric.PathsCompiler.$HOSTNAME;
import static pl.allegro.tech.hermes.common.metric.PathsCompiler.$PARTITION;
import static pl.allegro.tech.hermes.common.metric.PathsCompiler.$SUBSCRIPTION;
import static pl.allegro.tech.hermes.common.metric.PathsCompiler.$TOPIC;

public class Counters {
    public static final String  PRODUCER_PUBLISHED = "producer." + $HOSTNAME + ".published",
            PRODUCER_UNPUBLISHED = "producer." + $HOSTNAME + ".unpublished",
            CONSUMER_DELIVERED = "consumer." + $HOSTNAME + ".delivered",
            CONSUMER_DISCARDED = "consumer." + $HOSTNAME + ".discarded",
            CONSUMER_INFLIGHT = "consumer." + $HOSTNAME + ".inflight",
            CONSUMER_OFFSET_COMMIT_IDLE = "consumer." + $HOSTNAME + ".offset-commit-idle",
            CONSUMER_OFFSET_LAG = "consumer.offset." + $GROUP + "." + $TOPIC + "." + $SUBSCRIPTION + "." + $PARTITION + ".lag";
}
