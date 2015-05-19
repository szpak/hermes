package pl.allegro.tech.hermes.common.metric;

import static pl.allegro.tech.hermes.common.metric.PathsCompiler.$GROUP;
import static pl.allegro.tech.hermes.common.metric.PathsCompiler.$HOSTNAME;
import static pl.allegro.tech.hermes.common.metric.PathsCompiler.$HTTP_CODE;
import static pl.allegro.tech.hermes.common.metric.PathsCompiler.$TOPIC;

public class Meters {
    public static final String PRODUCER_METER = "producer." + $HOSTNAME + ".meter",
            PRODUCER_FAILED_METER = "producer." + $HOSTNAME + ".failed-meter",
            PRODUCER_STATUS_CODES = "producer." + $HOSTNAME + ".http-status-codes.codes" + $HTTP_CODE,
            PRODUCER_TOPIC_STATUS_CODES = "producer." + $HOSTNAME + ".http-status-codes." + $GROUP + "." + $TOPIC + ".code" + $HTTP_CODE,
            CONSUMER_METER = "consumer." + $HOSTNAME + ".meter",
            CONSUMER_FAILED_METER = "consumer." + $HOSTNAME + ".failed-meter",
            CONSUMER_DISCARDED_METER = "consumer." + $HOSTNAME + ".discarded-meter";
}
