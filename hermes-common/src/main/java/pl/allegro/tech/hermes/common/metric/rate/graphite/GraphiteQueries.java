package pl.allegro.tech.hermes.common.metric.rate.graphite;

import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.common.metric.Meters;
import pl.allegro.tech.hermes.common.metric.PathsCompiler;
import pl.allegro.tech.hermes.common.metric.rate.Rate;

import static java.lang.String.format;
import static pl.allegro.tech.hermes.common.metric.HermesMetrics.escapeDots;

public class GraphiteQueries {

    public static final String TOPIC_RATE_PATTERN          = "sumSeries(%s.producer.*.meter.%s.%s.m1_rate)";
    public static final String TOPIC_DELIVERY_RATE_PATTERN = "sumSeries(%s.consumer.*.meter.%s.%s.m1_rate)";
    public static final String SUBSCRIPTION_RATE_PATTERN   = "sumSeries(%s.consumer.*.%s.%s.%s.%s.%s)";

    private final String prefix;
    private final PathsCompiler pathsCompiler;

    public GraphiteQueries(String graphitePrefix, PathsCompiler pathsCompiler) {
        this.pathsCompiler = pathsCompiler;
        this.prefix = graphitePrefix;
    }

    public String getTopicRateQuery(TopicName topic) {
        return format(TOPIC_RATE_PATTERN, prefix, escapeDots(topic.getGroupName()), escapeDots(topic.getName()));
    }

    public String getTopicRateDeliveryQuery(TopicName topic) {
        return format(TOPIC_DELIVERY_RATE_PATTERN, prefix, escapeDots(topic.getGroupName()), escapeDots(topic.getName()));
    }

    public String getSubscriptionRateQuery(TopicName topicName, String subscriptionName) {
        return format(
                SUBSCRIPTION_RATE_PATTERN,
                prefix,
                pathsCompiler.compile(Meters.CONSUMER_METER),
                escapeDots(topicName.getGroupName()),
                escapeDots(topicName.getName()),
                escapeDots(subscriptionName),
                Rate.MINUTES_1.toString()
        );
    }
}
