package pl.allegro.tech.hermes.common.metric.rate;

import pl.allegro.tech.hermes.api.SubscriptionMetrics;
import pl.allegro.tech.hermes.api.TopicMetrics;
import pl.allegro.tech.hermes.api.TopicName;

public interface RateStorage {
    TopicMetrics getTopicRates(TopicName topicName);
    SubscriptionMetrics getSubscriptionRates(TopicName topicName, String subscriptionName);
}
