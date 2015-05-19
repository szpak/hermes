package pl.allegro.tech.hermes.common.metric.counter;

import pl.allegro.tech.hermes.api.TopicName;

public interface CounterStorage {

    void setTopicCounter(TopicName topicName, String counter, long count);

    void setSubscriptionCounter(TopicName topicName, String subscriptionName, String counter, long count);

    void setInflightCounter(String hostname, TopicName topicName, String subscriptionName, long count);

    long getTopicCounter(TopicName topicName, String counter);

    long getSubscriptionCounter(TopicName topicName, String subscriptionName, String counter);

    long getInflightCounter(TopicName topicName, String subscriptionName);

    int countInflightNodes(TopicName topicName, String subscriptionName);
}
