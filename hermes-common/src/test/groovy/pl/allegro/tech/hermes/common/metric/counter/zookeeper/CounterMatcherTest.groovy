package pl.allegro.tech.hermes.common.metric.counter.zookeeper

import spock.lang.Specification

class CounterMatcherTest extends Specification {

    def "should match topic counter"() {
        given:
        def counterName = "producer.localhost.published.lagMetricGroup.topic"
        def counterMatcher = new CounterMatcher(counterName, "localhost")

        when:
        counterMatcher.matches()
        def isTopic = counterMatcher.isTopic()
        def topicName = counterMatcher.topicName

        then:
        isTopic
        topicName == "lagMetricGroup.topic"
    }

    def "should match subscription counter"() {
        given:
        def counterName = "consumer.localhost.delivered.lagMetricGroup.topic.subscription.0.lag"
        def counterMatcher = new CounterMatcher(counterName, "localhost")

        when:
        counterMatcher.matches()
        def isSubscription = counterMatcher.isSubscription()
        def topicName = counterMatcher.topicName
        def subscriptionName = counterMatcher.subscriptionName

        then:
        isSubscription
        topicName == "lagMetricGroup.topic"
        subscriptionName == "subscription"
    }

    def "should match inflight counter"() {
        given:
        def counterName = "consumer.localhost.inflight.group.topic.subscription"
        def counterMatcher = new CounterMatcher(counterName, "localhost")

        when:
        counterMatcher.matches()
        def isInflight = counterMatcher.isInflight()
        def topicName = counterMatcher.topicName
        def subscriptionName = counterMatcher.subscriptionName

        then:
        isInflight
        topicName == "group.topic"
        subscriptionName == "subscription"
    }

    def "should return hostname from counter"() {
        when:
        def hostname = "localhost"
        def counterMatcher = new CounterMatcher("consumer.localhost.inflight.group.topic.subscription", hostname)

        then:
        counterMatcher.matches()
        counterMatcher.hostname == hostname
    }
}
