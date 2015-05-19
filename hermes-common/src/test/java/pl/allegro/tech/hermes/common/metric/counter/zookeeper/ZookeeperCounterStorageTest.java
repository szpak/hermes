package pl.allegro.tech.hermes.common.metric.counter.zookeeper;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.metric.Counters;
import pl.allegro.tech.hermes.common.metric.PathsCompiler;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionNotExistsException;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionRepository;
import pl.allegro.tech.hermes.infrastructure.zookeeper.counter.DistributedEphemeralCounter;
import pl.allegro.tech.hermes.infrastructure.zookeeper.counter.SharedCounter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ZookeeperCounterStorageTest {

    @Mock
    private SharedCounter sharedCounter;

    @Mock
    private DistributedEphemeralCounter ephemeralCounter;

    @Mock
    private ConfigFactory configFactory;

    @Mock
    private SubscriptionRepository subscriptionRepository;

    private ZookeeperCounterStorage storage;

    private PathsCompiler pathCompiler;

    @Before
    public void initialize() {
        when(configFactory.getStringProperty(Configs.ZOOKEEPER_ROOT)).thenReturn("/hermes");
        pathCompiler = new PathsCompiler("localhost");
        storage = new ZookeeperCounterStorage(sharedCounter, ephemeralCounter, configFactory, subscriptionRepository, pathCompiler);
    }

    @Test
    public void shouldIncrementTopicMetricUsingSharedCounter() {
        // given when
        storage.setTopicCounter(TopicName.fromQualifiedName("test.topic"), pathCompiler.compile(Counters.PRODUCER_PUBLISHED), 10);

        // then
        verify(sharedCounter).increment("/hermes/groups/test/topics/topic/metrics/published", 10);
    }

    @Test
    public void shouldReadValueFromTopicMetric() {
        // given
        when(sharedCounter.getValue("/hermes/groups/test/topics/topic/metrics/published")).thenReturn(10L);

        // when
        long value = storage.getTopicCounter(TopicName.fromQualifiedName("test.topic"), pathCompiler.compile(Counters.PRODUCER_PUBLISHED));

        // then
        assertThat(value).isEqualTo(10);
    }

    @Test
    public void shouldIncrementSubscriptionMetricUsingSharedCounter() {
        // given when
        storage.setSubscriptionCounter(TopicName.fromQualifiedName("test.topic"), "sub", pathCompiler.compile(Counters.CONSUMER_DELIVERED), 10);

        // then
        verify(sharedCounter).increment("/hermes/groups/test/topics/topic/subscriptions/sub/metrics/delivered", 10);
    }

    @Test
    public void shouldReadValueFromSubscriptionMetric() {
        // given
        when(sharedCounter.getValue("/hermes/groups/test/topics/topic/subscriptions/sub/metrics/delivered")).thenReturn(10L);

        // when
        long value = storage.getSubscriptionCounter(TopicName.fromQualifiedName("test.topic"), "sub", pathCompiler.compile(Counters.CONSUMER_DELIVERED));

        // then
        assertThat(value).isEqualTo(10);
    }

    @Test
    public void shouldIncrementInflightMetricUsingDistirbutedCounter() {
        // given when
        storage.setInflightCounter("host", TopicName.fromQualifiedName("test.topic"), "sub", 10);

        // then
        verify(ephemeralCounter).setCounterValue("/hermes/consumers/host/groups/test/topics/topic/subscriptions/sub/metrics/inflight", 10);
    }

    @Test
    public void shouldReadValueFromInflightMetric() {
        // given
        when(ephemeralCounter.getValue("/hermes/consumers", "/groups/test/topics/topic/subscriptions/sub/metrics/inflight"))
            .thenReturn(10L);

        // when
        long value = storage.getInflightCounter(TopicName.fromQualifiedName("test.topic"), "sub");

        // then
        assertThat(value).isEqualTo(10);
    }

    @Test
    public void shouldNotIncrementSharedCounterForNonExistingSubscription() {
        //given
        TopicName topicName = TopicName.fromQualifiedName("test.topic");
        String subscriptionName = "sub";
        doThrow(new SubscriptionNotExistsException(topicName, subscriptionName))
            .when(subscriptionRepository).ensureSubscriptionExists(topicName, subscriptionName);

        //when
        storage.setSubscriptionCounter(topicName, subscriptionName, pathCompiler.compile(Counters.CONSUMER_DELIVERED), 1L);

        //then
        verifyZeroInteractions(sharedCounter);
    }
}
