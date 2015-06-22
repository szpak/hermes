package pl.allegro.tech.hermes.test.helper.endpoint;

import pl.allegro.tech.hermes.api.Group;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.api.TopicName;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.allegro.tech.hermes.api.EndpointAddress.of;
import static pl.allegro.tech.hermes.api.Subscription.Builder.subscription;
import static pl.allegro.tech.hermes.api.SubscriptionPolicy.Builder.subscriptionPolicy;
import static pl.allegro.tech.hermes.api.Topic.Builder.topic;

public class HermesAPIOperations {

    private final HermesEndpoints endpoints;

    public HermesAPIOperations(HermesEndpoints endpoints) {
        this.endpoints = endpoints;
    }

    public void createGroup(String group) {
        if (!endpoints.group().list().contains(group)) {
            Response response = endpoints.group().create(Group.from(group));
            assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
        }
    }

    public void createTopic(String group, String topic) {
        createTopic(topic().withName(group, topic).withRetentionTime(1000).withDescription("Test topic").build());
    }

    public void createTopic(Topic topic) {
        List<String> topicList = new ArrayList<>();
        topicList.addAll(endpoints.topic().list(topic.getName().getGroupName(), false));
        topicList.addAll(endpoints.topic().list(topic.getName().getGroupName(), true));

        if (!topicList.contains(topic.getQualifiedName())) {
            Response response = endpoints.topic().create(topic);

            assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
        }
    }

    public Response createSubscription(String group, String topic, String subscriptionName, String endpoint) {
        Subscription subscription = subscription()
                .applyDefaults()
                .withName(subscriptionName)
                .withEndpoint(of(endpoint))
                .withSubscriptionPolicy(subscriptionPolicy().applyDefaults().build())
                .build();

        return createSubscription(group, topic, subscription);
    }

    public Response createSubscription(String group, String topic, Subscription subscription) {
        Response respone = endpoints.subscription().create(group + "." + topic, subscription);
        assertThat(respone.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
        return respone;
    }

    public void buildTopic(String group, String topic) {
        createGroup(group);
        createTopic(group, topic);
    }

    public void buildTopic(Topic topic) {
        createGroup(topic.getName().getGroupName());
        createTopic(topic);
    }

    public void buildSubscription(TopicName topicName, String subscription, String httpEndpointUrl) {
        buildSubscription(topicName.getGroupName(), topicName.getName(), subscription, httpEndpointUrl);
    }

    public void buildSubscription(String group, String topic, String subscription, String endpoint) {
        buildTopic(group, topic);
        createSubscription(group, topic, subscription, endpoint);
    }

    public void buildSubscription(TopicName topic, Subscription subscription) {
        buildTopic(topic.getGroupName(), topic.getName());
        createSubscription(topic.getGroupName(), topic.getName(), subscription);
    }

    public Response suspendSubscription(String group, String topic, String subscription) {
        return endpoints.subscription().updateState(group + "." + topic, subscription, Subscription.State.SUSPENDED);
    }

    public Response activateSubscription(String group, String topic, String subscription) {
        return endpoints.subscription().updateState(group + "." + topic, subscription, Subscription.State.ACTIVE);
    }

    public Response updateSubscription(String group, String topic, String subscription, Subscription updated) {
        return endpoints.subscription().update(group + "." + topic, subscription, updated);
    }

    public Topic getTopic(String group, String topic) {
        return endpoints.topic().get(group + "." + topic);
    }

    public Subscription getSubscription(String group, String topic, String subscription) {
        return endpoints.subscription().get(group + "." + topic, subscription);
    }

    public void updateTopic(TopicName topicName, Topic updated) {
        endpoints.topic().update(topicName.qualifiedName(), updated);
    }
}
