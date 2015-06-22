package pl.allegro.tech.hermes.integration.management;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.api.EndpointAddress;
import pl.allegro.tech.hermes.api.ErrorCode;
import pl.allegro.tech.hermes.integration.IntegrationTest;

import javax.ws.rs.core.Response;
import java.util.List;

import static pl.allegro.tech.hermes.api.Subscription.Builder.subscription;
import static pl.allegro.tech.hermes.api.Topic.Builder.topic;
import static pl.allegro.tech.hermes.integration.test.HermesAssertions.assertThat;

public class TopicManagementTest extends IntegrationTest {

    @Test
    public void shouldCreateTopic() {
        // given
        operations.createGroup("topicManagementTestCreateTopicGroup");

        // when
        Response response = management.topic().create(
                topic().withName("topicManagementTestCreateTopicGroup", "topic").applyDefaults().build());

        // then
        assertThat(response).hasStatus(Response.Status.CREATED);
        Assertions.assertThat(management.topic().get("topicManagementTestCreateTopicGroup.topic")).isNotNull();
    }

    @Test
    public void shouldListTopics() {
        // given
        operations.createGroup("topicManagementTestListTopicsGroup");
        operations.createTopic("topicManagementTestListTopicsGroup", "topic1");
        operations.createTopic("topicManagementTestListTopicsGroup", "topic2");

        // when then
        Assertions.assertThat(management.topic().list("topicManagementTestListTopicsGroup", false)).containsOnlyOnce(
                "topicManagementTestListTopicsGroup.topic1", "topicManagementTestListTopicsGroup.topic2");
    }

    @Test
    public void shouldRemoveTopic() {
        // given
        operations.createGroup("topicManagementTestRemoveTopicGroup");
        operations.createTopic("topicManagementTestRemoveTopicGroup", "topic");

        // when
        Response response = management.topic().remove("topicManagementTestRemoveTopicGroup.topic");

        // then
        assertThat(response).hasStatus(Response.Status.OK);
        Assertions.assertThat(management.topic().list("topicManagementTestRemoveTopicGroup", false)).isEmpty();
    }

    @Test
    public void shouldNotAllowOnDeletingTopicWithSubscriptions() {
        // given
        operations.createGroup("topicManagementTestRemoveNonemptyTopicGroup");
        operations.createTopic("topicManagementTestRemoveNonemptyTopicGroup", "topic");
        operations.createSubscription("topicManagementTestRemoveNonemptyTopicGroup", "topic",
                subscription().withName("subscription").withEndpoint(EndpointAddress.of("http://whatever.com")).applyDefaults().build());

        // when
        Response response = management.topic().remove("topicManagementTestRemoveNonemptyTopicGroup.topic");

        // then
        assertThat(response).hasStatus(Response.Status.FORBIDDEN).hasErrorCode(ErrorCode.TOPIC_NOT_EMPTY);
    }

    @Test
    public void shouldRecreateTopicAfterDeletion() {
        // given
        operations.createGroup("topicManagementTestRecreateTopicGroup");
        operations.createTopic("topicManagementTestRecreateTopicGroup", "topic");
        management.topic().remove("topicManagementTestRecreateTopicGroup.topic");

        wait.untilKafkaZookeeperNodeDeletion("/brokers/topics/topicManagementTestRecreateTopicGroup.topic");

        // when
        Response response = management.topic().create(
                topic().withName("topicManagementTestRecreateTopicGroup", "topic").applyDefaults().build());

        // then
        assertThat(response).hasStatus(Response.Status.CREATED);
        Assertions.assertThat(management.topic().get("topicManagementTestRecreateTopicGroup.topic")).isNotNull();
    }

    @Test
    public void shouldNotAllowOnCreatingSameTopicTwice() {
        // given
        operations.createGroup("topicManagementTestOverrideTopicGroup");
        operations.createTopic(topic().withName("topicManagementTestOverrideTopicGroup", "topic").build());

        // when
        Response response = management.topic().create(topic().withName("overrideTopicGroup", "topic").build());

        // then
        assertThat(response).hasStatus(Response.Status.BAD_REQUEST).hasErrorCode(ErrorCode.TOPIC_ALREADY_EXISTS);
    }

    @Test
    public void shouldReturnTopicsThatAreCurrentlyTracked() {
        // given
        operations.buildTopic(topic().withName("topicManagementTestTrackedGroup", "topic").withTrackingEnabled(true).build());
        operations.buildTopic(topic().withName("topicManagementTestUntrackedGroup", "topic").withTrackingEnabled(false).build());


        // when
        List<String> tracked = management.topic().list("", true);

        // then
        assertThat(tracked).contains("topicManagementTestTrackedGroup.topic").doesNotContain("topicManagementTestUntrackedGroup.topic");
    }

    @Test
    public void shouldReturnTopicsThatAreCurrentlyTrackedForGivenGroup() {
        // given
        operations.buildTopic(topic().withName("topicManagementTestMixedTrackedGroup", "trackedTopic").withTrackingEnabled(true).build());
        operations.buildTopic(topic().withName("topicManagementTestMixedTrackedGroup", "untrackedTopic").withTrackingEnabled(false).build());

        // when
        List<String> tracked = management.topic().list("topicManagementTestMixedTrackedGroup", true);

        // then
        assertThat(tracked).contains("topicManagementTestMixedTrackedGroup.trackedTopic")
                           .doesNotContain("topicManagementTestMixedTrackedGroup.untrackedTopic");
    }
}
