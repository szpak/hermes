package pl.allegro.tech.hermes.integration;

import com.googlecode.catchexception.CatchException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.integration.env.SharedServices;
import pl.allegro.tech.hermes.test.helper.endpoint.RemoteServiceEndpoint;
import pl.allegro.tech.hermes.test.helper.message.TestMessage;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.List;

import static com.googlecode.catchexception.CatchException.catchException;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaSingleMessageReaderTest extends IntegrationTest {

    private static final int NUMBER_OF_PARTITIONS = 2;
    private RemoteServiceEndpoint remoteService;

    @BeforeMethod
    public void initializeAlways() {
        remoteService = new RemoteServiceEndpoint(SharedServices.services().serviceMock());
    }

    @Test
    public void shouldFetchSingleMessageByTopicPartitionAndOffset() {
        // given
        operations.buildSubscription("fetchSingleMessageByTopicPartitionAndOffset", "topic", "subscription", HTTP_ENDPOINT_URL);

        List<String> messages = new ArrayList<String>() {{ range(0, 3).forEach(i -> add(TestMessage.random().body())); }};

        remoteService.expectMessages(messages);

        String qualifiedTopicName = "fetchSingleMessageByTopicPartitionAndOffset.topic";
        messages.forEach(message -> publisher.publish(qualifiedTopicName, message));

        remoteService.waitUntilReceived();

        // when
        List<String> previews = fetchPreviewsFromAllPartitions(qualifiedTopicName, 10);

        // then
        assertThat(previews).hasSize(messages.size()).contains(messages.toArray(new String[messages.size()]));
    }

    @Test
    public void shouldReturnNotFoundErrorForNonExistingOffset() {
        // given
        operations.buildSubscription("notFoundErrorForNonExistingOffset", "offsetTestTopic", "subscription", HTTP_ENDPOINT_URL);
        List<String> messages = new ArrayList<String>() {{ range(0, 3).forEach(i -> add(TestMessage.random().body())); }};

        remoteService.expectMessages(messages);
        messages.forEach(message -> publisher.publish("notFoundErrorForNonExistingOffset.offsetTestTopic", message));

        remoteService.waitUntilReceived();

        // when
        catchException(management.topic()).preview("notFoundErrorForNonExistingOffset.offsetTestTopic", PRIMARY_KAFKA_CLUSTER_NAME, 0, 10L);

        // then
        assertThat(CatchException.<NotFoundException>caughtException()).isInstanceOf(NotFoundException.class);
    }

    @Test
    public void shouldReturnNotFoundErrorForNonExistingPartition() {
        // given
        operations.buildTopic("notFoundErrorForNonExistingPartition", "partitionTestTopic");

        // when
        catchException(management.topic()).preview("notFoundErrorForNonExistingPartition.partitionTestTopic", PRIMARY_KAFKA_CLUSTER_NAME, 10, 0L);

        // then
        assertThat(CatchException.<NotFoundException>caughtException()).isInstanceOf(NotFoundException.class);
    }

    private List<String> fetchPreviewsFromAllPartitions(String qualifiedTopicName, int upToOffset) {
        List<String> result = new ArrayList<>();
        for (int p = 0; p < NUMBER_OF_PARTITIONS; p++) {
            long offset = 0;
            while (offset <= upToOffset) {
                try {
                    String wrappedMessage = management.topic().preview(qualifiedTopicName, PRIMARY_KAFKA_CLUSTER_NAME, p, offset);
                    result.add(unwrap(wrappedMessage));
                    offset++;
                } catch (Exception e) {
                    break;
                }
            }
        }
        return result;
    }

    private String unwrap(String wrappedMessage) {
        String msg = wrappedMessage.split("\"message\":", 2)[1];
        return msg.substring(0, msg.length() - 1);
    }

}
