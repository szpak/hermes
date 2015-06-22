package pl.allegro.tech.hermes.integration;

import com.googlecode.catchexception.CatchException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.integration.client.SlowClient;

import java.io.IOException;

import static com.googlecode.catchexception.CatchException.catchException;
import static org.assertj.core.api.Assertions.assertThat;

public class PublishingTimeoutTest extends IntegrationTest {

    private SlowClient client;

    @BeforeClass
    public void initialize() {
        this.client = new SlowClient();
    }

    @Test
    public void shouldHandleRequestTimeout() throws IOException, InterruptedException {
        // given
        operations.buildTopic("handleRequestTimeout", "topic");
        wait.untilTopicIsCreated("handleRequestTimeout", "topic");

        int clientTimeout = 5000;
        int pauseTimeBetweenChunks = 300;
        int delayBeforeSendingFirstData = 0;

        // when
        long start = System.currentTimeMillis();
        String response = client.slowEvent(
            clientTimeout, pauseTimeBetweenChunks, delayBeforeSendingFirstData, "handleRequestTimeout.topic"
        );
        long elapsed = System.currentTimeMillis() - start;

        //then
        assertThat(response).contains("408 Request Time-out");
        assertThat(elapsed).isLessThan(2500);
    }

    @Test
    public void shouldCloseConnectionAfterSendingDelayData() throws IOException, InterruptedException {
        //given
        operations.buildTopic("closeConnectionAfterSendingDelayData", "topic");
        wait.untilTopicIsCreated("closeConnectionAfterSendingDelayData", "topic");

        int clientTimeout = 5000;
        int pauseTimeBetweenChunks = 0;
        int delayBeforeSendingFirstData = 3000;

        //when
        catchException(client).slowEvent(
            clientTimeout, pauseTimeBetweenChunks, delayBeforeSendingFirstData, "closeConnectionAfterSendingDelayData.topic"
        );

        //then
        assertThat(CatchException.<Exception>caughtException()).hasMessage("Broken pipe");
    }

}
