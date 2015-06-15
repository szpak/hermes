package pl.allegro.tech.hermes.client;

import org.junit.Test;
import pl.allegro.tech.hermes.client.jetty.JettyHttp2HermesSender;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.allegro.tech.hermes.client.HermesClientBuilder.hermesClient;

public class JettyHttp2SenderTest {

    @Test
    public void shouldPublishMessageViaHttp2Protocol() throws URISyntaxException, IOException {
        // given
        URI uri = URI.create("http://localhost:8080");
        HermesClient client = hermesClient(new JettyHttp2HermesSender()).withURI(uri).build();

        // when
        HermesResponse response = client.publish("topic", "what up?").join();

        // then
        assertThat(response.isSuccess()).isTrue();
    }
}
