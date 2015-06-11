package pl.allegro.tech.hermes.client;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;
import pl.allegro.tech.hermes.client.jetty.JettyHttp2HermesSender;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.allegro.tech.hermes.client.HermesClientBuilder.hermesClient;

public class JettyHttp2SenderTest {

    private MockWebServer createMockWebServer() {
        MockWebServer server = new MockWebServer();
        server.setProtocolNegotiationEnabled(false);
        server.useHttps((SSLSocketFactory) SSLSocketFactory.getDefault(), false);
        server.enqueue(new MockResponse().setResponseCode(201));
        return server;
    }

    @Test
    public void shouldPublishMessageViaHttp2Protocol() throws URISyntaxException, IOException {
        // given
        MockWebServer server = createMockWebServer();
        server.enqueue(new MockResponse().setResponseCode(201));
        server.start();
//        try {
//            Thread.sleep(100_000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        URI uri = server.getUrl("").toURI();
//        URI uri = new URI("https://http2.akamai.com");
        HermesClient client = hermesClient(new JettyHttp2HermesSender()).withURI(uri).build();

        // when
        HermesResponse response = client.publish("topic", "message").join();

        // then
        System.out.println(response.getFailureCause().get());
        assertThat(response.isSuccess()).isTrue();
//        assertThat(server.getRequestCount()).isEqualTo(1);
    }
}
