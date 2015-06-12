package pl.allegro.tech.hermes.client;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.api.Stream;
import org.eclipse.jetty.http2.api.server.ServerSessionListener;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.http2.frames.DataFrame;
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.FuturePromise;
import org.eclipse.jetty.util.Jetty;
import org.eclipse.jetty.util.Promise;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;
import pl.allegro.tech.hermes.client.jetty.JettyHttp2HermesSender;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.allegro.tech.hermes.client.HermesClientBuilder.hermesClient;

public class JettyHttp2SenderTest {

    private MockWebServer createMockWebServer() {
        MockWebServer server = new MockWebServer();
        server.setProtocolNegotiationEnabled(true);
        server.useHttps((SSLSocketFactory) SSLSocketFactory.getDefault(), true);
        server.enqueue(new MockResponse().setResponseCode(201).setBody("mirko"));
        return server;
    }

    @Test
    public void shouldPublishMessageViaHttp2Protocol() throws URISyntaxException, IOException {
        // given
//        MockWebServer server = createMockWebServer();
//        server.enqueue(new MockResponse().setResponseCode(201));
//        server.start();
//
//        URI uri = server.getUrl("").toURI();
        URI uri = URI.create("http://localhost:8080");
        HermesClient client = hermesClient(new JettyHttp2HermesSender()).withURI(uri).build();

        // when
        HermesResponse response = client.publish("topic", "message").join();

        // then
//        System.out.println(response.getFailureCause().get());
        assertThat(response.isSuccess()).isTrue();
//        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    public void example() throws Exception {

//        MockWebServer server = createMockWebServer();
//        server.enqueue(new MockResponse().setResponseCode(201));
//        server.start();
//        URI uri = server.getUrl("").toURI();


        HTTP2Client client = new HTTP2Client();
        SslContextFactory sslContextFactory = new SslContextFactory();
//        sslContextFactory.setIncludeProtocols("TLSv1.2", "TLSv1.1", "TLSv1");
        sslContextFactory.setTrustAll(true);
        client.addBean(sslContextFactory);
        client.start();

        String host = "localhost";
        int port = 8443;

        FuturePromise<Session> sessionPromise = new FuturePromise<>();
        client.connect(sslContextFactory, new InetSocketAddress(host, port), new ServerSessionListener.Adapter(), sessionPromise);
        Session session = sessionPromise.get(5, TimeUnit.SECONDS);

        HttpFields requestFields = new HttpFields();
        requestFields.put("User-Agent", client.getClass().getName() + "/" + Jetty.VERSION);
//        MetaData.Request metaData = new MetaData.Request("GET", new HttpURI(uri.toString(uri.toString()), HttpVersion.HTTP_2, requestFields);
        MetaData.Request metaData = new MetaData.Request("GET", new HttpURI("https://" + host + ":" + port + "/"), HttpVersion.HTTP_2, requestFields);
        HeadersFrame headersFrame = new HeadersFrame(0, metaData, null, true);
        final CountDownLatch latch = new CountDownLatch(1);
        session.newStream(headersFrame, new Promise.Adapter<Stream>(), new Stream.Listener.Adapter() {
            @Override
            public void onHeaders(Stream stream, HeadersFrame frame) {
                System.err.println(frame.getMetaData());
                if (frame.isEndStream())
                    latch.countDown();
            }

            @Override
            public void onData(Stream stream, DataFrame frame, Callback callback) {
                callback.succeeded();
                if (frame.isEndStream())
                    latch.countDown();
            }
        });

        latch.await(5, TimeUnit.SECONDS);

        client.stop();

//        assertThat(server.getRequestCount()).isEqualTo(1);
    }
}
