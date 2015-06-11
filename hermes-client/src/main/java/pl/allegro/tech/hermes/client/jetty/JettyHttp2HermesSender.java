package pl.allegro.tech.hermes.client.jetty;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import pl.allegro.tech.hermes.client.HermesMessage;
import pl.allegro.tech.hermes.client.HermesResponse;
import pl.allegro.tech.hermes.client.HermesSender;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import static pl.allegro.tech.hermes.client.HermesResponseBuilder.hermesResponse;

public class JettyHttp2HermesSender implements HermesSender {

    private final HttpClient client;

    public JettyHttp2HermesSender() {
        this(new SslContextFactory(true));
    }

    public JettyHttp2HermesSender(SslContextFactory sslContextFactory) {
        HTTP2Client http2Client = new HTTP2Client();
        HttpClientTransportOverHTTP2 transport = new HttpClientTransportOverHTTP2(http2Client);
        this.client = new HttpClient(transport, sslContextFactory);
        transport.setHttpClient(this.client);
        try {
            this.client.start();
        } catch (Exception e) {
        }
    }

    @Override
    public CompletableFuture<HermesResponse> send(URI uri, HermesMessage message) {
        CompletableFuture<HermesResponse> future = new CompletableFuture<>();
        uri = URI.create("https://http2.akamai.com/");
        client.newRequest(uri)
                .method(HttpMethod.POST)
                .content(new StringContentProvider(message.getBody()))
                .send(result -> {
                    if (result.isSucceeded()) {
                        future.complete(fromJettyResponse(result.getResponse()));
                    } else {
                        future.completeExceptionally(result.getFailure());
                    }
                });
        return future;
    }

    private HermesResponse fromJettyResponse(Response response) {
        return hermesResponse()
                .withHttpStatus(response.getStatus())
                .withHeaderSupplier(header -> response.getHeaders().get(header))
                .build();
    }
}
