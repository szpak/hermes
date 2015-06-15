package pl.allegro.tech.hermes.client.jetty;

import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.api.Stream;
import org.eclipse.jetty.http2.api.server.ServerSessionListener;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.frames.DataFrame;
import org.eclipse.jetty.http2.frames.Frame;
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.http2.frames.PushPromiseFrame;
import org.eclipse.jetty.http2.frames.ResetFrame;
import org.eclipse.jetty.util.*;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import pl.allegro.tech.hermes.client.HermesMessage;
import pl.allegro.tech.hermes.client.HermesResponse;
import pl.allegro.tech.hermes.client.HermesSender;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.*;

import static pl.allegro.tech.hermes.client.HermesResponseBuilder.hermesResponse;

public class JettyHttp2HermesSender implements HermesSender {

    private final SslContextFactory sslContextFactory;

    public JettyHttp2HermesSender() {
        this(new SslContextFactory(true));
    }

    public JettyHttp2HermesSender(SslContextFactory sslContextFactory) {
        this.sslContextFactory = sslContextFactory;
    }

    @Override
    public CompletableFuture<HermesResponse> send(URI uri, HermesMessage message) {
//        uri = URI.create("https://localhost:8081/");
        uri = URI.create("https://localhost:8443/");
        CompletableFuture<HermesResponse> future = new CompletableFuture<>();

        HTTP2Client client = new HTTP2Client();
        client.addBean(sslContextFactory);
        try {
            client.start();
        } catch (Exception e) {
            throw new RuntimeException("Could not start Jetty HTTP/2 client");
        }

        FuturePromise<Session> sessionPromise = new FuturePromise<>();
        client.connect(sslContextFactory, new InetSocketAddress(uri.getHost(), uri.getPort()), new ServerSessionListener.Adapter(), sessionPromise);
        Session session;
        try {
            session = sessionPromise.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Could not connect to server", e);
        }

        HttpFields requestFields = new HttpFields();
        requestFields.put("Host", uri.getHost() + ":" + uri.getPort());
        requestFields.put("User-Agent", client.getClass().getName() + "/" + Jetty.VERSION);
        requestFields.put("Content-Type", "text/plain");
        requestFields.put("Content-Length", message.getBody().getBytes(Charset.forName("UTF-8")).length + "");

        MetaData.Request metaData = new MetaData.Request("POST", new HttpURI(uri), HttpVersion.HTTP_2, requestFields);
        HeadersFrame headersFrame = new HeadersFrame(0, metaData, null, false);

        session.newStream(headersFrame, new Promise<Stream>() {
            @Override
            public void succeeded(Stream stream) {
                System.out.println("succeeded " + stream.getId());

                ByteBuffer buffer = ByteBuffer.allocate(message.getBody().length());
                buffer.put(message.getBody().getBytes(Charset.forName("UTF-8")));
                DataFrame dataFrame = new DataFrame(stream.getId(), buffer, true);
                stream.data(dataFrame, new Callback() {
                    @Override
                    public void succeeded() {
                        System.out.println("succeeded callback");
                    }

                    @Override
                    public void failed(Throwable x) {
                        System.out.println("failed callback");
                    }
                });
            }

            @Override
            public void failed(Throwable x) {
                System.out.println("failed");
            }
        }, new Stream.Listener.Adapter() {
            @Override
            public void onHeaders(Stream stream, HeadersFrame frame) {
                System.out.println("on headers, " + stream.getId() + ", " + frame.isEndStream());
                MetaData.Response response = (MetaData.Response) frame.getMetaData();

                System.out.println(response.getStatus());
                for (HttpField field : frame.getMetaData().getFields()) {
                    System.out.println(field.getName() + ": " + field.getValue());
                }
            }

            @Override
            public void onData(Stream stream, DataFrame frame, Callback callback) {
                byte[] bytes = new byte[frame.getData().remaining()];
                frame.getData().get(bytes);
                System.out.println("on data, " + stream.getId() + ", " + frame.isEndStream() + ", " + new String(bytes));
                try {
                    client.stop();
                } catch (Exception e) {
                    future.complete(hermesResponse().withHttpStatus(205).build());
                    e.printStackTrace();
                }
            }

            @Override
            public void onTimeout(Stream stream, Throwable x) {
                System.out.println("on timeout");
            }

            @Override
            public Stream.Listener onPush(Stream stream, PushPromiseFrame frame) {
                System.out.println("on push");
                return null;
            }

            @Override
            public void onReset(Stream stream, ResetFrame frame) {
                System.out.println("on reset");
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
