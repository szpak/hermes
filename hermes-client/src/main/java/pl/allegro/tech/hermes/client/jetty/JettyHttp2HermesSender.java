package pl.allegro.tech.hermes.client.jetty;

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
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.http2.frames.PushPromiseFrame;
import org.eclipse.jetty.http2.frames.ResetFrame;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.FuturePromise;
import org.eclipse.jetty.util.Jetty;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.client.HermesMessage;
import pl.allegro.tech.hermes.client.HermesResponse;
import pl.allegro.tech.hermes.client.HermesSender;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import static pl.allegro.tech.hermes.client.HermesResponseBuilder.hermesResponse;

public class JettyHttp2HermesSender implements HermesSender {

    private final static Logger logger = LoggerFactory.getLogger(JettyHttp2HermesSender.class);

    private final SslContextFactory sslContextFactory;

    public JettyHttp2HermesSender() {
        this(new SslContextFactory(true));
    }

    public JettyHttp2HermesSender(SslContextFactory sslContextFactory) {
        this.sslContextFactory = sslContextFactory;
    }

    @Override
    public CompletableFuture<HermesResponse> send(URI uri, HermesMessage message) {
        CompletableFuture<HermesResponse> future = new CompletableFuture<>();

        logger.info("sending event");
        HTTP2Client client = new HTTP2Client();
        client.addBean(sslContextFactory);

        Executors.newSingleThreadScheduledExecutor().execute(() -> {
            try {
                client.start();
//                sendWithJettyHttp2Client(client, URI.create("https://nghttp2.org:443/httpbin/post"), message, future);
//                sendWithJettyHttp2Client(client, URI.create("https://localhost:8082/"), message, future);
                sendWithJettyHttp2Client(client, URI.create("https://localhost:8081/"), message, future);

            } catch(Exception e) {
                logger.error("Could not send event", e);
                future.completeExceptionally(e);
            } finally {
                try {
                    client.stop();
                } catch(Exception e) {}
            }
        });

        return future;
    }

    private void sendWithJettyHttp2Client(HTTP2Client client, URI uri, HermesMessage message, CompletableFuture<HermesResponse> future) throws Exception {

        FuturePromise<Session> sessionPromise = new FuturePromise<>();
        client.connect(sslContextFactory, new InetSocketAddress(uri.getHost(), uri.getPort()), new ServerSessionListener.Adapter(), sessionPromise);
        Session session = sessionPromise.get(5, TimeUnit.SECONDS);

        HttpFields requestFields = new HttpFields();
        requestFields.put("Host", uri.getHost() + ":" + uri.getPort());
        requestFields.put("Accept", "*/*");
        requestFields.put("Accept-Encoding", "gzip, deflate");
        requestFields.put("User-Agent", client.getClass().getName() + "/" + Jetty.VERSION);
        requestFields.put("Content-Type", "text/plain");
        requestFields.put("Content-Length", message.getBody().getBytes(Charset.forName("UTF-8")).length + "");

        MetaData.Request metaData = new MetaData.Request("POST", new HttpURI(uri), HttpVersion.HTTP_2, requestFields);

        HeadersFrame headersFrame = new HeadersFrame(0, metaData, null, false);
        FuturePromise<Stream> streamPromise = new FuturePromise<>();
        CountDownLatch latch = new CountDownLatch(2);

        Stream.Listener responseListener = new Stream.Listener.Adapter()
        {
            @Override
            public void onHeaders(Stream stream, HeadersFrame frame)
            {
                MetaData.Response response = (MetaData.Response) frame.getMetaData();
                logger.info("on headers, stream={}, endStream={}, httpStatus={}", stream.getId(), frame.isEndStream(), response.getStatus());

                for (HttpField field : frame.getMetaData().getFields()) {
                    logger.info("Header: {}={}", field.getName(), field.getValue());
                }
            }

            @Override
            public void onData(Stream stream, DataFrame frame, Callback callback)
            {
                byte[] bytes = new byte[frame.getData().remaining()];
                frame.getData().get(bytes);
                logger.info("on data, stream=" + stream.getId() + ", endStream=" + frame.isEndStream() + ", data=" + new String(bytes));
                callback.succeeded();
                latch.countDown();
            }

            @Override
            public Stream.Listener onPush(Stream stream, PushPromiseFrame frame) {
                logger.info("on push");
                return null;
            }

            @Override
            public void onReset(Stream stream, ResetFrame frame) {
                logger.info("on reset");

            }

            @Override
            public void onTimeout(Stream stream, Throwable x) {
                logger.info("on timeout");
            }
        };

        session.newStream(headersFrame, streamPromise, responseListener);
        Stream stream = streamPromise.get(5, TimeUnit.SECONDS);

        ByteBuffer buffer = ByteBuffer.allocate(message.getBody().length());
        buffer.put(message.getBody().getBytes(Charset.forName("UTF-8")));
        DataFrame dataFrame = new DataFrame(stream.getId(), buffer, true);

        Callback callback = new Callback() {
            @Override
            public void succeeded() {
                logger.info("data succeeded");
                latch.countDown();
            }

            @Override
            public void failed(Throwable x) {
                logger.info("data failed");
                latch.countDown();
                latch.countDown();
            }
        };
        stream.data(dataFrame, callback);

        latch.await();

        future.complete(hermesResponse().withHttpStatus(201).build());
    }
}
