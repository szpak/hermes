package pl.allegro.tech.hermes.frontend.server;

import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.RequestDumpingHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ServletInfo;
import io.undertow.servlet.util.ImmediateInstanceFactory;
import org.apache.zookeeper.common.IOUtils;
import org.xnio.IoUtils;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.frontend.HermesFrontend;
import pl.allegro.tech.hermes.frontend.cache.topic.TopicsCache;
import pl.allegro.tech.hermes.frontend.publishing.PublishingServlet;
import pl.allegro.tech.hermes.frontend.services.HealthCheckService;
import pl.allegro.tech.hermes.frontend.validator.MessageValidator;

import javax.inject.Inject;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.servlet.Servlet;
import javax.servlet.ServletException;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.concurrent.Executors;

import static io.undertow.Handlers.path;
import static io.undertow.Handlers.redirect;
import static io.undertow.UndertowOptions.*;
import static io.undertow.servlet.Servlets.deployment;
import static io.undertow.servlet.Servlets.servlet;
import static org.xnio.Options.BACKLOG;
import static org.xnio.Options.READ_TIMEOUT;
import static pl.allegro.tech.hermes.common.config.Configs.*;

public class HermesServer {

    private Undertow undertow;
    private HermesShutdownHandler gracefulShutdown;

    private final HermesMetrics hermesMetrics;
    private final ConfigFactory configFactory;
    private final MessageValidator messageValidator;
    private final TopicsCache topicsCache;
    private final PublishingServlet publishingServlet;
    private final HealthCheckService healthCheckService;
    private final int port;
    private final int portSSL;
    private final String host;

    @Inject
    public HermesServer(
            TopicsCache topicsCache,
            ConfigFactory configFactory,
            HermesMetrics hermesMetrics,
            MessageValidator messageValidator,
            PublishingServlet publishingServlet,
            HealthCheckService healthCheckService) {

        this.topicsCache = topicsCache;
        this.configFactory = configFactory;
        this.messageValidator = messageValidator;
        this.hermesMetrics = hermesMetrics;
        this.publishingServlet = publishingServlet;
        this.healthCheckService = healthCheckService;

        this.port = configFactory.getIntProperty(FRONTEND_PORT);
        this.portSSL = configFactory.getIntProperty(FRONTEND_SSL_PORT);
        this.host = configFactory.getStringProperty(FRONTEND_HOST);
    }

    public void start() {
        topicsCache.start(ImmutableList.of(messageValidator));
        configureServer().start();
    }

    public void gracefulShutdown() throws InterruptedException {
        healthCheckService.shutdown();

        Thread.sleep(configFactory.getIntProperty(Configs.FRONTEND_GRACEFUL_SHUTDOWN_INITIAL_WAIT_MS));

        gracefulShutdown.handleShutdown();
    }

    public void shutdown() throws InterruptedException {
        undertow.stop();
    }

    private Undertow configureServer() {
        gracefulShutdown = new HermesShutdownHandler(deployAndStart(), hermesMetrics);

        this.undertow = Undertow.builder()
                .addHttpListener(port, host)
                .addHttpsListener(portSSL, host, prepareSSLContext())
                .setServerOption(REQUEST_PARSE_TIMEOUT, configFactory.getIntProperty(FRONTEND_REQUEST_PARSE_TIMEOUT))
                .setServerOption(MAX_HEADERS, configFactory.getIntProperty(FRONTEND_MAX_HEADERS))
                .setServerOption(MAX_PARAMETERS, configFactory.getIntProperty(FRONTEND_MAX_PARAMETERS))
                .setServerOption(MAX_COOKIES, configFactory.getIntProperty(FRONTEND_MAX_COOKIES))
                .setServerOption(ALWAYS_SET_KEEP_ALIVE, configFactory.getBooleanProperty(FRONTEND_SET_KEEP_ALIVE))
                .setServerOption(ENABLE_HTTP2, configFactory.getBooleanProperty(FRONTEND_HTTP2))
                .setSocketOption(BACKLOG, configFactory.getIntProperty(FRONTEND_BACKLOG_SIZE))
                .setSocketOption(READ_TIMEOUT, configFactory.getIntProperty(FRONTEND_READ_TIMEOUT))
                .setIoThreads(configFactory.getIntProperty(FRONTEND_IO_THREADS_COUNT))
                .setWorkerThreads(configFactory.getIntProperty(FRONTEND_WORKER_THREADS_COUNT))
                .setBufferSize(configFactory.getIntProperty(FRONTEND_BUFFER_SIZE))
//                .setHandler(gracefulShutdown)
                .setHandler(exchange -> {
                    if (exchange.isInIoThread()) {
                        exchange.dispatch(Executors.newSingleThreadExecutor(), ex ->
                        {
                            exchange.setResponseCode(201);
                            exchange.startBlocking();
                            String data = CharStreams.toString(new InputStreamReader(ex.getInputStream()));
                            exchange.getRequestHeaders().forEach(header -> System.out.println(header.getHeaderName() + " = " + header.getFirst()));
                            System.out.println("data: " + data);
                        });
                        return;
                    }

                })
                .build();

        return undertow;
    }

    private PathHandler deployAndStart() {
        try {
            HttpHandler handler = deploy().start();
            handler = isEnabled(FRONTEND_REQUEST_DUMPER) ? new RequestDumpingHandler(handler) : handler;
            return path().addExactPath("/", redirect("/status/health"))
                    .addExactPath("/status/ping", redirect("/status/health"))
                    .addPrefixPath("/status/health", new HealthCheckHandler(healthCheckService))
                    .addPrefixPath("/", handler);
        } catch (ServletException e) {
            throw new IllegalStateException("Something went wrong while starting servlet in undertow", e);
        }
    }

    private DeploymentManager deploy() {
        DeploymentManager manager = Servlets.defaultContainer().addDeployment(prepareDeployment());
        manager.deploy();
        return manager;
    }

    private DeploymentInfo prepareDeployment() {
        HermesDispatcher dispatcher = new HermesDispatcher(publishingServlet, new NotFoundServlet(), "topics");
        ServletInfo dispatcherInfo = servletInfo("dispatcher", "/*", HermesDispatcher.class, dispatcher);
        return deployment()
                .setClassLoader(HermesFrontend.class.getClassLoader())
                .setContextPath("/")
                .setDeploymentName("hermes")
                .addServlet(dispatcherInfo);
    }

    private ServletInfo servletInfo(String name, String mapping, Class<? extends Servlet> clazz, Servlet servlet) {
        return servlet(name, clazz, new ImmediateInstanceFactory<>(servlet))
                .addMapping(mapping).setAsyncSupported(true).setLoadOnStartup(1);
    }

    private boolean isEnabled(Configs property) {
        return configFactory.getBooleanProperty(property);
    }

    private SSLContext prepareSSLContext() {
        try {
            return createSSLContext(loadKeyStore(configFactory.getStringProperty(FRONTEND_SSL_KEYSTORE_LOCATION),
                                                 configFactory.getStringProperty(FRONTEND_SSL_KEYSTORE_PASSWORD),
                                                 configFactory.getStringProperty(FRONTEND_SSL_KEYSTORE_FORMAT)),
                                    loadKeyStore(configFactory.getStringProperty(FRONTEND_SSL_TRUSTSTORE_LOCATION),
                                                 configFactory.getStringProperty(FRONTEND_SSL_TRUSTSTORE_PASSWORD),
                                                 configFactory.getStringProperty(FRONTEND_SSL_TRUSTSTORE_FORMAT)));
        } catch (Exception e) {
            throw new IllegalStateException("Something went wrong with setting up SSL context.", e);
        }
    }

    private KeyStore loadKeyStore(String name, String password, String format) throws Exception {
        try (InputStream stream = HermesServer.class.getClassLoader().getResourceAsStream(name)) {
            KeyStore loadedKeystore = KeyStore.getInstance(format);
            loadedKeystore.load(stream, password.toCharArray());
            return loadedKeystore;
        }
    }

    private SSLContext createSSLContext(final KeyStore keyStore, final KeyStore trustStore) throws Exception {
        KeyManager[] keyManagers;
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        char[] pass = configFactory.getStringProperty(FRONTEND_SSL_KEYSTORE_PASSWORD).toCharArray();
        keyManagerFactory.init(keyStore, pass);
        keyManagers = keyManagerFactory.getKeyManagers();

        TrustManager[] trustManagers;
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        trustManagers = trustManagerFactory.getTrustManagers();

        SSLContext sslContext;
        sslContext = SSLContext.getInstance(configFactory.getStringProperty(FRONTEND_SSL_PROTOCOL));
        sslContext.init(keyManagers, trustManagers, SecureRandom.getInstanceStrong());

        return sslContext;
    }
}
