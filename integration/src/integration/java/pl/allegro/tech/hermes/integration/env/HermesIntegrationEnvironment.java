package pl.allegro.tech.hermes.integration.env;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.test.helper.environment.KafkaStarter;
import pl.allegro.tech.hermes.test.helper.environment.SecondKafka;
import pl.allegro.tech.hermes.test.helper.environment.Starter;
import pl.allegro.tech.hermes.test.helper.environment.WireMockStarter;
import pl.allegro.tech.hermes.test.helper.environment.ZookeeperStarter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class HermesIntegrationEnvironment implements EnvironmentAware {

    private static final Map<Class<?>, Starter<?>> STARTERS = new LinkedHashMap<>();

    static {
        STARTERS.put(GraphiteMockStarter.class, new GraphiteMockStarter(GRAPHITE_SERVER_PORT));
        STARTERS.put(WireMockStarter.class, new WireMockStarter(HTTP_ENDPOINT_PORT));
        STARTERS.put(GraphiteHttpMockStarter.class, new GraphiteHttpMockStarter());
        STARTERS.put(OAuthServerMockStarter.class, new OAuthServerMockStarter());
        STARTERS.put(ZookeeperStarter.class, new ZookeeperStarter());
        STARTERS.put(KafkaStarter.class, new KafkaStarter("/server.properties"));
        STARTERS.put(SecondKafka.class, new SecondKafka());
        STARTERS.put(JmsStarter.class, new JmsStarter());
        STARTERS.put(ConsumersStarter.class, new ConsumersStarter());
        STARTERS.put(FrontendStarter.class, new FrontendStarter(FRONTEND_HEALTH_ENDPOINT));
        STARTERS.put(ManagementStarter.class, new ManagementStarter(MANAGEMENT_PORT));
    }

    private CuratorFramework zookeeper;

    private CuratorFramework kafkaZookeeper;

    @BeforeSuite
    public void prepareEnvironment() throws Exception {
        for (Starter<?> starter : STARTERS.values()) {
            starter.start();
        }

        this.zookeeper = startZookeeperClient();
        this.kafkaZookeeper = startKafkaZookeeperClient();

        SharedServices.initialize(STARTERS, zookeeper, kafkaZookeeper);
    }

    private CuratorFramework startZookeeperClient() throws InterruptedException {
        final CuratorFramework zookeeperClient = CuratorFrameworkFactory.builder()
                .connectString(CONFIG_FACTORY.getStringProperty(Configs.ZOOKEEPER_CONNECT_STRING))
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        zookeeperClient.start();
        return zookeeperClient;
    }

    private CuratorFramework startKafkaZookeeperClient() throws InterruptedException {
        final CuratorFramework zookeeperClient = CuratorFrameworkFactory.builder()
                .connectString(CONFIG_FACTORY.getStringProperty(Configs.KAFKA_ZOOKEEPER_CONNECT_STRING))
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        zookeeperClient.start();
        return zookeeperClient;
    }

    @AfterSuite(alwaysRun = true)
    public void cleanEnvironment() throws Exception {
        ArrayList<Starter<?>> reversedStarters = new ArrayList<>(STARTERS.values());
        Collections.reverse(reversedStarters);

        for (Starter<?> starter : reversedStarters) {
            starter.stop();
        }
        zookeeper.close();
    }

    @Test
    public void shouldTriggerBeforeAndAfterMethods() {
    }
}
