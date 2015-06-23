package pl.allegro.tech.hermes.test.helper.environment;

import com.jayway.awaitility.Duration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static com.jayway.awaitility.Awaitility.await;

public class KafkaStarter implements Starter<KafkaLocal> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStarter.class);
    private final Properties kafkaProperties;

    private KafkaLocal kafkaLocal;

    public KafkaStarter(String properties) {
        kafkaProperties = loadDefaultProperties(properties);
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting in-memory Kafka");
        kafkaLocal = new KafkaLocal(kafkaProperties);
        waitForStartup(Integer.valueOf(kafkaProperties.getProperty("broker.id")),
                kafkaProperties.getProperty("zookeeper.connect").split("/")[1]);
    }

    private Properties loadDefaultProperties(String p) {
        Properties properties = new Properties();
        try {
            logger.info("Loading default kafka properties file");
            properties.load(this.getClass().getResourceAsStream(p));
        } catch (IOException e) {
            throw new IllegalStateException("Error while loading kafka properties", e);
        }
        return properties;
    }



    private void waitForStartup(int brokerId, String zookeeperConnect) throws InterruptedException {
        final CuratorFramework client = startZookeeperClient("localhost:2181");
        await().atMost(Duration.ONE_MINUTE).until(() -> {
            try {
                return client.getChildren().forPath("/" + zookeeperConnect + "/brokers/ids").contains(Integer.toString(brokerId));
            } catch (InterruptedException e) {
                return false;
            }
        });
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping in-memory Kafka");
        kafkaLocal.stop();
    }

    @Override
    public KafkaLocal instance() {
        return kafkaLocal;
    }

    private CuratorFramework startZookeeperClient(String connectString) throws InterruptedException {
        CuratorFramework zookeeperClient = CuratorFrameworkFactory.builder()
            .connectString(connectString)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();
        zookeeperClient.start();
        zookeeperClient.blockUntilConnected();
        return zookeeperClient;
    }


}
