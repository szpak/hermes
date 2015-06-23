package pl.allegro.tech.hermes.test.helper.environment;

import com.jayway.awaitility.Duration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

import static com.jayway.awaitility.Awaitility.await;

public class ZookeeperStarter implements Starter<ZooKeeperLocal> {

    private final Properties zkProperties;
    private ZooKeeperLocal zookeeper;

    public ZookeeperStarter() {
        zkProperties = loadDefaultZkProperties();
    }

    private Properties loadDefaultZkProperties() {
        Properties properties = new Properties();
        try {
            properties.load(this.getClass().getResourceAsStream("/zookeeper.properties"));
        } catch (IOException e) {
            throw new IllegalStateException("Error while loading kafka properties", e);
        }
        return properties;
    }

    @Override
    public void start() throws Exception {
        System.out.println("removing zk data");
        FileUtils.deleteDirectory(new File(zkProperties.getProperty("dataDir")));

        //start local zookeeper
        System.out.println("starting local zookeeper...");
        zookeeper = new ZooKeeperLocal(zkProperties);

        await().atMost(Duration.FIVE_SECONDS).until(() -> {
            try {
                Socket socket = new Socket("localhost", 2181);
                return true;
            } catch (IOException e) {
                return false;
            }
        });
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public ZooKeeperLocal instance() {
        return zookeeper;
    }


}
