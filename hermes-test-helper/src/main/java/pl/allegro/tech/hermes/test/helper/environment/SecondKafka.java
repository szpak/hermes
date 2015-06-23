package pl.allegro.tech.hermes.test.helper.environment;

public class SecondKafka extends KafkaStarter {

    public SecondKafka() {
        super("/server2.properties");
    }
}
