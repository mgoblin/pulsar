package ru.syntez.integration.pulsar.pulsar.scenarios;

import org.apache.pulsar.client.api.PulsarClient;
import ru.syntez.integration.pulsar.pulsar.PulsarConfig;

import java.util.logging.Logger;

public class ProducerWithKeys implements ProducerTestScenario {
    private final static Logger LOG = Logger.getLogger(ProducerWithKeys.class.getName());

    private final PulsarClient client;
    private final PulsarConfig config;

    public ProducerWithKeys(PulsarClient client, PulsarConfig config) {
        this.client = client;
        this.config = config;
    }

    @Override
    public int run(String topicName) {
        return 0;
    }
}
