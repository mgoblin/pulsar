package ru.syntez.integration.pulsar.pulsar.scenarios;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.pulsar.PulsarConfig;
import ru.syntez.integration.pulsar.pulsar.sender.PulsarSender;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ProducerWithoutKeys implements ProducerTestScenario {

    private final static Logger LOG = Logger.getLogger(ProducerWithoutKeys.class.getName());

    private final PulsarClient client;
    private final PulsarConfig config;

    public ProducerWithoutKeys(PulsarClient client, PulsarConfig config) {
        this.client = client;
        this.config = config;
    }

    @Override
    public int run(String topicName) {
        try (Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .compressionType(CompressionType.LZ4)
                .create()) {

            PulsarSender sender = new PulsarSender(producer);
            final int sentCount = sender.send(
                    RoutingDocument::createInvoice,
                    document -> null,
                    config.getMessageCount()
            );
            producer.flush();
            return sentCount;
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error producing documents to pulsar", e);
            return 0;
        }
    }
}
