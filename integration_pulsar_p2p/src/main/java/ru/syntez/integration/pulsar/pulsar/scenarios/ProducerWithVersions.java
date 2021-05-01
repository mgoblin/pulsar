package ru.syntez.integration.pulsar.pulsar.scenarios;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.pulsar.PulsarConfig;
import ru.syntez.integration.pulsar.pulsar.sender.PulsarSender;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ProducerWithVersions implements ProducerTestScenario {

    private final static Logger LOG = Logger.getLogger(ProducerWithVersions.class.getName());

    private final PulsarClient client;
    private final PulsarConfig config;

    public ProducerWithVersions(PulsarClient client, PulsarConfig config) {
        this.client = client;
        this.config = config;
    }

    @Override
    public int run(String topicName) {
        try (Producer<byte[]> producer =
                     client.newProducer()
                             .topic(topicName)
                             .compressionType(CompressionType.LZ4)
                             .create()) {

            PulsarSender sender = new PulsarSender(producer);

            int unknownDocsCount = sender.sendWithDocIdKey(
                    RoutingDocument::createUnknown,
                    config.getMessageCount()
            );

            int orderDocsCount = sender.sendWithDocIdKey(
                    RoutingDocument::createOrder,
                    config.getMessageCount()
            );

            int invoiceDocsCount = sender.sendWithDocIdKey(
                    RoutingDocument::createInvoice,
                    config.getMessageCount()
            );

            producer.flush();
            return unknownDocsCount + invoiceDocsCount + orderDocsCount;

        } catch (PulsarClientException e) {
            LOG.log(Level.SEVERE, "Error producing documents to pulsar", e);
            return 0;
        }
    }
}
