package ru.syntez.integration.pulsar.pulsar;

import lombok.Data;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

@Data
public class PulsarConfig {

    private final static Logger LOG = Logger.getLogger(PulsarConfig.class.getName());

    private String  brokers;
    private Integer messageCount;
    private Integer timeoutBeforeConsume;
    private ProducerConfig producer;

    private String  topicName;
    private String  topicInputRouteName;
    private String  topicInputFilterName;
    private String  topicOutputOrderName;
    private String  topicOutputInvoiceName;
    private String  topicOutputFilterName;

    public static Optional<PulsarConfig> loadFromResource(String resourceName) {
        final URL resourceURL = PulsarConfig.class.getResource(resourceName);
        if (resourceURL != null) {
            Yaml yaml = new Yaml();
            try (InputStream in = Files.newInputStream(Paths.get(resourceURL.toURI()))) {
                PulsarConfig config = yaml.loadAs(in, ru.syntez.integration.pulsar.pulsar.PulsarConfig.class);
                LOG.log(Level.INFO, config.toString());
                return Optional.of(config);
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Error load PulsarConfig from resource", e);
                return Optional.empty();
            }
        } else {
            LOG.log(
                    Level.WARNING,
                    "Error load PulsarConfig from resource " + resourceName);
            return Optional.empty();
        }
    }

}
