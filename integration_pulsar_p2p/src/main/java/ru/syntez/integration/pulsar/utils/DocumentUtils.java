package ru.syntez.integration.pulsar.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.exceptions.TestMessageException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DocumentUtils {

    private final static Logger LOG = Logger.getLogger(DocumentUtils.class.getName());

    private static ObjectMapper xmlMapper() {
        JacksonXmlModule xmlModule = new JacksonXmlModule();
        xmlModule.setDefaultUseWrapper(false);
        ObjectMapper xmlMapper = new XmlMapper(xmlModule);
        xmlMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return new XmlMapper(xmlModule);
    }

    public static RoutingDocument createDocument() {
        String messageXml = "<?xml version=\"1.0\" encoding=\"windows-1251\"?><routingDocument><docId>1</docId><docType>order</docType></routingDocument>";
        try {
            return xmlMapper().readValue(messageXml, RoutingDocument.class);
        } catch (IOException e) {
            LOG.log(
                    Level.WARNING,
                    String.format("Convert '%s' to routing document failed", messageXml),
                    e
            );
            throw new TestMessageException(e);
        }
    }

    public static byte[] serializeDocument(RoutingDocument document) throws JsonProcessingException {
        return xmlMapper().writeValueAsString(document).getBytes();
    }

}
