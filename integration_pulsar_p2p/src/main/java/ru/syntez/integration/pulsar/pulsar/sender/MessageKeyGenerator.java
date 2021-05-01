package ru.syntez.integration.pulsar.pulsar.sender;

import ru.syntez.integration.pulsar.entities.RoutingDocument;

import java.util.Optional;

@FunctionalInterface
public interface MessageKeyGenerator {
    String generate(RoutingDocument document);
}
