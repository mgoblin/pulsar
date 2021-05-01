package ru.syntez.integration.pulsar;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.client.api.*;
import ru.syntez.integration.pulsar.entities.DocumentTypeEnum;
import ru.syntez.integration.pulsar.entities.KeyTypeEnum;
import ru.syntez.integration.pulsar.entities.RoutingDocument;
import ru.syntez.integration.pulsar.pulsar.ConsumerCreator;
import ru.syntez.integration.pulsar.pulsar.PulsarConfig;
import ru.syntez.integration.pulsar.pulsar.sender.PulsarSender;
import ru.syntez.integration.pulsar.utils.ResultOutput;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static ru.syntez.integration.pulsar.pulsar.PulsarConfig.loadFromResource;
import static ru.syntez.integration.pulsar.utils.DocumentUtils.createDocument;
import static ru.syntez.integration.pulsar.utils.DocumentUtils.serializeDocument;

/**
 * Main class
 *
 * @author Skyhunter
 */
public class IntegrationPulsarApplication {

    private final static Logger LOG = Logger.getLogger(ru.syntez.integration.pulsar.IntegrationPulsarApplication.class.getName());
    private static AtomicInteger msg_sent_counter = new AtomicInteger(0);
    private static AtomicInteger msg_received_counter = new AtomicInteger(0);
    private static PulsarConfig config;
    private static Map<String, Set<String>> consumerRecordSetMap = new ConcurrentHashMap<>();
    private static PulsarClient client;

    private static final String SUBSCRIPTION_NAME = "shared-demo";
    private static final String SUBSCRIPTION_KEY_NAME = "key-shared-demo";

    public static void main(String[] args) {

        config = loadFromResource("/application.yml").orElseThrow(RuntimeException::new);

        try {
            client = PulsarClient.builder()
                    .serviceUrl(config.getBrokers())
                    .build();

            //кейс ATLEAST_ONCE
            LOG.info("Запуск проверки гарантии доставки ATLEAST_ONCE...");
            runConsumersWithoutTimeout(3, false, false);
            LOG.info("Проверка гарантии доставки ATLEAST_ONCE завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс ATMOST_ONCE
            LOG.info("Запуск проверки гарантии доставки ATMOST_ONCE...");
            runConsumersWithoutTimeout(3, true, false);
            LOG.info("Проверка гарантии доставки ATMOST_ONCE завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс EFFECTIVELY_ONCE
            // TODO сделать отдельное пространство имен с топиком
            LOG.info("Запуск проверки гарантии доставки EFFECTIVELY_ONCE + дедупликация...");
            runConsumersWithoutTimeout(3, true, true);
            LOG.info("Проверка гарантии доставки EFFECTIVELY_ONCE + дедупликация завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс TTL
            LOG.info("Запуск проверки TTL...");
            runConsumersWithTimeout(3);
            LOG.info("Проверка TTL завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс Routing
            LOG.info(String.format("Запуск проверки Routing...", msg_sent_counter.get()));
            runConsumersWithRouting();
            LOG.info("Проверка Routing завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            //кейс Filter
            LOG.info(String.format("Запуск проверки Filter...", msg_sent_counter.get()));
            runConsumersWithFilter();
            LOG.info("Проверка Filter завершена.");
            ResultOutput.outputResult(msg_sent_counter.get(), msg_received_counter.get(), consumerRecordSetMap);
            resetResults();

            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("Проверка всех кейсов завершена.");

    }

    private static void resetResults() {
        msg_sent_counter = new AtomicInteger(0);
        msg_received_counter = new AtomicInteger(0);
        consumerRecordSetMap = new ConcurrentHashMap<>();
    }

    private static void runConsumersWithFilter() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> {
            try {
                runProducerWithKeys(config.getTopicInputFilterName());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executorService.execute(() -> {
            try {
                String consumerId = "filter";
                Consumer consumer = ConsumerCreator.createConsumer(
                        client, config.getTopicOutputFilterName(), consumerId,
                        String.format("%s_%s", SUBSCRIPTION_KEY_NAME, consumerId),
                        true);
                startConsumer(consumer);
                consumer.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
        executorService.shutdown();
        //Минуты должно хватить на обработку всех сообщений
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Запуск обработки сообщений после маршрутизации
     *
     * @throws InterruptedException
     */
    private static void runConsumersWithRouting() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> {
            try {
                runProducerWithKeys(config.getTopicInputRouteName());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executorService.execute(() -> {
            try {
                String consumerId = "order";
                Consumer consumer = ConsumerCreator.createConsumer(
                        client, config.getTopicOutputOrderName(), consumerId,
                        String.format("%s_%s", SUBSCRIPTION_KEY_NAME, consumerId),
                        true
                );
                startConsumer(consumer);
                // consumer.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
        executorService.execute(() -> {
            try {
                String consumerId = "invoice";
                Consumer consumer = ConsumerCreator.createConsumer(
                        client, config.getTopicOutputInvoiceName(), consumerId,
                        String.format("%s_%s", SUBSCRIPTION_KEY_NAME, consumerId),
                        true
                );
                startConsumer(consumer);
                // consumer.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
        executorService.shutdown();
        //Минуты должно хватить на обработку всех сообщений
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Запуск одновременной обработки
     *
     * @param consumerCount - количество консьюмеров
     * @param withKeys      - флаг генерации ключей сообщений
     * @param withVersions  - флаг генерации нескольких сообщений для одного ключа
     * @throws InterruptedException
     */
    private static void runConsumersWithoutTimeout(Integer consumerCount, Boolean withKeys, Boolean withVersions) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        if (withKeys) {
            if (withVersions) {
                executorService.execute(ru.syntez.integration.pulsar.IntegrationPulsarApplication::runProducerWithVersions);
            } else {
                executorService.execute(() -> {
                    try {
                        runProducerWithKeys(config.getTopicName());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        } else {
            executorService.execute(ru.syntez.integration.pulsar.IntegrationPulsarApplication::runProducerWithoutKeys);
        }
        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            executorService.execute(() -> {
                Consumer consumer = null;
                try {
                    consumer = createAndStartConsumer(consumerId, withKeys);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                } finally {
                    if (consumer != null) {
                        try {
                            consumer.close();
                        } catch (PulsarClientException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        executorService.shutdown();
        //Минуты должно хватить на обработку всех сообщений
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Запуск обработки через время после записи
     *
     * @param consumerCount
     * @throws InterruptedException
     */
    private static void runConsumersWithTimeout(Integer consumerCount) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        executorService.execute(() -> {
            try {
                runProducerWithKeys(config.getTopicName());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executorService.awaitTermination(config.getTimeoutBeforeConsume(), TimeUnit.MINUTES);

        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            executorService.execute(() -> {
                Consumer consumer = null;
                try {
                    consumer = createAndStartConsumer(consumerId, true);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                } finally {
                    if (consumer != null) {
                        try {
                            consumer.close();
                        } catch (PulsarClientException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        executorService.shutdown();
        //Минуты должно хватить на обработку всех сообщений
        executorService.awaitTermination(1, TimeUnit.MINUTES);

    }

    /**
     * Отправка сообщений без ключа для первого кейса - проверка гарантии at-least-once
     * TODO выделить в отдельный юзкейс
     */
    private static void runProducerWithoutKeys() {
        RoutingDocument document = createDocument();
        try {
            Producer<byte[]> producer = client.newProducer()
                    .topic(config.getTopicName())
                    .compressionType(CompressionType.LZ4)
                    .create();
            for (int index = 0; index < config.getMessageCount(); index++) {
                document.setDocId(index);
                document.setDocType(DocumentTypeEnum.invoice);
                byte[] msgValue = serializeDocument(document);
                producer.newMessage().value(msgValue).send();
                msg_sent_counter.incrementAndGet();
                //LOG.info("Send message " + index);
            }
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Отправка сообщений с уникальным ключом для второго кейса - проверка гарантии at-most-once
     * TODO выделить в отдельный юзкейс
     */
    private static void runProducerWithKeys(String topicName) throws PulsarClientException, JsonProcessingException {
        RoutingDocument document = createDocument();
        //Close producer
        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .compressionType(CompressionType.LZ4)
                .create();

        PulsarSender sender = new PulsarSender(producer);
        int sentMessageCount = sender.send(
                (int id) -> {
                    RoutingDocument doc = createDocument();
                    doc.setDocType((id % 2) == 0 ? DocumentTypeEnum.order : DocumentTypeEnum.invoice);
                    doc.setDocId(id);
                    return doc;
                },
                (RoutingDocument doc) -> Optional.of(String.valueOf(doc.getDocId())),
                config.getMessageCount()
        );
        for (int index = 0; index < config.getMessageCount(); index++) {
            document.setDocId(index);
            if ((index % 2) == 0) {
                document.setDocType(DocumentTypeEnum.order);
            } else {
                document.setDocType(DocumentTypeEnum.invoice);
            }
            byte[] msgValue = serializeDocument(document);
            String messageKey = document.getDocType().name() + "_" + getMessageKey(index, KeyTypeEnum.UUID);
            producer.newMessage()
                    .key(messageKey)
                    .value(msgValue)
                    .property("my-key", "my-value")
                    .send();
            msg_sent_counter.incrementAndGet();
            //LOG.info("Send message " + index + "; Key=" + messageKey + "; topic = " + topicName);
        }
        producer.flush();
        LOG.info(String.format("Количество отправленных уникальных сообщений: %s", msg_sent_counter.get()));

    }

    /**
     * Генерация сообщений с тремя версиями на один ключ
     * TODO выделить в отдельный юзкейс
     *
     * @return
     */
    private static void runProducerWithVersions() {
        try (Producer<byte[]> producer =
                     client.newProducer()
                             .topic(config.getTopicName())
                             .compressionType(CompressionType.LZ4)
                             .create()) {

            PulsarSender sender = new PulsarSender(producer);

            int sentUnknownDocumentsCount = sender.sendWithDocIdKey(
                    RoutingDocument::createUnknown,
                    config.getMessageCount()
            );

            int sentOrderDocumentsCount = sender.sendWithDocIdKey(
                    RoutingDocument::createOrder,
                    config.getMessageCount()
            );

            int sentInvoiceDocumentsCount = sender.sendWithDocIdKey(
                    RoutingDocument::createInvoice,
                    config.getMessageCount()
            );


        } catch (PulsarClientException e) {
            LOG.log(Level.SEVERE, "Error producing documents to pulsar", e);
        }
    }

    private static Consumer createAndStartConsumer(String consumerId, Boolean withKeys) throws PulsarClientException {
        String subscriptionName;
        if (withKeys) {
            subscriptionName = SUBSCRIPTION_KEY_NAME;
        } else {
            subscriptionName = SUBSCRIPTION_NAME;
        }
        Consumer<byte[]> consumer = ConsumerCreator.createConsumer(
                client,
                config.getTopicName(),
                consumerId,
                subscriptionName,
                withKeys);
        startConsumer(consumer);
        return consumer;
    }

    private static void startConsumer(Consumer<byte[]> consumer) throws PulsarClientException {
        while (msg_received_counter.get() < config.getMessageCount()) {
            Message message = consumer.receive(1, TimeUnit.SECONDS);
            if (message == null) {
                continue;
            }
            msg_received_counter.incrementAndGet();

            Set consumerRecordSet = consumerRecordSetMap.get(consumer.getConsumerName());
            if (consumerRecordSet == null) {
                consumerRecordSet = new HashSet();
            }
            consumerRecordSet.add(message.getValue());
            consumerRecordSetMap.put(consumer.getConsumerName(), consumerRecordSet);

            //LOG.info(String.format("Consumer %s read record key=%s, number=%s, value=%s, topic=%s",
            //        consumerId,
            //        message.getKey(),
            //        msg_received_counter,
            //        new String(message.getData()),
            //        message.getTopicName()
            //));
        }
    }


    private static String getMessageKey(Integer index, KeyTypeEnum keyType) {
        if (KeyTypeEnum.ONE.equals(keyType)) {
            return "key_1";
        } else if (KeyTypeEnum.UUID.equals(keyType)) {
            return UUID.randomUUID().toString();
        } else if (KeyTypeEnum.NUMERIC.equals(keyType)) {
            return String.format("key_%s", index);
        } else {
            return null;
        }
    }

}
