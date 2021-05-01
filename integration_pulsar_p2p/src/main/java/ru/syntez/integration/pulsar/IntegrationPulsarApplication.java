package ru.syntez.integration.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import ru.syntez.integration.pulsar.entities.KeyTypeEnum;
import ru.syntez.integration.pulsar.pulsar.ConsumerCreator;
import ru.syntez.integration.pulsar.pulsar.PulsarConfig;
import ru.syntez.integration.pulsar.pulsar.scenarios.ProducerTestScenario;
import ru.syntez.integration.pulsar.pulsar.scenarios.ProducerWithKeys;
import ru.syntez.integration.pulsar.pulsar.scenarios.ProducerWithVersions;
import ru.syntez.integration.pulsar.pulsar.scenarios.ProducerWithoutKeys;
import ru.syntez.integration.pulsar.utils.ResultOutput;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static ru.syntez.integration.pulsar.pulsar.PulsarConfig.loadFromResource;

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
            ProducerTestScenario testScenario = new ProducerWithKeys(client, config);
            testScenario.run(config.getTopicInputFilterName());
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
            ProducerTestScenario testScenario = new ProducerWithKeys(client, config);
            testScenario.run(config.getTopicInputFilterName());
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

        Runnable producerScenario = () -> {
            ProducerTestScenario testScenario;
            if (withKeys && withVersions) testScenario = new ProducerWithVersions(client, config);
            else if (!withKeys) testScenario = new ProducerWithoutKeys(client, config);
            else  testScenario = new ProducerWithKeys(client, config);

            testScenario.run(config.getTopicName());
        };

        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        executorService.execute(producerScenario);


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
            ProducerTestScenario testScenario = new ProducerWithKeys(client, config);
            testScenario.run(config.getTopicName());
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
