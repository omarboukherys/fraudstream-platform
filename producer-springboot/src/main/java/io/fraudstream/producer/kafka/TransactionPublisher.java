package io.fraudstream.producer.kafka;

import io.fraudstream.producer.generator.ProducerProperties;
import io.fraudstream.producer.model.Transaction;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Publie les transactions dans Kafka. La clé du message est le customerId : toutes les transactions d'un client
 * atterrissent dans la même partition, ce qui garantit l'ordre par client côté Spark (indispensable pour
 * détecter "impossible travel" ou "card testing").
 */
@Component
public class TransactionPublisher {

    private static final Logger log = LoggerFactory.getLogger(TransactionPublisher.class);

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final String topic;
    private final Counter published;
    private final Counter failed;
    private final Counter fraudInjected;

    public TransactionPublisher(KafkaTemplate<String, Transaction> kafkaTemplate,
                                ProducerProperties props,
                                MeterRegistry registry) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = props.topic();
        this.published = registry.counter("fraudstream.producer.published");
        this.failed = registry.counter("fraudstream.producer.failed");
        this.fraudInjected = registry.counter("fraudstream.producer.fraud_injected");
    }

    public CompletableFuture<Void> publish(Transaction tx) {
        return kafkaTemplate.send(topic, tx.customerId(), tx)
                .thenAccept(result -> {
                    published.increment();
                    if (tx.isFraudLabel()) fraudInjected.increment();
                    if (log.isDebugEnabled()) {
                        log.debug("sent {} -> partition {} offset {}", tx.transactionId(),
                                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    }
                })
                .exceptionally(ex -> {
                    failed.increment();
                    log.warn("failed to publish {}: {}", tx.transactionId(), ex.getMessage());
                    return null;
                });
    }
}
