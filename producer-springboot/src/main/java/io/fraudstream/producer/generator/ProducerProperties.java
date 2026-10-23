package io.fraudstream.producer.generator;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Paramètres du générateur, surchargeables par variables d'environnement (PRODUCER_RATE_PER_SECOND, ...). */
@ConfigurationProperties(prefix = "producer")
public record ProducerProperties(
        int ratePerSecond,
        double fraudRatio,
        int customers,
        long seed,
        String topic
) {}
