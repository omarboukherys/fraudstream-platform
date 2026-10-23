package io.fraudstream.producer.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Événement "transaction" publié dans le topic Kafka {@code transactions.raw}.
 * Le schéma JSON est le contrat partagé avec le job Spark (voir streaming-spark/.../TransactionSchema.scala).
 */
public record Transaction(
        String transactionId,
        String customerId,
        String accountId,
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX", timezone = "UTC")
        Instant eventTime,
        BigDecimal amount,
        String currency,
        String merchantId,
        String merchantCategory,
        Channel channel,
        String country,
        String city,
        String deviceId,
        String ipAddress,
        /** Vérité terrain synthétique : sert uniquement à évaluer la détection (jamais lue par le scoring). */
        boolean isFraudLabel,
        /** Scénario ayant généré la fraude (null si transaction légitime). */
        String fraudScenario
) {
    public enum Channel { CARD_PRESENT, ONLINE, ATM, TRANSFER }
}
