package io.fraudstream.producer.generator;

import java.math.BigDecimal;

/** Comportement "normal" d'un client synthétique : sert de base pour générer des transactions plausibles. */
public record CustomerProfile(
        String customerId,
        String accountId,
        String homeCountry,
        String homeCity,
        String usualDeviceId,
        BigDecimal typicalAmount,   // montant moyen habituel (MAD)
        double onlineRatio          // part des achats en ligne
) {}
