package io.fraudstream.producer.generator;

import io.fraudstream.producer.model.Transaction;
import io.fraudstream.producer.model.Transaction.Channel;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Générateur de transactions synthétiques.
 *
 * Chaque appel à {@link #next()} renvoie une liste de 1..N transactions : une transaction légitime la plupart du temps,
 * ou un "scénario de fraude" (plusieurs transactions corrélées) avec la probabilité {@code fraudRatio}.
 *
 * Les scénarios reproduisent des patterns connus du métier :
 *  - CARD_TESTING      : rafale de petits montants en ligne sur un nouveau device (test de carte volée)
 *  - IMPOSSIBLE_TRAVEL : deux achats physiques dans deux pays éloignés à quelques minutes d'intervalle
 *  - HIGH_AMOUNT       : montant très supérieur à l'habitude du client, à l'étranger
 *  - ACCOUNT_TAKEOVER  : nouveau device + virement important vers un bénéficiaire inconnu
 */
@Component
public class TransactionGenerator {

    static final String[] MOROCCAN_CITIES = {"Casablanca", "Rabat", "Marrakech", "Tanger", "Fès", "Agadir", "Oujda", "Meknès"};
    static final String[] MERCHANT_CATEGORIES = {"GROCERY", "FUEL", "RESTAURANT", "ELECTRONICS", "TRAVEL", "CLOTHING", "PHARMACY", "TELECOM", "CRYPTO_EXCHANGE", "GAMBLING"};
    static final String[] FOREIGN_COUNTRIES = {"FR", "ES", "US", "AE", "TR", "NG", "CN", "GB"};

    private final List<CustomerProfile> customers = new ArrayList<>();
    private final double fraudRatio;
    private final Random seeded;

    @org.springframework.beans.factory.annotation.Autowired
    public TransactionGenerator(ProducerProperties props) {
        this(props.customers(), props.fraudRatio(), props.seed());
    }

    TransactionGenerator(int customerCount, double fraudRatio, long seed) {
        this.fraudRatio = fraudRatio;
        this.seeded = new Random(seed);
        for (int i = 0; i < customerCount; i++) {
            String city = MOROCCAN_CITIES[seeded.nextInt(MOROCCAN_CITIES.length)];
            BigDecimal typical = BigDecimal.valueOf(80 + seeded.nextInt(900)); // 80..980 MAD
            customers.add(new CustomerProfile(
                    String.format("C%05d", i),
                    String.format("ACC%08d", 10_000_000 + i),
                    "MA",
                    city,
                    "dev-" + UUID.nameUUIDFromBytes(("dev" + i).getBytes()).toString().substring(0, 8),
                    typical,
                    0.2 + seeded.nextDouble() * 0.5));
        }
    }

    public List<CustomerProfile> customers() { return List.copyOf(customers); }

    /** Produit le prochain lot d'événements (1 transaction légitime, ou un scénario de fraude multi-événements). */
    public List<Transaction> next() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        CustomerProfile c = customers.get(rnd.nextInt(customers.size()));
        if (rnd.nextDouble() < fraudRatio) {
            return switch (rnd.nextInt(4)) {
                case 0 -> cardTesting(c);
                case 1 -> impossibleTravel(c);
                case 2 -> highAmount(c);
                default -> accountTakeover(c);
            };
        }
        return List.of(legitimate(c, Instant.now()));
    }

    // ---------------------------------------------------------------- transactions légitimes

    public Transaction legitimate(CustomerProfile c, Instant at) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        boolean online = rnd.nextDouble() < c.onlineRatio();
        Channel channel = online ? Channel.ONLINE : (rnd.nextInt(10) == 0 ? Channel.ATM : Channel.CARD_PRESENT);
        // log-normale autour du montant habituel : beaucoup de petits achats, quelques gros
        BigDecimal amount = lognormalAround(c.typicalAmount(), 0.6);
        String mcc = MERCHANT_CATEGORIES[rnd.nextInt(8)]; // jamais CRYPTO/GAMBLING en légitime
        return build(c, at, amount, mcc, channel, c.homeCountry(), c.homeCity(), c.usualDeviceId(), false, null);
    }

    // ---------------------------------------------------------------- scénarios de fraude

    public List<Transaction> cardTesting(CustomerProfile c) {
        List<Transaction> out = new ArrayList<>();
        Instant t0 = Instant.now();
        String newDevice = "dev-" + randomHex(8);
        int burst = 6 + ThreadLocalRandom.current().nextInt(6);
        for (int i = 0; i < burst; i++) {
            BigDecimal amount = BigDecimal.valueOf(1 + ThreadLocalRandom.current().nextInt(9)); // 1..9 MAD
            out.add(build(c, t0.plusSeconds(i * 3L), amount, "ELECTRONICS", Channel.ONLINE,
                    c.homeCountry(), c.homeCity(), newDevice, true, "CARD_TESTING"));
        }
        return out;
    }

    public List<Transaction> impossibleTravel(CustomerProfile c) {
        Instant t0 = Instant.now();
        String abroad = FOREIGN_COUNTRIES[ThreadLocalRandom.current().nextInt(FOREIGN_COUNTRIES.length)];
        return List.of(
                build(c, t0, lognormalAround(c.typicalAmount(), 0.4), "RESTAURANT", Channel.CARD_PRESENT,
                        c.homeCountry(), c.homeCity(), c.usualDeviceId(), true, "IMPOSSIBLE_TRAVEL"),
                build(c, t0.plusSeconds(240), lognormalAround(c.typicalAmount().multiply(BigDecimal.valueOf(3)), 0.4),
                        "ELECTRONICS", Channel.CARD_PRESENT, abroad, "Unknown", c.usualDeviceId(), true, "IMPOSSIBLE_TRAVEL"));
    }

    public List<Transaction> highAmount(CustomerProfile c) {
        String abroad = FOREIGN_COUNTRIES[ThreadLocalRandom.current().nextInt(FOREIGN_COUNTRIES.length)];
        BigDecimal amount = c.typicalAmount().multiply(BigDecimal.valueOf(12 + ThreadLocalRandom.current().nextInt(20)));
        return List.of(build(c, Instant.now(), amount, "TRAVEL", Channel.ONLINE,
                abroad, "Unknown", c.usualDeviceId(), true, "HIGH_AMOUNT"));
    }

    public List<Transaction> accountTakeover(CustomerProfile c) {
        String newDevice = "dev-" + randomHex(8);
        BigDecimal amount = c.typicalAmount().multiply(BigDecimal.valueOf(8 + ThreadLocalRandom.current().nextInt(15)));
        return List.of(build(c, Instant.now(), amount, "CRYPTO_EXCHANGE", Channel.TRANSFER,
                c.homeCountry(), c.homeCity(), newDevice, true, "ACCOUNT_TAKEOVER"));
    }

    // ---------------------------------------------------------------- helpers

    private Transaction build(CustomerProfile c, Instant at, BigDecimal amount, String mcc, Channel channel,
                              String country, String city, String deviceId, boolean fraud, String scenario) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        return new Transaction(
                UUID.randomUUID().toString(),
                c.customerId(),
                c.accountId(),
                at,
                amount.setScale(2, RoundingMode.HALF_UP),
                "MAD",
                "M" + (1000 + rnd.nextInt(9000)),
                mcc,
                channel,
                country,
                city,
                deviceId,
                rnd.nextInt(256) + "." + rnd.nextInt(256) + "." + rnd.nextInt(256) + "." + rnd.nextInt(256),
                fraud,
                scenario);
    }

    static BigDecimal lognormalAround(BigDecimal mean, double sigma) {
        double g = ThreadLocalRandom.current().nextGaussian();
        double v = mean.doubleValue() * Math.exp(sigma * g - sigma * sigma / 2);
        return BigDecimal.valueOf(Math.max(1.0, v));
    }

    private static String randomHex(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) sb.append(Integer.toHexString(ThreadLocalRandom.current().nextInt(16)));
        return sb.toString();
    }
}
