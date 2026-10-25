package io.fraudstream.producer.generator;

import io.fraudstream.producer.model.Transaction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TransactionGeneratorTest {

    private final TransactionGenerator gen = new TransactionGenerator(50, 0.0, 1L);

    @Test
    void legitimateTransactionsStayInHomeCountryWithUsualDevice() {
        CustomerProfile c = gen.customers().get(0);
        Transaction tx = gen.legitimate(c, Instant.now());
        assertEquals("MA", tx.country());
        assertEquals(c.usualDeviceId(), tx.deviceId());
        assertFalse(tx.isFraudLabel());
        assertTrue(tx.amount().compareTo(BigDecimal.ONE) >= 0);
        assertEquals(2, tx.amount().scale());
    }

    @Test
    void cardTestingIsABurstOfSmallOnlinePaymentsOnNewDevice() {
        CustomerProfile c = gen.customers().get(1);
        List<Transaction> burst = gen.cardTesting(c);
        assertTrue(burst.size() >= 6);
        burst.forEach(tx -> {
            assertTrue(tx.amount().compareTo(BigDecimal.TEN) < 0);
            assertEquals(Transaction.Channel.ONLINE, tx.channel());
            assertNotEquals(c.usualDeviceId(), tx.deviceId());
            assertEquals("CARD_TESTING", tx.fraudScenario());
        });
    }

    @Test
    void impossibleTravelChangesCountryWithinMinutes() {
        List<Transaction> pair = gen.impossibleTravel(gen.customers().get(2));
        assertEquals(2, pair.size());
        assertNotEquals(pair.get(0).country(), pair.get(1).country());
        long seconds = pair.get(1).eventTime().getEpochSecond() - pair.get(0).eventTime().getEpochSecond();
        assertTrue(seconds < 600);
    }

    @Test
    void noScenarioEverDatesAnEventInTheFuture() {
        CustomerProfile c = gen.customers().get(3);
        Instant before = Instant.now();
        List<Transaction> all = new java.util.ArrayList<>();
        all.addAll(gen.cardTesting(c));
        all.addAll(gen.impossibleTravel(c));
        all.addAll(gen.highAmount(c));
        all.addAll(gen.accountTakeover(c));
        Instant after = Instant.now().plusMillis(50);
        all.forEach(tx -> assertFalse(tx.eventTime().isAfter(after), "eventTime dans le futur : " + tx.eventTime()));
        // et le scénario se termine bien "maintenant" (pas dans un passé lointain)
        assertTrue(gen.impossibleTravel(c).get(1).eventTime().isAfter(before.minusSeconds(5)));
    }

    @Test
    void fraudRatioZeroNeverEmitsFraud() {
        for (int i = 0; i < 500; i++) {
            gen.next().forEach(tx -> assertFalse(tx.isFraudLabel()));
        }
    }
}
