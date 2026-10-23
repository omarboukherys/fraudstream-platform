package io.fraudstream.producer.api;

import io.fraudstream.producer.generator.TransactionGenerator;
import io.fraudstream.producer.kafka.StreamScheduler;
import io.fraudstream.producer.kafka.TransactionPublisher;
import io.fraudstream.producer.model.Transaction;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * API de pilotage de la démo :
 *   GET  /api/producer/status         état et débit courant
 *   POST /api/producer/rate/{n}       change le débit (lots/seconde)
 *   POST /api/producer/pause | resume
 *   POST /api/producer/inject/{scenario}   injecte un scénario de fraude à la demande (CARD_TESTING, IMPOSSIBLE_TRAVEL, HIGH_AMOUNT, ACCOUNT_TAKEOVER)
 *   POST /api/producer/transactions   publie une transaction fournie dans le corps (tests manuels)
 */
@RestController
@RequestMapping("/api/producer")
public class ProducerController {

    private final StreamScheduler scheduler;
    private final TransactionGenerator generator;
    private final TransactionPublisher publisher;

    public ProducerController(StreamScheduler scheduler, TransactionGenerator generator, TransactionPublisher publisher) {
        this.scheduler = scheduler;
        this.generator = generator;
        this.publisher = publisher;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of("running", scheduler.isRunning(), "ratePerSecond", scheduler.getRate(),
                "customers", generator.customers().size());
    }

    @PostMapping("/rate/{rate}")
    public Map<String, Object> rate(@PathVariable int rate) {
        scheduler.setRate(rate);
        return status();
    }

    @PostMapping("/pause")
    public Map<String, Object> pause() { scheduler.pause(); return status(); }

    @PostMapping("/resume")
    public Map<String, Object> resume() { scheduler.resume(); return status(); }

    @PostMapping("/inject/{scenario}")
    public List<String> inject(@PathVariable String scenario, @RequestParam(required = false) String customerId) {
        var customers = generator.customers();
        var c = customerId == null
                ? customers.get((int) (Math.random() * customers.size()))
                : customers.stream().filter(p -> p.customerId().equals(customerId)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("unknown customer " + customerId));
        List<Transaction> batch = switch (scenario.toUpperCase()) {
            case "CARD_TESTING" -> generator.cardTesting(c);
            case "IMPOSSIBLE_TRAVEL" -> generator.impossibleTravel(c);
            case "HIGH_AMOUNT" -> generator.highAmount(c);
            case "ACCOUNT_TAKEOVER" -> generator.accountTakeover(c);
            default -> throw new IllegalArgumentException("unknown scenario " + scenario);
        };
        batch.forEach(publisher::publish);
        return batch.stream().map(Transaction::transactionId).toList();
    }

    @PostMapping("/transactions")
    public Map<String, String> publish(@Valid @RequestBody Transaction tx) {
        publisher.publish(tx);
        return Map.of("transactionId", tx.transactionId(), "status", "queued");
    }
}
