package io.fraudstream.producer.kafka;

import io.fraudstream.producer.generator.ProducerProperties;
import io.fraudstream.producer.generator.TransactionGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Boucle de génération : toutes les secondes, émet `ratePerSecond` lots d'événements. Pilotable via l'API REST. */
@Component
public class StreamScheduler {

    private static final Logger log = LoggerFactory.getLogger(StreamScheduler.class);

    private final TransactionGenerator generator;
    private final TransactionPublisher publisher;
    private final AtomicInteger ratePerSecond;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public StreamScheduler(TransactionGenerator generator, TransactionPublisher publisher, ProducerProperties props) {
        this.generator = generator;
        this.publisher = publisher;
        this.ratePerSecond = new AtomicInteger(props.ratePerSecond());
    }

    @Scheduled(fixedRate = 1000)
    void tick() {
        if (!running.get()) return;
        int n = ratePerSecond.get();
        int events = 0;
        for (int i = 0; i < n; i++) {
            for (var tx : generator.next()) {
                publisher.publish(tx);
                events++;
            }
        }
        if (log.isDebugEnabled()) log.debug("tick: {} events", events);
    }

    public void setRate(int rate) { ratePerSecond.set(Math.max(0, rate)); }
    public int getRate() { return ratePerSecond.get(); }
    public void pause() { running.set(false); }
    public void resume() { running.set(true); }
    public boolean isRunning() { return running.get(); }
}
