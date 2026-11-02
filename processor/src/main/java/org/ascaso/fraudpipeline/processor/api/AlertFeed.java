package org.ascaso.fraudpipeline.processor.api;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import org.ascaso.fraudpipeline.events.FraudAlert;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/** Últimas alertas en memoria y difusión a los navegadores conectados. Un cliente lento pierde eventos, no frena al resto. */
@Component
public class AlertFeed {

    private static final int CAPACITY = 500;

    private final Deque<FraudAlert> recent = new ArrayDeque<>();
    private final Sinks.Many<FraudAlert> sink = Sinks.many().multicast().directBestEffort();

    public void publish(FraudAlert alert) {
        synchronized (recent) {
            recent.addFirst(alert);
            if (recent.size() > CAPACITY) {
                recent.removeLast();
            }
        }
        sink.tryEmitNext(alert);
    }

    public List<FraudAlert> recent(String cardId, int limit) {
        synchronized (recent) {
            return recent.stream()
                    .filter(alert -> cardId == null || alert.cardId().equals(cardId))
                    .limit(limit)
                    .toList();
        }
    }

    public Optional<FraudAlert> find(String alertId) {
        synchronized (recent) {
            return recent.stream().filter(alert -> alert.alertId().equals(alertId)).findFirst();
        }
    }

    public Flux<FraudAlert> live() {
        return sink.asFlux();
    }
}
