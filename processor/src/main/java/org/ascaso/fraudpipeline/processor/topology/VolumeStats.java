package org.ascaso.fraudpipeline.processor.topology;

import java.math.BigDecimal;
import java.time.Instant;

import org.ascaso.fraudpipeline.events.Transaction;

/** Agregado de una ventana: operaciones, importe total y último evento visto. */
public record VolumeStats(long count, BigDecimal totalEur, Instant lastAt) {

    public static VolumeStats empty() {
        return new VolumeStats(0, BigDecimal.ZERO, Instant.EPOCH);
    }

    VolumeStats add(Transaction transaction) {
        Instant last = transaction.occurredAt().isAfter(lastAt) ? transaction.occurredAt() : lastAt;
        return new VolumeStats(count + 1, totalEur.add(transaction.amountEur()), last);
    }
}
