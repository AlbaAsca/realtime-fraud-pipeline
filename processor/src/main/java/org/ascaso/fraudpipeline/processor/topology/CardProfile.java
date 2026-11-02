package org.ascaso.fraudpipeline.processor.topology;

import java.time.Instant;

import org.ascaso.fraudpipeline.events.Transaction;

/**
 * Lo que el pipeline recuerda de una tarjeta: gasto habitual (media y varianza del logaritmo del
 * importe, con el algoritmo de Welford, sin guardar historial) y su último pago presencial.
 */
public record CardProfile(
        long count,
        double logMean,
        double logM2,
        Instant lastSeenAt,
        String lastCity,
        String lastCountry,
        double lastLatitude,
        double lastLongitude) {

    /** Evita desviaciones ridículas con poco historial, que dispararían alertas falsas. */
    private static final double MIN_STD_DEV = 0.4;

    static CardProfile empty() {
        return new CardProfile(0, 0, 0, null, null, null, 0, 0);
    }

    /** Cuántas desviaciones típicas se aleja este importe de lo habitual, en escala logarítmica. */
    double zScore(double amountEur) {
        double variance = count > 1 ? logM2 / (count - 1) : 0;
        return (Math.log(amountEur) - logMean) / Math.max(Math.sqrt(variance), MIN_STD_DEV);
    }

    CardProfile withAmount(double amountEur) {
        double value = Math.log(amountEur);
        long newCount = count + 1;
        double delta = value - logMean;
        double newMean = logMean + delta / newCount;
        double newM2 = logM2 + delta * (value - newMean);
        return new CardProfile(newCount, newMean, newM2, lastSeenAt, lastCity, lastCountry, lastLatitude, lastLongitude);
    }

    CardProfile withLocation(Transaction t) {
        return new CardProfile(count, logMean, logM2, t.occurredAt(), t.city(), t.country(), t.latitude(), t.longitude());
    }
}
