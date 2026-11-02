package org.ascaso.fraudpipeline.producer;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Volumen de tráfico, frecuencia de ataques y proporción de datos sucios. */
@ConfigurationProperties("generator")
public record GeneratorProperties(
        int cards,
        int transactionsPerSecond,
        Duration attackEvery,
        int minHistoryForSpike,
        double duplicateRate,
        double malformedRate,
        double invalidRate,
        double lateRate) {
}
