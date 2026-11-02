package org.ascaso.fraudpipeline.processor.topology;

import java.math.BigDecimal;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Umbrales de detección, fuera del código para poder ajustarlos sin recompilar. */
@ConfigurationProperties("fraud.rules")
public record FraudRules(
        int velocityThreshold,
        Duration velocityWindow,
        Duration velocityAdvance,
        Duration windowGrace,
        double travelMinKm,
        double travelMaxKmh,
        int spikeMinHistory,
        double spikeZScore,
        BigDecimal spikeMinAmountEur,
        Duration alertCooldown,
        Duration dedupWindow) {
}
