package org.ascaso.fraudpipeline.events;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Evento de salida. El {@code alertId} se deriva de la causa, no es aleatorio: si se reprocesa el
 * mismo hecho sale el mismo id, y cualquier consumidor puede deduplicar por él.
 */
public record FraudAlert(
        String alertId,
        String cardId,
        AlertRule rule,
        String reason,
        BigDecimal amountEur,
        String country,
        String city,
        Instant occurredAt) {

    public static String idFor(String cardId, AlertRule rule, String cause) {
        return UUID.nameUUIDFromBytes((cardId + '|' + rule + '|' + cause).getBytes(StandardCharsets.UTF_8)).toString();
    }
}
