package org.ascaso.fraudpipeline.events;

/** Reglas de detección que pueden disparar una alerta. */
public enum AlertRule {
    /** Demasiadas operaciones de una tarjeta en un minuto: el patrón típico de probar tarjetas robadas. */
    VELOCITY,
    /** Dos pagos presenciales demasiado lejos para el tiempo que los separa. */
    IMPOSSIBLE_TRAVEL,
    /** Un importe muy por encima de lo habitual en esa tarjeta. */
    AMOUNT_SPIKE
}
