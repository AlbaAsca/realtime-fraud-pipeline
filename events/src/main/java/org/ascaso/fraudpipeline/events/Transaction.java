package org.ascaso.fraudpipeline.events;

import java.math.BigDecimal;
import java.time.Instant;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

/**
 * Evento de entrada, en JSON y con clave {@code cardId}. Las anotaciones son el contrato: lo que no
 * las cumple va a la DLQ. Los campos desconocidos se ignoran, así que añadir uno no rompe a nadie.
 */
public record Transaction(
        @NotBlank String transactionId,
        @NotBlank String cardId,
        @NotBlank String merchant,
        @NotBlank String category,
        @NotNull @DecimalMin("0.01") @DecimalMax("100000.00") BigDecimal amountEur,
        @NotNull Channel channel,
        @NotNull @Pattern(regexp = "[A-Z]{2}") String country,
        @NotBlank String city,
        @Min(-90) @Max(90) double latitude,
        @Min(-180) @Max(180) double longitude,
        @NotNull Instant occurredAt) {
}
