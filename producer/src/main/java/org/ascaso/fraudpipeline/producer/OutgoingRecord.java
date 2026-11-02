package org.ascaso.fraudpipeline.producer;

/** Mensaje listo para enviar: el valor es texto porque a propósito no siempre es JSON válido. */
public record OutgoingRecord(String key, String value, long timestamp) {
}
