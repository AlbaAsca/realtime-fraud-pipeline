package org.ascaso.fraudpipeline.processor.topology;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.ascaso.fraudpipeline.events.EventJson;
import org.ascaso.fraudpipeline.events.Transaction;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import tools.jackson.core.JacksonException;

/**
 * Convierte bytes en eventos sin lanzar nunca: un mensaje venenoso no puede tumbar el pipeline.
 * Además fija el timestamp del registro al del evento, para que las ventanas usen event time.
 */
class TransactionParser implements FixedKeyProcessor<String, byte[], Parsed> {

    private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

    private FixedKeyProcessorContext<String, Parsed> context;

    @Override
    public void init(FixedKeyProcessorContext<String, Parsed> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, byte[]> record) {
        Parsed parsed = parse(record.key(), record.value());
        if (parsed instanceof Parsed.Valid valid) {
            context.forward(record.<Parsed>withValue(parsed).withTimestamp(valid.transaction().occurredAt().toEpochMilli()));
        } else {
            context.forward(record.withValue(parsed));
        }
    }

    static Parsed parse(String key, byte[] raw) {
        if (raw == null) {
            return new Parsed.Invalid(null, "empty-value");
        }
        Transaction transaction;
        try {
            transaction = EventJson.MAPPER.readValue(raw, Transaction.class);
        } catch (JacksonException e) {
            return new Parsed.Invalid(raw, "malformed-json: " + e.getOriginalMessage());
        }
        Set<ConstraintViolation<Transaction>> violations = VALIDATOR.validate(transaction);
        if (!violations.isEmpty()) {
            String detail = violations.stream()
                    .map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));
            return new Parsed.Invalid(raw, "invalid-schema: " + detail);
        }
        // Sin esta garantía, el estado de una tarjeta podría estar repartido entre particiones.
        if (!transaction.cardId().equals(key)) {
            return new Parsed.Invalid(raw, "key-mismatch: key must be the cardId");
        }
        return new Parsed.Valid(transaction);
    }
}
