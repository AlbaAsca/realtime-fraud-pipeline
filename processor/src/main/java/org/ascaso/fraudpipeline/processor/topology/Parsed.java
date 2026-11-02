package org.ascaso.fraudpipeline.processor.topology;

import org.ascaso.fraudpipeline.events.Transaction;

/** Resultado de interpretar un mensaje crudo: un evento válido o el motivo por el que no lo es. */
sealed interface Parsed {

    record Valid(Transaction transaction) implements Parsed {
    }

    record Invalid(byte[] raw, String reason) implements Parsed {
    }
}
