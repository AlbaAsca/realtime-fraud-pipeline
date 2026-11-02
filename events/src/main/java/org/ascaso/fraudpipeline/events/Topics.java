package org.ascaso.fraudpipeline.events;

/** Nombres de los tópicos del pipeline. */
public final class Topics {

    public static final String TRANSACTIONS = "transactions";
    public static final String TRANSACTIONS_DLQ = "transactions.dlq";
    public static final String FRAUD_ALERTS = "fraud-alerts";
    public static final String FRAUD_ALERTS_DLQ = "fraud-alerts.dlq";

    private Topics() {
    }
}
