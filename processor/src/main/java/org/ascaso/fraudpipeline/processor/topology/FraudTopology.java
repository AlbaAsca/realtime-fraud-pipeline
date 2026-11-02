package org.ascaso.fraudpipeline.processor.topology;

import java.time.Duration;
import java.util.Locale;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.ascaso.fraudpipeline.events.AlertRule;
import org.ascaso.fraudpipeline.events.FraudAlert;
import org.ascaso.fraudpipeline.events.Topics;
import org.ascaso.fraudpipeline.events.Transaction;

/**
 * El pipeline completo. Es una clase sin Spring a propósito: la misma topología corre en producción y
 * en {@code TopologyTestDriver}.
 *
 * <pre>
 * transactions ─ parser ─┬─ inválido ─ transactions.dlq
 *                        └─ válido ─ deduplicación ─┬─ ventana 1 min por país (tumbling) ─ panel
 *                                                   ├─ ventana 60 s/10 s por tarjeta (hopping) ─ VELOCITY ─┐
 *                                                   └─ perfil de tarjeta ─ IMPOSSIBLE_TRAVEL, AMOUNT_SPIKE ─┤
 *                                                                        fraud-alerts ─ enfriamiento ───────┘
 * </pre>
 */
public final class FraudTopology {

    public static final String SEEN_TRANSACTIONS = "seen-transactions";
    public static final String CARD_PROFILES = "card-profiles";
    public static final String ALERT_COOLDOWN = "alert-cooldown";
    public static final String CARD_ACTIVITY = "card-activity-60s";
    public static final String VOLUME_BY_COUNTRY = "volume-by-country-1m";

    private static final Duration VOLUME_WINDOW = Duration.ofMinutes(1);
    private static final Duration VOLUME_GRACE = Duration.ofMinutes(1);
    private static final Duration VOLUME_RETENTION = Duration.ofHours(3);
    private static final Duration ACTIVITY_RETENTION = Duration.ofMinutes(15);

    private static final Serde<String> STRINGS = Serdes.String();
    private static final Serde<Transaction> TRANSACTIONS = JsonSerde.of(Transaction.class);
    private static final Serde<FraudAlert> ALERTS = JsonSerde.of(FraudAlert.class);
    private static final Serde<VolumeStats> STATS = JsonSerde.of(VolumeStats.class);

    private FraudTopology() {
    }

    public static KStream<String, FraudAlert> build(StreamsBuilder builder, FraudRules rules, PipelineMetrics metrics) {
        addStores(builder, rules);

        KStream<String, Transaction> valid = parseAndRejectInvalid(builder, metrics);
        KStream<String, Transaction> unique = valid.processValues(
                () -> new DeduplicationProcessor(rules.dedupWindow(), metrics), Named.as("deduplicate"), SEEN_TRANSACTIONS);

        aggregateVolumeByCountry(unique);
        KStream<String, FraudAlert> velocityAlerts = detectVelocity(unique, rules);
        KStream<String, FraudAlert> profileAlerts = unique.processValues(
                () -> new CardProfileProcessor(rules), Named.as("card-profile-rules"), CARD_PROFILES);

        KStream<String, FraudAlert> alerts = velocityAlerts
                .merge(profileAlerts, Named.as("all-alerts"))
                .processValues(() -> new AlertCooldownProcessor(rules.alertCooldown(), metrics),
                        Named.as("alert-cooldown"), ALERT_COOLDOWN);
        alerts.to(Topics.FRAUD_ALERTS, Produced.with(STRINGS, ALERTS).withName("to-fraud-alerts"));
        return alerts;
    }

    private static void addStores(StreamsBuilder builder, FraudRules rules) {
        Duration dedup = rules.dedupWindow();
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(SEEN_TRANSACTIONS, dedup.multipliedBy(2), dedup, false), STRINGS, Serdes.Long()));
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(CARD_PROFILES), STRINGS, JsonSerde.of(CardProfile.class)));
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ALERT_COOLDOWN), STRINGS, Serdes.Long()));
    }

    /** La fuente lee bytes, no JSON: así un mensaje roto es un dato más y no un fallo de deserialización. */
    private static KStream<String, Transaction> parseAndRejectInvalid(StreamsBuilder builder, PipelineMetrics metrics) {
        var result = builder.stream(Topics.TRANSACTIONS, Consumed.with(STRINGS, Serdes.ByteArray()).withName("transactions-source"))
                .processValues(TransactionParser::new, Named.as("parse-and-validate"))
                .split(Named.as("parsed-"))
                .branch((key, parsed) -> parsed instanceof Parsed.Invalid, Branched.as("invalid"))
                .defaultBranch(Branched.as("valid"));

        result.get("parsed-invalid")
                .mapValues(parsed -> (Parsed.Invalid) parsed, Named.as("as-invalid"))
                .processValues(() -> new DeadLetterProcessor(metrics), Named.as("dead-letter-headers"))
                .to(Topics.TRANSACTIONS_DLQ, Produced.with(STRINGS, Serdes.ByteArray()).withName("to-dlq"));

        return result.get("parsed-valid")
                .mapValues(parsed -> ((Parsed.Valid) parsed).transaction(), Named.as("as-transaction"));
    }

    /** Ventana fija de 1 minuto, para la gráfica. Agrupar por país obliga a Kafka Streams a reparticionar. */
    private static void aggregateVolumeByCountry(KStream<String, Transaction> transactions) {
        transactions
                .groupBy((card, transaction) -> transaction.country(), Grouped.with("by-country", STRINGS, TRANSACTIONS))
                .windowedBy(TimeWindows.ofSizeAndGrace(VOLUME_WINDOW, VOLUME_GRACE))
                .aggregate(VolumeStats::empty, (country, transaction, stats) -> stats.add(transaction),
                        Materialized.<String, VolumeStats, WindowStore<Bytes, byte[]>>as(VOLUME_BY_COUNTRY)
                                .withKeySerde(STRINGS).withValueSerde(STATS).withRetention(VOLUME_RETENTION));
    }

    /**
     * Ventana solapada: una ráfaga que cruce el final de un minuto seguiría cabiendo entera en otra.
     * Sin caché, para reaccionar con cada evento y no al vaciarla.
     */
    private static KStream<String, FraudAlert> detectVelocity(KStream<String, Transaction> transactions, FraudRules rules) {
        KTable<Windowed<String>, VolumeStats> activity = transactions
                .groupByKey(Grouped.with(STRINGS, TRANSACTIONS))
                .windowedBy(TimeWindows.ofSizeAndGrace(rules.velocityWindow(), rules.windowGrace()).advanceBy(rules.velocityAdvance()))
                .aggregate(VolumeStats::empty, (card, transaction, stats) -> stats.add(transaction),
                        Materialized.<String, VolumeStats, WindowStore<Bytes, byte[]>>as(CARD_ACTIVITY)
                                .withKeySerde(STRINGS).withValueSerde(STATS).withRetention(ACTIVITY_RETENTION).withCachingDisabled());

        return activity.toStream(Named.as("card-activity-changes"))
                .filter((window, stats) -> stats != null && stats.count() >= rules.velocityThreshold(), Named.as("over-velocity"))
                .map((window, stats) -> {
                    String card = window.key();
                    String reason = String.format(Locale.ROOT, "%d operaciones en %d s (%.2f €)",
                            stats.count(), rules.velocityWindow().toSeconds(), stats.totalEur());
                    String id = FraudAlert.idFor(card, AlertRule.VELOCITY, String.valueOf(window.window().start()));
                    return KeyValue.pair(card, new FraudAlert(id, card, AlertRule.VELOCITY, reason, stats.totalEur(), null, null, stats.lastAt()));
                }, Named.as("velocity-alert"));
    }
}
