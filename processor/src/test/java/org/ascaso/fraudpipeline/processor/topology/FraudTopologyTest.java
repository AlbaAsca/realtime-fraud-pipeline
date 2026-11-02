package org.ascaso.fraudpipeline.processor.topology;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.ascaso.fraudpipeline.events.AlertRule;
import org.ascaso.fraudpipeline.events.Channel;
import org.ascaso.fraudpipeline.events.EventJson;
import org.ascaso.fraudpipeline.events.FraudAlert;
import org.ascaso.fraudpipeline.events.Topics;
import org.ascaso.fraudpipeline.events.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** La lógica de streams sin broker: tiempo controlado por el test y resultados deterministas. */
class FraudTopologyTest {

    private static final FraudRules RULES = new FraudRules(8, Duration.ofSeconds(60), Duration.ofSeconds(10),
            Duration.ofSeconds(30), 500, 1000, 5, 3.5, new BigDecimal("300"), Duration.ofMinutes(5), Duration.ofMinutes(10));
    private static final Instant T0 = Instant.parse("2026-11-02T10:00:00Z");
    private static final String CARD = "card-0001";

    private record Place(String city, String country, double latitude, double longitude) {
    }

    private static final Place MADRID = new Place("Madrid", "ES", 40.4168, -3.7038);
    private static final Place ALCALA = new Place("Alcalá", "ES", 40.4818, -3.3635);
    private static final Place TOKYO = new Place("Tokio", "JP", 35.6762, 139.6503);

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, byte[]> input;
    private TestOutputTopic<String, String> alerts;
    private TestOutputTopic<String, byte[]> deadLetters;
    private PipelineMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new PipelineMetrics(new SimpleMeterRegistry());
        StreamsBuilder builder = new StreamsBuilder();
        FraudTopology.build(builder, RULES, metrics);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(builder.build(), config);

        input = driver.createInputTopic(Topics.TRANSACTIONS, new StringSerializer(), new ByteArraySerializer());
        alerts = driver.createOutputTopic(Topics.FRAUD_ALERTS, new StringDeserializer(), new StringDeserializer());
        deadLetters = driver.createOutputTopic(Topics.TRANSACTIONS_DLQ, new StringDeserializer(), new ByteArrayDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static Transaction transaction(String id, Instant at, String amount, Channel channel, Place place) {
        return new Transaction(id, CARD, "Zara", "fashion", new BigDecimal(amount), channel,
                place.country(), place.city(), place.latitude(), place.longitude(), at);
    }

    private static Transaction pos(Instant at, String amount, Place place) {
        return transaction(UUID.randomUUID().toString(), at, amount, Channel.POS, place);
    }

    private static Transaction online(Instant at, String amount) {
        return transaction(UUID.randomUUID().toString(), at, amount, Channel.ONLINE, MADRID);
    }

    private void send(Transaction transaction) {
        input.pipeInput(transaction.cardId(), EventJson.MAPPER.writeValueAsBytes(transaction), transaction.occurredAt());
    }

    private List<FraudAlert> emittedAlerts() {
        return alerts.readValuesToList().stream().map(json -> EventJson.MAPPER.readValue(json, FraudAlert.class)).toList();
    }

    private static String header(org.apache.kafka.streams.test.TestRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    // --- Entrada: nada de lo que llegue debe tumbar el pipeline ---

    @Test
    void normalTransactionRaisesNothing() {
        send(pos(T0, "25.00", MADRID));

        assertThat(alerts.isEmpty()).isTrue();
        assertThat(deadLetters.isEmpty()).isTrue();
        assertThat(metrics.accepted()).isEqualTo(1);
    }

    @Test
    void malformedJsonGoesToDlqUntouchedWithReasonAndOrigin() {
        byte[] garbage = "{not json".getBytes(StandardCharsets.UTF_8);

        input.pipeInput(CARD, garbage, T0);

        var record = deadLetters.readRecord();
        assertThat(record.key()).isEqualTo(CARD);
        assertThat(record.value()).isEqualTo(garbage);
        assertThat(header(record, DeadLetterProcessor.REASON_HEADER)).startsWith("malformed-json");
        assertThat(header(record, DeadLetterProcessor.ORIGIN_HEADER)).startsWith(Topics.TRANSACTIONS + "-");
        assertThat(metrics.invalid()).isEqualTo(1);
        assertThat(metrics.accepted()).isZero();
    }

    @Test
    void schemaViolationGoesToDlqNamingTheField() {
        send(pos(T0, "-5.00", MADRID));

        assertThat(header(deadLetters.readRecord(), DeadLetterProcessor.REASON_HEADER))
                .startsWith("invalid-schema").contains("amountEur");
    }

    @Test
    void missingFieldGoesToDlq() {
        input.pipeInput(CARD, "{\"transactionId\":\"t1\"}".getBytes(StandardCharsets.UTF_8), T0);

        assertThat(deadLetters.isEmpty()).isFalse();
    }

    @Test
    void keyDifferentFromCardGoesToDlq() {
        Transaction transaction = pos(T0, "25.00", MADRID);

        input.pipeInput("other-card", EventJson.MAPPER.writeValueAsBytes(transaction), T0);

        assertThat(header(deadLetters.readRecord(), DeadLetterProcessor.REASON_HEADER)).startsWith("key-mismatch");
    }

    @Test
    void emptyValueGoesToDlq() {
        input.pipeInput(CARD, (byte[]) null, T0);

        assertThat(header(deadLetters.readRecord(), DeadLetterProcessor.REASON_HEADER)).isEqualTo("empty-value");
    }

    @Test
    void pipelineKeepsWorkingAfterPoisonMessage() {
        input.pipeInput(CARD, "{not json".getBytes(StandardCharsets.UTF_8), T0);
        send(pos(T0.plusSeconds(1), "25.00", MADRID));

        assertThat(metrics.accepted()).isEqualTo(1);
    }

    // --- Idempotencia ---

    @Test
    void duplicateIsCountedOnce() {
        Transaction transaction = pos(T0, "25.00", MADRID);

        send(transaction);
        send(transaction);

        assertThat(metrics.accepted()).isEqualTo(1);
        assertThat(metrics.duplicates()).isEqualTo(1);
        assertThat(volumeInMinute("ES", T0)).isEqualTo(1);
    }

    @Test
    void duplicatesCannotTriggerVelocity() {
        Transaction transaction = online(T0, "1.00");
        for (int i = 0; i < 20; i++) {
            send(transaction);
        }

        assertThat(alerts.isEmpty()).isTrue();
    }

    // --- Ventanas ---

    private long volumeInMinute(String country, Instant minute) {
        WindowStore<String, org.apache.kafka.streams.state.ValueAndTimestamp<VolumeStats>> store =
                driver.getTimestampedWindowStore(FraudTopology.VOLUME_BY_COUNTRY);
        var value = store.fetch(country, minute.toEpochMilli());
        return value == null ? 0 : value.value().count();
    }

    @Test
    void tumblingWindowCountsPerMinuteAndCountry() {
        send(pos(T0, "10.00", MADRID));
        send(pos(T0.plusSeconds(30), "10.00", MADRID));
        send(pos(T0.plusSeconds(61), "10.00", MADRID));

        assertThat(volumeInMinute("ES", T0)).isEqualTo(2);
        assertThat(volumeInMinute("ES", T0.plusSeconds(60))).isEqualTo(1);
        assertThat(volumeInMinute("JP", T0)).isZero();
    }

    @Test
    void lateEventWithinGraceCountsInItsOwnMinute() {
        send(pos(T0.plusSeconds(90), "10.00", MADRID));
        send(online(T0.plusSeconds(10), "10.00"));

        assertThat(volumeInMinute("ES", T0)).isEqualTo(1);
    }

    @Test
    void eventLaterThanGraceIsLeftOutOfTheWindow() {
        send(pos(T0.plusSeconds(300), "10.00", MADRID));
        send(online(T0.plusSeconds(10), "10.00"));

        assertThat(volumeInMinute("ES", T0)).isZero();
    }

    @Test
    void hoppingWindowCountsEventInEveryOverlappingWindow() {
        send(online(T0.plusSeconds(5), "10.00"));

        WindowStore<String, org.apache.kafka.streams.state.ValueAndTimestamp<VolumeStats>> store =
                driver.getTimestampedWindowStore(FraudTopology.CARD_ACTIVITY);
        try (var windows = store.fetch(CARD, T0.minusSeconds(60), T0.plusSeconds(60))) {
            assertThat(windows).toIterable().hasSize(6);
        }
    }

    // --- Regla VELOCITY ---

    @Test
    void burstRaisesOneVelocityAlertDespiteOverlappingWindows() {
        for (int i = 0; i < 12; i++) {
            send(online(T0.plusSeconds(i * 2), "1.50"));
        }

        List<FraudAlert> emitted = emittedAlerts();
        assertThat(emitted).hasSize(1);
        assertThat(emitted.getFirst().rule()).isEqualTo(AlertRule.VELOCITY);
        assertThat(emitted.getFirst().cardId()).isEqualTo(CARD);
        assertThat(metrics.suppressed()).isPositive();
    }

    @Test
    void burstAcrossAMinuteBoundaryIsStillDetected() {
        for (int i = 0; i < 8; i++) {
            send(online(T0.minusSeconds(8).plusSeconds(i * 2), "1.50"));
        }

        assertThat(emittedAlerts()).extracting(FraudAlert::rule).containsExactly(AlertRule.VELOCITY);
    }

    @Test
    void belowThresholdRaisesNothing() {
        for (int i = 0; i < 7; i++) {
            send(online(T0.plusSeconds(i), "1.50"));
        }

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void spreadOutActivityRaisesNothing() {
        for (int i = 0; i < 30; i++) {
            send(online(T0.plusSeconds(i * 30), "1.50"));
        }

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void newBurstAfterCooldownAlertsAgain() {
        for (int i = 0; i < 8; i++) {
            send(online(T0.plusSeconds(i), "1.50"));
        }
        for (int i = 0; i < 8; i++) {
            send(online(T0.plus(Duration.ofMinutes(6)).plusSeconds(i), "1.50"));
        }

        assertThat(emittedAlerts()).hasSize(2);
    }

    // --- Regla IMPOSSIBLE_TRAVEL ---

    @Test
    void farPaymentsTooCloseInTimeRaiseAlert() {
        send(pos(T0, "20.00", MADRID));
        send(pos(T0.plusSeconds(120), "20.00", TOKYO));

        List<FraudAlert> emitted = emittedAlerts();
        assertThat(emitted).extracting(FraudAlert::rule).containsExactly(AlertRule.IMPOSSIBLE_TRAVEL);
        assertThat(emitted.getFirst().reason()).contains("Madrid", "Tokio");
        assertThat(emitted.getFirst().city()).isEqualTo("Tokio");
    }

    @Test
    void farPaymentsWithEnoughTimeRaiseNothing() {
        send(pos(T0, "20.00", MADRID));
        send(pos(T0.plus(Duration.ofHours(15)), "20.00", TOKYO));

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void nearbyPaymentsSecondsApartRaiseNothing() {
        send(pos(T0, "20.00", MADRID));
        send(pos(T0.plusSeconds(1), "20.00", ALCALA));

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void onlinePaymentsSayNothingAboutLocation() {
        send(pos(T0, "20.00", MADRID));
        send(transaction("online-1", T0.plusSeconds(60), "20.00", Channel.ONLINE, TOKYO));

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void lateEventDoesNotTriggerTravelNorMoveLastLocation() {
        send(pos(T0.plusSeconds(60), "20.00", MADRID));
        send(pos(T0, "20.00", TOKYO));
        send(pos(T0.plusSeconds(120), "20.00", MADRID));

        assertThat(alerts.isEmpty()).isTrue();
    }

    // --- Regla AMOUNT_SPIKE ---

    private void buildHistory(int transactions) {
        String[] amounts = {"20.00", "30.00", "25.00", "40.00", "18.00", "35.00"};
        for (int i = 0; i < transactions; i++) {
            send(pos(T0.plus(Duration.ofHours(i)), amounts[i % amounts.length], MADRID));
        }
    }

    @Test
    void hugeAmountOnCardWithHistoryRaisesAlert() {
        buildHistory(6);

        send(pos(T0.plus(Duration.ofDays(1)), "1800.00", MADRID));

        List<FraudAlert> emitted = emittedAlerts();
        assertThat(emitted).extracting(FraudAlert::rule).containsExactly(AlertRule.AMOUNT_SPIKE);
        assertThat(emitted.getFirst().amountEur()).isEqualByComparingTo("1800.00");
    }

    @Test
    void withoutEnoughHistoryThereIsNoBaselineToCompare() {
        buildHistory(4);

        send(pos(T0.plus(Duration.ofDays(1)), "1800.00", MADRID));

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void ordinaryAmountRaisesNothing() {
        buildHistory(6);

        send(pos(T0.plus(Duration.ofDays(1)), "45.00", MADRID));

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void relativelyHighButSmallAmountRaisesNothing() {
        buildHistory(6);

        send(pos(T0.plus(Duration.ofDays(1)), "250.00", MADRID));

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void suspiciousAmountDoesNotPolluteTheBaseline() {
        buildHistory(6);
        KeyValueStore<String, CardProfile> profiles = driver.getKeyValueStore(FraudTopology.CARD_PROFILES);
        double before = profiles.get(CARD).logMean();

        send(pos(T0.plus(Duration.ofDays(1)), "1800.00", MADRID));

        assertThat(profiles.get(CARD).logMean()).isEqualTo(before);
        assertThat(profiles.get(CARD).count()).isEqualTo(6);
    }

    // --- Salida ---

    @Test
    void alertIsKeyedByCardAndItsIdIsDeterministic() {
        Transaction first = transaction("first", T0, "20.00", Channel.POS, MADRID);
        Transaction second = transaction("second", T0.plusSeconds(10), "20.00", Channel.POS, TOKYO);
        send(first);
        send(second);

        var record = alerts.readRecord();
        assertThat(record.key()).isEqualTo(CARD);
        assertThat(EventJson.MAPPER.readValue(record.value(), FraudAlert.class).alertId())
                .isEqualTo(FraudAlert.idFor(CARD, AlertRule.IMPOSSIBLE_TRAVEL, "second"));
    }
}
