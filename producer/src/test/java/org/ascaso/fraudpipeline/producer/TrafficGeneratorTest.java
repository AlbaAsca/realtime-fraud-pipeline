package org.ascaso.fraudpipeline.producer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.ascaso.fraudpipeline.events.EventJson;
import org.ascaso.fraudpipeline.events.Transaction;
import org.junit.jupiter.api.Test;

class TrafficGeneratorTest {

    private static final Instant START = Instant.parse("2026-11-02T10:00:00Z");

    private static GeneratorProperties properties(double duplicate, double malformed, double invalid, double late) {
        return new GeneratorProperties(50, 100, Duration.ofHours(1), 5, duplicate, malformed, invalid, late);
    }

    private static List<OutgoingRecord> run(GeneratorProperties properties, int seconds) {
        TrafficGenerator generator = new TrafficGenerator(properties, 42);
        List<OutgoingRecord> all = new ArrayList<>();
        for (int i = 0; i < seconds * 5; i++) {
            all.addAll(generator.tick(START.plusMillis(i * 200L), Duration.ofMillis(200)));
        }
        return all;
    }

    @Test
    void cleanTrafficIsValidJsonKeyedByCard() {
        List<OutgoingRecord> records = run(properties(0, 0, 0, 0), 30);

        assertThat(records).hasSizeGreaterThan(1000);
        assertThat(records).allSatisfy(record -> {
            Transaction transaction = EventJson.MAPPER.readValue(record.value(), Transaction.class);
            assertThat(transaction.cardId()).isEqualTo(record.key());
            assertThat(transaction.occurredAt().toEpochMilli()).isEqualTo(record.timestamp());
        });
    }

    @Test
    void sameSeedProducesSameTraffic() {
        GeneratorProperties noisy = properties(0.01, 0.01, 0.01, 0.01);

        assertThat(run(noisy, 5)).isEqualTo(run(noisy, 5));
    }

    @Test
    void cardsKeepTheirHomeAcrossRestarts() {
        GeneratorProperties clean = properties(0, 0, 0, 0);

        assertThat(new TrafficGenerator(clean, 1).homeCities()).isEqualTo(new TrafficGenerator(clean, 2).homeCities());
    }

    @Test
    void injectsDuplicates() {
        List<OutgoingRecord> records = run(properties(1, 0, 0, 0), 2);

        Map<String, Long> occurrences = records.stream()
                .collect(Collectors.groupingBy(OutgoingRecord::value, Collectors.counting()));
        assertThat(occurrences.values()).allMatch(count -> count == 2);
    }

    @Test
    void injectsMalformedPayloads() {
        assertThat(run(properties(0, 1, 0, 0), 2)).allMatch(record -> record.value().endsWith("oops"));
    }

    @Test
    void attacksArriveInTurns() {
        GeneratorProperties attacks = new GeneratorProperties(50, 100, Duration.ofSeconds(5), 1, 0, 0, 0, 0);

        List<Transaction> transactions = run(attacks, 60).stream()
                .map(record -> EventJson.MAPPER.readValue(record.value(), Transaction.class))
                .toList();

        assertThat(transactions).extracting(Transaction::category).contains("gift-cards", "travel", "luxury");
    }
}
