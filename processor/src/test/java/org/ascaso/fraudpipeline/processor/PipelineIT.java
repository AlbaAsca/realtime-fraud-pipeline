package org.ascaso.fraudpipeline.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.ascaso.fraudpipeline.events.AlertRule;
import org.ascaso.fraudpipeline.events.Channel;
import org.ascaso.fraudpipeline.events.EventJson;
import org.ascaso.fraudpipeline.events.FraudAlert;
import org.ascaso.fraudpipeline.events.Topics;
import org.ascaso.fraudpipeline.events.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.kafka.KafkaContainer;

import reactor.test.StepVerifier;

/** De punta a punta contra un Kafka real: lo que TopologyTestDriver no puede demostrar. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.kafka.streams.state-dir=target/kafka-streams-it")
class PipelineIT {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.0");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        KAFKA.start();
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private StreamsBuilderFactoryBean streamsFactory;

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).responseTimeout(Duration.ofSeconds(60)).build();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            KafkaStreams streams = streamsFactory.getKafkaStreams();
            return streams != null && streams.state() == KafkaStreams.State.RUNNING;
        });
    }

    private void sendCardTesting(String card, int count) {
        Instant start = Instant.now();
        for (int i = 0; i < count; i++) {
            Transaction transaction = new Transaction(UUID.randomUUID().toString(), card, "gift", "gift-cards",
                    new BigDecimal("1.50"), Channel.ONLINE, "ES", "Madrid", 40.4, -3.7, start.plusMillis(i));
            kafka.send(Topics.TRANSACTIONS, card, EventJson.MAPPER.writeValueAsString(transaction));
        }
        kafka.flush();
    }

    private List<ConsumerRecord<String, String>> readAll(String topic) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            consumer.poll(Duration.ofSeconds(3)).records(topic).forEach(records::add);
            return records;
        }
    }

    @Test
    void burstEndsUpInAlertTopicAndInTheApi() {
        sendCardTesting("card-burst", 10);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> client.get().uri("/api/alerts?cardId=card-burst").exchange()
                .expectBody()
                .jsonPath("$._embedded.fraudAlertList.length()").isEqualTo(1)
                .jsonPath("$._embedded.fraudAlertList[0].rule").isEqualTo(AlertRule.VELOCITY.name()));

        assertThat(readAll(Topics.FRAUD_ALERTS)).extracting(ConsumerRecord::key).contains("card-burst");
    }

    @Test
    void alertsArriveOverServerSentEvents() {
        var events = client.get().uri("/api/alerts/events").accept(MediaType.TEXT_EVENT_STREAM).exchange()
                .expectStatus().isOk()
                .returnResult(new ParameterizedTypeReference<ServerSentEvent<FraudAlert>>() { })
                .getResponseBody()
                .filter(event -> event.data() != null && event.data().cardId().equals("card-sse"));

        StepVerifier.create(events)
                .then(() -> sendCardTesting("card-sse", 10))
                .expectNextMatches(event -> event.data().rule() == AlertRule.VELOCITY)
                .thenCancel()
                .verify(Duration.ofSeconds(60));
    }

    @Test
    void stateIsQueryableThroughTheApi() {
        sendCardTesting("card-query", 3);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> client.get().uri("/api/cards/card-query").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.historySize").isEqualTo(3)
                .jsonPath("$.transactionsLastWindow").isEqualTo(3));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> client.get().uri("/api/stats").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.state").isEqualTo("RUNNING")
                .jsonPath("$.perMinute.length()").value(length -> assertThat((Integer) length).isPositive()));
    }

    @Test
    void poisonMessageGoesToDlqAndPipelineKeepsRunning() {
        kafka.send(Topics.TRANSACTIONS, "card-poison", "{broken");
        kafka.flush();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            ConsumerRecord<String, String> dead = readAll(Topics.TRANSACTIONS_DLQ).stream()
                    .filter(record -> "card-poison".equals(record.key())).findFirst().orElseThrow();
            assertThat(dead.value()).isEqualTo("{broken");
            assertThat(new String(dead.headers().lastHeader("dlq-reason").value(), StandardCharsets.UTF_8)).startsWith("malformed-json");
        });
        assertThat(streamsFactory.getKafkaStreams().state()).isEqualTo(KafkaStreams.State.RUNNING);
    }

    @Test
    void unreadableAlertGoesToItsDlqWithoutBlockingTheListener() {
        kafka.send(Topics.FRAUD_ALERTS, "card-bad-alert", "not an alert");
        sendCardTesting("card-after-bad-alert", 10);
        kafka.flush();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(readAll(Topics.FRAUD_ALERTS_DLQ))
                .extracting(ConsumerRecord::key).contains("card-bad-alert"));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> client.get().uri("/api/alerts?cardId=card-after-bad-alert").exchange()
                .expectBody().jsonPath("$._embedded.fraudAlertList.length()").isEqualTo(1));
    }
}
