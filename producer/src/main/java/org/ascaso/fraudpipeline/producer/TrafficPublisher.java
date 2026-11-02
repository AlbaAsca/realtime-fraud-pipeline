package org.ascaso.fraudpipeline.producer;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.ascaso.fraudpipeline.events.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

/** Envía a Kafka lo que produce el generador. El timestamp del mensaje es el del evento. */
@Configuration
public class TrafficPublisher {

    private static final Logger log = LoggerFactory.getLogger(TrafficPublisher.class);
    private static final long TICK_MS = 200;

    private final KafkaTemplate<String, String> kafka;
    private final TrafficGenerator generator;
    private long sent;

    public TrafficPublisher(KafkaTemplate<String, String> kafka, GeneratorProperties properties) {
        this.kafka = kafka;
        this.generator = new TrafficGenerator(properties, System.nanoTime());
    }

    @Bean
    static NewTopic transactionsTopic() {
        return TopicBuilder.name(Topics.TRANSACTIONS).partitions(6).replicas(1).build();
    }

    @Scheduled(fixedRate = TICK_MS)
    void publish() {
        for (OutgoingRecord out : generator.tick(Instant.now(), Duration.ofMillis(TICK_MS))) {
            kafka.send(new ProducerRecord<>(Topics.TRANSACTIONS, null, out.timestamp(), out.key(), out.value()))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.warn("No se pudo enviar el mensaje de {}: {}", out.key(), error.getMessage());
                        }
                    });
            if (++sent % 1000 == 0) {
                log.info("{} mensajes enviados", sent);
            }
        }
    }
}
