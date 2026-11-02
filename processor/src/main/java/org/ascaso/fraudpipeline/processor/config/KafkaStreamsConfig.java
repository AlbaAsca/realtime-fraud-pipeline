package org.ascaso.fraudpipeline.processor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.KStream;
import org.ascaso.fraudpipeline.events.FraudAlert;
import org.ascaso.fraudpipeline.events.Topics;
import org.ascaso.fraudpipeline.processor.topology.FraudRules;
import org.ascaso.fraudpipeline.processor.topology.FraudTopology;
import org.ascaso.fraudpipeline.processor.topology.PipelineMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Infraestructura de Kafka Streams, aparte para que los tests de slice no intenten arrancarla. */
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsConfig.class);

    @Bean
    KStream<String, FraudAlert> fraudPipeline(StreamsBuilder builder, FraudRules rules, PipelineMetrics metrics) {
        return FraudTopology.build(builder, rules, metrics);
    }

    /** Si un hilo muere por un fallo transitorio se sustituye, en vez de quedar la instancia a medias. */
    @Bean
    StreamsBuilderFactoryBeanConfigurer replaceFailedThreads() {
        return factory -> factory.setStreamsUncaughtExceptionHandler(exception -> {
            log.error("Hilo de Kafka Streams caído; se sustituye", exception);
            return StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    }

    @Bean
    KafkaAdmin.NewTopics pipelineTopics() {
        return new KafkaAdmin.NewTopics(
                topic(Topics.TRANSACTIONS, 6),
                topic(Topics.TRANSACTIONS_DLQ, 1),
                topic(Topics.FRAUD_ALERTS, 3),
                topic(Topics.FRAUD_ALERTS_DLQ, 1));
    }

    private static NewTopic topic(String name, int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1).build();
    }
}
