package org.ascaso.fraudpipeline.processor.config;

import org.apache.kafka.common.TopicPartition;
import org.ascaso.fraudpipeline.events.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import tools.jackson.core.JacksonException;

/**
 * Tres reintentos con espera creciente y, si sigue fallando, a la DLQ. Un JSON roto no se arregla
 * reintentando, así que va directo.
 */
@Configuration
public class AlertListenerConfig {

    @Bean
    DefaultErrorHandler alertErrorHandler(KafkaOperations<?, ?> kafka) {
        var recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, exception) -> new TopicPartition(Topics.FRAUD_ALERTS_DLQ, 0));
        var backOff = new ExponentialBackOff(200, 2);
        backOff.setMaxAttempts(3);
        var handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(JacksonException.class);
        return handler;
    }
}
