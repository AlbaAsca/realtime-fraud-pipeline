package org.ascaso.fraudpipeline.processor.api;

import org.ascaso.fraudpipeline.events.EventJson;
import org.ascaso.fraudpipeline.events.FraudAlert;
import org.ascaso.fraudpipeline.events.Topics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Lleva las alertas del tópico al panel, leyendo solo lo confirmado por la transacción de Kafka Streams. */
@Component
public class AlertListener {

    private final AlertFeed feed;

    public AlertListener(AlertFeed feed) {
        this.feed = feed;
    }

    @KafkaListener(topics = Topics.FRAUD_ALERTS)
    void onAlert(String payload) {
        feed.publish(EventJson.MAPPER.readValue(payload, FraudAlert.class));
    }
}
