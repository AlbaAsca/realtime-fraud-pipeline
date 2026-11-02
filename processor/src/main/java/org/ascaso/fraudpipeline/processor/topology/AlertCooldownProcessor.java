package org.ascaso.fraudpipeline.processor.topology;

import java.time.Duration;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.ascaso.fraudpipeline.events.FraudAlert;

/**
 * Una ráfaga cae en varias ventanas solapadas y cada una dispararía su alerta. Aquí se deja pasar
 * solo la primera por tarjeta y regla durante el periodo de enfriamiento: un analista no quiere seis avisos.
 */
class AlertCooldownProcessor implements FixedKeyProcessor<String, FraudAlert, FraudAlert> {

    private final long cooldownMs;
    private final PipelineMetrics metrics;
    private FixedKeyProcessorContext<String, FraudAlert> context;
    private KeyValueStore<String, Long> lastAlerts;

    AlertCooldownProcessor(Duration cooldown, PipelineMetrics metrics) {
        this.cooldownMs = cooldown.toMillis();
        this.metrics = metrics;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, FraudAlert> context) {
        this.context = context;
        this.lastAlerts = context.getStateStore(FraudTopology.ALERT_COOLDOWN);
    }

    @Override
    public void process(FixedKeyRecord<String, FraudAlert> record) {
        String key = record.key() + '|' + record.value().rule();
        long at = record.timestamp();
        Long last = lastAlerts.get(key);
        if (last != null && Math.abs(at - last) < cooldownMs) {
            metrics.suppressed.increment();
            return;
        }
        lastAlerts.put(key, at);
        metrics.alerts.increment();
        context.forward(record);
    }
}
