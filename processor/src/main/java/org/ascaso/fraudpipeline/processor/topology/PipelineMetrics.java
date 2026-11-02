package org.ascaso.fraudpipeline.processor.topology;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** Contadores del pipeline, visibles en el panel y en {@code /actuator/metrics/pipeline.records}. */
@Component
public class PipelineMetrics {

    final Counter accepted;
    final Counter invalid;
    final Counter duplicates;
    final Counter alerts;
    final Counter suppressed;

    public PipelineMetrics(MeterRegistry registry) {
        accepted = registry.counter("pipeline.records", "outcome", "accepted");
        invalid = registry.counter("pipeline.records", "outcome", "invalid");
        duplicates = registry.counter("pipeline.records", "outcome", "duplicate");
        alerts = registry.counter("pipeline.alerts", "outcome", "emitted");
        suppressed = registry.counter("pipeline.alerts", "outcome", "suppressed");
    }

    public long accepted() {
        return (long) accepted.count();
    }

    public long invalid() {
        return (long) invalid.count();
    }

    public long duplicates() {
        return (long) duplicates.count();
    }

    public long alerts() {
        return (long) alerts.count();
    }

    public long suppressed() {
        return (long) suppressed.count();
    }
}
