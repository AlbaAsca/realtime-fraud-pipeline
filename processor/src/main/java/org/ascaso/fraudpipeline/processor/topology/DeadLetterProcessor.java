package org.ascaso.fraudpipeline.processor.topology;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

/** Deja el mensaje original intacto y anota en cabeceras por qué se rechazó y de dónde venía. */
class DeadLetterProcessor implements FixedKeyProcessor<String, Parsed.Invalid, byte[]> {

    static final String REASON_HEADER = "dlq-reason";
    static final String ORIGIN_HEADER = "dlq-origin";

    private final PipelineMetrics metrics;
    private FixedKeyProcessorContext<String, byte[]> context;

    DeadLetterProcessor(PipelineMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, byte[]> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, Parsed.Invalid> record) {
        Headers headers = record.headers();
        headers.add(REASON_HEADER, record.value().reason().getBytes(StandardCharsets.UTF_8));
        context.recordMetadata().ifPresent(origin -> headers.add(ORIGIN_HEADER,
                "%s-%d@%d".formatted(origin.topic(), origin.partition(), origin.offset()).getBytes(StandardCharsets.UTF_8)));
        metrics.invalid.increment();
        context.forward(record.withValue(record.value().raw()));
    }
}
