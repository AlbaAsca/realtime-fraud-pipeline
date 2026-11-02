package org.ascaso.fraudpipeline.processor.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.ascaso.fraudpipeline.events.EventJson;

/** Serde JSON para eventos y estado. Solo lee datos ya validados: la entrada cruda pasa antes por el parser. */
final class JsonSerde {

    private JsonSerde() {
    }

    static <T> Serde<T> of(Class<T> type) {
        return Serdes.serdeFrom(
                (topic, value) -> value == null ? null : EventJson.MAPPER.writeValueAsBytes(value),
                (topic, bytes) -> bytes == null ? null : EventJson.MAPPER.readValue(bytes, type));
    }
}
