package org.ascaso.fraudpipeline.processor.api;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.ascaso.fraudpipeline.processor.topology.PipelineMetrics;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class PipelineQueriesTest {

    private final StreamsBuilderFactoryBean factory = mock(StreamsBuilderFactoryBean.class);
    private final PipelineQueries queries = new PipelineQueries(factory, new PipelineMetrics(new SimpleMeterRegistry()), null);

    @Test
    void streamsNotCreatedYetMeansNotReady() {
        assertThatThrownBy(queries::stats).isInstanceOf(PipelineNotReadyException.class);
    }

    /** Así ocurre de verdad al arrancar: pedir el almacén funciona y es la lectura la que falla. */
    @Test
    @SuppressWarnings("unchecked")
    void storeFailingDuringTheReadMeansNotReady() {
        KafkaStreams streams = mock(KafkaStreams.class);
        ReadOnlyWindowStore<Object, Object> store = mock(ReadOnlyWindowStore.class);
        when(factory.getKafkaStreams()).thenReturn(streams);
        when(streams.store(any())).thenReturn(store);
        when(store.fetchAll(any(), any())).thenThrow(new InvalidStateStoreException("stream thread is STARTING"));

        assertThatThrownBy(queries::stats).isInstanceOf(PipelineNotReadyException.class);
    }
}
