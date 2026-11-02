package org.ascaso.fraudpipeline.processor.api;

/** El estado local aún no se puede consultar: Kafka Streams está arrancando o reequilibrando. */
public class PipelineNotReadyException extends RuntimeException {

    public PipelineNotReadyException(Throwable cause) {
        super("El pipeline está arrancando o reequilibrando particiones", cause);
    }
}
