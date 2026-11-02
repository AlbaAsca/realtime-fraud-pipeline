package org.ascaso.fraudpipeline.events;

import tools.jackson.databind.json.JsonMapper;

/** Único mapper JSON de los eventos, para que productor y procesador hablen exactamente igual. */
public final class EventJson {

    public static final JsonMapper MAPPER = JsonMapper.builder().build();

    private EventJson() {
    }
}
