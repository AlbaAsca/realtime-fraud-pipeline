package org.ascaso.fraudpipeline.events;

/** Canal de pago. Solo los presenciales dicen algo fiable sobre dónde está físicamente la tarjeta. */
public enum Channel {
    POS,
    ATM,
    ONLINE;

    public boolean isCardPresent() {
        return this != ONLINE;
    }
}
