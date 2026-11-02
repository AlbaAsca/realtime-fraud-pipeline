package org.ascaso.fraudpipeline.processor.topology;

/** Distancia entre dos puntos de la Tierra (fórmula del haversine). */
final class Geo {

    private static final double EARTH_RADIUS_KM = 6371;

    private Geo() {
    }

    static double distanceKm(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.pow(Math.sin(dLat / 2), 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.pow(Math.sin(dLon / 2), 2);
        return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(a));
    }
}
