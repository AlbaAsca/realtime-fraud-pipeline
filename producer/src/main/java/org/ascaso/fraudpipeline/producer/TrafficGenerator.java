package org.ascaso.fraudpipeline.producer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.UUID;

import org.ascaso.fraudpipeline.events.Channel;
import org.ascaso.fraudpipeline.events.EventJson;
import org.ascaso.fraudpipeline.events.Transaction;

/**
 * Simula una red de pagos: titulares con ciudad y gasto propios, ataques periódicos y datos sucios.
 * No toca Kafka ni el reloj: recibe el instante y una semilla, así los tests son deterministas.
 */
public class TrafficGenerator {

    private record City(String name, String country, double latitude, double longitude) {
    }

    private record Merchant(String name, String category, double priceFactor) {
    }

    private static final class Card {
        final String id;
        final City home;
        final double typicalAmount;
        int history;

        Card(String id, City home, double typicalAmount) {
            this.id = id;
            this.home = home;
            this.typicalAmount = typicalAmount;
        }
    }

    private record Scheduled(Instant due, Transaction transaction) {
    }

    private static final List<City> HOME_CITIES = List.of(
            new City("Madrid", "ES", 40.4168, -3.7038),
            new City("Barcelona", "ES", 41.3874, 2.1686),
            new City("Valencia", "ES", 39.4699, -0.3763),
            new City("Sevilla", "ES", 37.3891, -5.9845),
            new City("Bilbao", "ES", 43.2630, -2.9350),
            new City("Lisboa", "PT", 38.7223, -9.1393),
            new City("París", "FR", 48.8566, 2.3522),
            new City("Berlín", "DE", 52.5200, 13.4050),
            new City("Roma", "IT", 41.9028, 12.4964));

    private static final List<City> FAR_CITIES = List.of(
            new City("Tokio", "JP", 35.6762, 139.6503),
            new City("Nueva York", "US", 40.7128, -74.0060),
            new City("Bogotá", "CO", 4.7110, -74.0721),
            new City("Lagos", "NG", 6.5244, 3.3792));

    private static final List<Merchant> MERCHANTS = List.of(
            new Merchant("Mercadona", "groceries", 1.2),
            new Merchant("Carrefour", "groceries", 1.5),
            new Merchant("Repsol", "fuel", 1.4),
            new Merchant("Renfe", "transport", 1.1),
            new Merchant("Bar Manolo", "restaurants", 0.5),
            new Merchant("La Tagliatella", "restaurants", 1.0),
            new Merchant("Zara", "fashion", 1.8),
            new Merchant("MediaMarkt", "electronics", 3.0),
            new Merchant("Amazon", "marketplace", 1.3),
            new Merchant("Netflix", "subscriptions", 0.4));

    private static final Merchant LUXURY = new Merchant("Joyería Suárez", "luxury", 1);
    private static final Merchant DUTY_FREE = new Merchant("Duty Free", "travel", 1);
    private static final Merchant GIFT_CARDS = new Merchant("digital-gift-cards.example", "gift-cards", 1);

    private static final long POPULATION_SEED = 2026;

    private final GeneratorProperties properties;
    private final Random random;
    private final List<Card> cards = new ArrayList<>();
    private final PriorityQueue<Scheduled> pending = new PriorityQueue<>(Comparator.comparing(Scheduled::due));
    private Instant nextAttack;
    private int attackTurn;
    private double carry;

    public TrafficGenerator(GeneratorProperties properties, long seed) {
        this.properties = properties;
        this.random = new Random(seed);
        // Población fija: si cada arranque mudara a los titulares, el procesador, que recuerda dónde
        // estaba cada tarjeta, vería cientos de viajes imposibles que no son tales.
        Random population = new Random(POPULATION_SEED);
        for (int i = 0; i < properties.cards(); i++) {
            City home = HOME_CITIES.get(population.nextInt(HOME_CITIES.size()));
            cards.add(new Card("card-%04d".formatted(i), home, 15 + population.nextDouble() * 45));
        }
    }

    /** Solo para los tests: dónde vive cada tarjeta. */
    List<String> homeCities() {
        return cards.stream().map(card -> card.home.name()).toList();
    }

    /** Devuelve lo que hay que enviar en este tick; un seno suave evita que la gráfica sea plana. */
    public List<OutgoingRecord> tick(Instant now, Duration elapsed) {
        double wave = 1 + 0.35 * Math.sin(now.getEpochSecond() / 45.0);
        carry += properties.transactionsPerSecond() * wave * elapsed.toMillis() / 1000.0;
        int count = (int) carry;
        carry -= count;

        List<OutgoingRecord> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            addWithNoise(out, normalTransaction(now));
        }
        if (nextAttack == null) {
            nextAttack = now.plus(properties.attackEvery());
        } else if (!now.isBefore(nextAttack)) {
            scheduleAttack(now);
            nextAttack = now.plus(properties.attackEvery());
        }
        while (!pending.isEmpty() && !pending.peek().due().isAfter(now)) {
            out.add(toRecord(pending.poll().transaction()));
        }
        return out;
    }

    private void addWithNoise(List<OutgoingRecord> out, Transaction transaction) {
        double dice = random.nextDouble();
        double malformedUpTo = properties.malformedRate();
        double invalidUpTo = malformedUpTo + properties.invalidRate();
        double duplicateUpTo = invalidUpTo + properties.duplicateRate();
        double lateUpTo = duplicateUpTo + properties.lateRate();
        if (dice < malformedUpTo) {
            out.add(new OutgoingRecord(transaction.cardId(), "{\"transactionId\": \"" + transaction.transactionId() + "\", oops",
                    transaction.occurredAt().toEpochMilli()));
        } else if (dice < invalidUpTo) {
            out.add(toRecord(with(transaction, new BigDecimal("-12.50"), transaction.occurredAt())));
        } else if (dice < duplicateUpTo) {
            out.add(toRecord(transaction));
            out.add(toRecord(transaction));
        } else if (dice < lateUpTo) {
            // Llega ahora, pero ocurrió antes: el móvil sin cobertura o el datáfono que reintenta.
            out.add(toRecord(with(transaction, transaction.amountEur(),
                    transaction.occurredAt().minusSeconds(5 + random.nextInt(40)))));
        } else {
            out.add(toRecord(transaction));
        }
    }

    private Transaction normalTransaction(Instant now) {
        Card card = cards.get(random.nextInt(cards.size()));
        card.history++;
        Merchant merchant = MERCHANTS.get(random.nextInt(MERCHANTS.size()));
        Channel channel = switch (merchant.category()) {
            case "marketplace", "subscriptions" -> Channel.ONLINE;
            default -> random.nextDouble() < 0.05 ? Channel.ATM : Channel.POS;
        };
        // Importe log-normal: muchos pagos pequeños y pocos grandes, como en la realidad.
        double amount = card.typicalAmount * merchant.priceFactor() * Math.exp(random.nextGaussian() * 0.6);
        return transaction(card, merchant, amount, channel, card.home, now);
    }

    private void scheduleAttack(Instant now) {
        switch (attackTurn++ % 3) {
            case 0 -> scheduleCardTesting(now);
            case 1 -> scheduleImpossibleTravel(now);
            default -> scheduleAmountSpike(now);
        }
    }

    /** Muchos cargos pequeños en segundos, para comprobar qué tarjetas robadas siguen vivas. */
    private void scheduleCardTesting(Instant now) {
        Card card = cards.get(random.nextInt(cards.size()));
        for (int i = 0; i < 10; i++) {
            Instant at = now.plusMillis(i * 2500L);
            pending.add(new Scheduled(at, transaction(card, GIFT_CARDS, 0.5 + random.nextDouble() * 3,
                    Channel.ONLINE, card.home, at)));
        }
    }

    /** Un pago en casa y, dos segundos después, otro al otro lado del mundo. */
    private void scheduleImpossibleTravel(Instant now) {
        Card card = cards.get(random.nextInt(cards.size()));
        City far = FAR_CITIES.get(random.nextInt(FAR_CITIES.size()));
        Instant later = now.plusSeconds(2);
        pending.add(new Scheduled(now, transaction(card, MERCHANTS.get(0), card.typicalAmount, Channel.POS, card.home, now)));
        pending.add(new Scheduled(later, transaction(card, DUTY_FREE, card.typicalAmount * 2, Channel.POS, far, later)));
    }

    /** Un cargo decenas de veces mayor de lo habitual, en una tarjeta con historial suficiente. */
    private void scheduleAmountSpike(Instant now) {
        List<Card> eligible = cards.stream().filter(card -> card.history >= properties.minHistoryForSpike()).toList();
        if (eligible.isEmpty()) {
            scheduleCardTesting(now);
            return;
        }
        Card card = eligible.get(random.nextInt(eligible.size()));
        pending.add(new Scheduled(now, transaction(card, LUXURY, card.typicalAmount * 60, Channel.POS, card.home, now)));
    }

    private Transaction transaction(Card card, Merchant merchant, double amount, Channel channel, City city, Instant at) {
        byte[] seed = new byte[16];
        random.nextBytes(seed);
        return new Transaction(
                UUID.nameUUIDFromBytes(seed).toString(),
                card.id,
                merchant.name(),
                merchant.category(),
                BigDecimal.valueOf(Math.max(amount, 0.5)).setScale(2, RoundingMode.HALF_UP),
                channel,
                city.country(),
                city.name(),
                city.latitude() + (random.nextDouble() - 0.5) * 0.04,
                city.longitude() + (random.nextDouble() - 0.5) * 0.04,
                at);
    }

    private static Transaction with(Transaction t, BigDecimal amount, Instant at) {
        return new Transaction(t.transactionId(), t.cardId(), t.merchant(), t.category(), amount, t.channel(),
                t.country(), t.city(), t.latitude(), t.longitude(), at);
    }

    private static OutgoingRecord toRecord(Transaction transaction) {
        return new OutgoingRecord(transaction.cardId(), EventJson.MAPPER.writeValueAsString(transaction),
                transaction.occurredAt().toEpochMilli());
    }
}
