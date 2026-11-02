package org.ascaso.fraudpipeline.processor.api;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.ascaso.fraudpipeline.processor.topology.CardProfile;
import org.ascaso.fraudpipeline.processor.topology.FraudRules;
import org.ascaso.fraudpipeline.processor.topology.FraudTopology;
import org.ascaso.fraudpipeline.processor.topology.PipelineMetrics;
import org.ascaso.fraudpipeline.processor.topology.VolumeStats;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

/** Interactive queries: la API lee directamente el estado local de Kafka Streams, sin base de datos aparte. */
@Service
public class PipelineQueries {

    private static final int STATS_MINUTES = 15;

    public record CardView(String cardId, long transactionsLastWindow, BigDecimal amountLastWindowEur, long historySize,
                           BigDecimal usualAmountEur, String lastCity, String lastCountry, Instant lastSeenAt) {
    }

    public record VolumePoint(Instant minute, long count, BigDecimal totalEur) {
    }

    public record CountryVolume(String country, long count, BigDecimal totalEur) {
    }

    public record PipelineStats(String state, long accepted, long invalid, long duplicates, long alerts, long suppressedAlerts,
                                List<VolumePoint> perMinute, List<CountryVolume> byCountry) {
    }

    private final StreamsBuilderFactoryBean streamsFactory;
    private final PipelineMetrics metrics;
    private final FraudRules rules;

    public PipelineQueries(StreamsBuilderFactoryBean streamsFactory, PipelineMetrics metrics, FraudRules rules) {
        this.streamsFactory = streamsFactory;
        this.metrics = metrics;
        this.rules = rules;
    }

    public Optional<CardView> card(String cardId) {
        return whileAvailable(() -> readCard(cardId));
    }

    public PipelineStats stats() {
        return whileAvailable(this::readStats);
    }

    /** El almacén puede dejar de estar disponible también a mitad de lectura, no solo al pedirlo. */
    private static <T> T whileAvailable(Supplier<T> query) {
        try {
            return query.get();
        } catch (InvalidStateStoreException e) {
            throw new PipelineNotReadyException(e);
        }
    }

    private Optional<CardView> readCard(String cardId) {
        CardProfile profile = store(FraudTopology.CARD_PROFILES, QueryableStoreTypes.<String, CardProfile>keyValueStore()).get(cardId);
        if (profile == null) {
            return Optional.empty();
        }
        VolumeStats busiest = busiestOpenWindow(cardId);
        BigDecimal usual = BigDecimal.valueOf(Math.exp(profile.logMean())).setScale(2, RoundingMode.HALF_UP);
        return Optional.of(new CardView(cardId, busiest.count(), busiest.totalEur(), profile.count(), usual,
                profile.lastCity(), profile.lastCountry(), profile.lastSeenAt()));
    }

    /** Entre las ventanas abiertas ahora mismo, la de mayor actividad: es la que ve la regla VELOCITY. */
    private VolumeStats busiestOpenWindow(String cardId) {
        ReadOnlyWindowStore<String, VolumeStats> activity = store(FraudTopology.CARD_ACTIVITY, QueryableStoreTypes.windowStore());
        Instant now = Instant.now();
        VolumeStats busiest = VolumeStats.empty();
        try (WindowStoreIterator<VolumeStats> windows = activity.fetch(cardId, now.minus(rules.velocityWindow()), now)) {
            while (windows.hasNext()) {
                VolumeStats stats = windows.next().value;
                if (stats.count() > busiest.count()) {
                    busiest = stats;
                }
            }
        }
        return busiest;
    }

    private PipelineStats readStats() {
        ReadOnlyWindowStore<String, VolumeStats> volume = store(FraudTopology.VOLUME_BY_COUNTRY, QueryableStoreTypes.windowStore());
        Instant to = Instant.now();
        Instant from = to.minus(Duration.ofMinutes(STATS_MINUTES)).truncatedTo(ChronoUnit.MINUTES);
        Map<Instant, VolumePoint> perMinute = new TreeMap<>();
        Map<String, CountryVolume> byCountry = new TreeMap<>();
        try (KeyValueIterator<Windowed<String>, VolumeStats> windows = volume.fetchAll(from, to)) {
            while (windows.hasNext()) {
                KeyValue<Windowed<String>, VolumeStats> window = windows.next();
                String country = window.key.key();
                Instant minute = window.key.window().startTime();
                VolumeStats stats = window.value;
                perMinute.merge(minute, new VolumePoint(minute, stats.count(), stats.totalEur()),
                        (a, b) -> new VolumePoint(minute, a.count() + b.count(), a.totalEur().add(b.totalEur())));
                byCountry.merge(country, new CountryVolume(country, stats.count(), stats.totalEur()),
                        (a, b) -> new CountryVolume(country, a.count() + b.count(), a.totalEur().add(b.totalEur())));
            }
        }
        List<CountryVolume> countries = new ArrayList<>(byCountry.values());
        countries.sort(Comparator.comparingLong(CountryVolume::count).reversed());
        return new PipelineStats(streams().state().name(), metrics.accepted(), metrics.invalid(), metrics.duplicates(),
                metrics.alerts(), metrics.suppressed(), List.copyOf(perMinute.values()), countries);
    }

    private <S> S store(String name, QueryableStoreType<S> type) {
        return streams().store(StoreQueryParameters.fromNameAndType(name, type));
    }

    private KafkaStreams streams() {
        KafkaStreams streams = streamsFactory.getKafkaStreams();
        if (streams == null) {
            throw new PipelineNotReadyException(null);
        }
        return streams;
    }
}
