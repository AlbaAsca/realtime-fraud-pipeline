package org.ascaso.fraudpipeline.processor.api;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import org.ascaso.fraudpipeline.events.FraudAlert;
import org.ascaso.fraudpipeline.processor.api.PipelineQueries.CardView;
import org.ascaso.fraudpipeline.processor.api.PipelineQueries.PipelineStats;
import org.springframework.hateoas.CollectionModel;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.RepresentationModel;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** API de solo lectura en HAL. Las consultas al estado local son bloqueantes: van a un scheduler elástico. */
@RestController
@RequestMapping("/api")
public class PipelineApi {

    private static final Duration STATS_PERIOD = Duration.ofSeconds(2);
    private static final Duration HEARTBEAT_PERIOD = Duration.ofSeconds(15);

    private final AlertFeed feed;
    private final PipelineQueries queries;

    public PipelineApi(AlertFeed feed, PipelineQueries queries) {
        this.feed = feed;
        this.queries = queries;
    }

    @GetMapping
    public RepresentationModel<?> root() {
        return new RepresentationModel<>(List.of(
                Link.of("/api").withSelfRel(),
                Link.of("/api/alerts{?cardId,limit}").withRel("alerts"),
                Link.of("/api/alerts/events").withRel("alert-events"),
                Link.of("/api/cards/{cardId}").withRel("card"),
                Link.of("/api/stats").withRel("stats"),
                Link.of("/api/stats/events").withRel("stats-events")));
    }

    @GetMapping("/alerts")
    public CollectionModel<EntityModel<FraudAlert>> alerts(@RequestParam(required = false) String cardId,
                                                           @RequestParam(defaultValue = "50") int limit) {
        List<EntityModel<FraudAlert>> alerts = feed.recent(cardId, Math.clamp(limit, 1, 500)).stream().map(this::toModel).toList();
        return CollectionModel.of(alerts, Link.of("/api/alerts").withSelfRel(), Link.of("/api/alerts/events").withRel("events"));
    }

    @GetMapping("/alerts/{alertId}")
    public ResponseEntity<EntityModel<FraudAlert>> alert(@PathVariable String alertId) {
        return feed.find(alertId)
                .map(this::toModel)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping(value = "/alerts/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<FraudAlert>> alertEvents() {
        Flux<ServerSentEvent<FraudAlert>> alerts = feed.live()
                .map(alert -> ServerSentEvent.builder(alert).event("alert").id(alert.alertId()).build());
        // El primer latido sale al instante: así el cliente recibe las cabeceras sin esperar a la primera alerta.
        Flux<ServerSentEvent<FraudAlert>> heartbeat = Flux.interval(Duration.ZERO, HEARTBEAT_PERIOD)
                .map(tick -> ServerSentEvent.<FraudAlert>builder().comment("heartbeat").build());
        return Flux.merge(alerts, heartbeat);
    }

    @GetMapping("/cards/{cardId}")
    public Mono<ResponseEntity<EntityModel<CardView>>> card(@PathVariable String cardId) {
        return Mono.fromCallable(() -> queries.card(cardId))
                .subscribeOn(Schedulers.boundedElastic())
                .map(card -> card
                        .map(view -> EntityModel.of(view,
                                Link.of("/api/cards/" + cardId).withSelfRel(),
                                Link.of("/api/alerts?cardId=" + cardId).withRel("alerts")))
                        .map(ResponseEntity::ok)
                        .orElseGet(() -> ResponseEntity.notFound().build()));
    }

    @GetMapping("/stats")
    public Mono<EntityModel<PipelineStats>> stats() {
        return Mono.fromCallable(queries::stats)
                .subscribeOn(Schedulers.boundedElastic())
                .map(stats -> EntityModel.of(stats,
                        Link.of("/api/stats").withSelfRel(),
                        Link.of("/api/stats/events").withRel("events")));
    }

    @GetMapping(value = "/stats/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<PipelineStats>> statsEvents() {
        return Flux.interval(Duration.ZERO, STATS_PERIOD)
                .onBackpressureDrop()
                .concatMap(tick -> Mono.fromCallable(queries::stats)
                        .subscribeOn(Schedulers.boundedElastic())
                        // Durante un reequilibrio se salta el tick en vez de cortar la conexión.
                        .onErrorResume(PipelineNotReadyException.class, e -> Mono.empty()))
                .map(stats -> ServerSentEvent.builder(stats).event("stats").build());
    }

    @ExceptionHandler(PipelineNotReadyException.class)
    ProblemDetail pipelineNotReady(PipelineNotReadyException exception) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, exception.getMessage());
        problem.setType(URI.create("about:blank"));
        problem.setTitle("Pipeline no disponible");
        return problem;
    }

    private EntityModel<FraudAlert> toModel(FraudAlert alert) {
        return EntityModel.of(alert,
                Link.of("/api/alerts/" + alert.alertId()).withSelfRel(),
                Link.of("/api/cards/" + alert.cardId()).withRel("card"));
    }
}
