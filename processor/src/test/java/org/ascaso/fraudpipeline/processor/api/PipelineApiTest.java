package org.ascaso.fraudpipeline.processor.api;

import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.ascaso.fraudpipeline.events.AlertRule;
import org.ascaso.fraudpipeline.events.FraudAlert;
import org.ascaso.fraudpipeline.processor.api.PipelineQueries.CardView;
import org.ascaso.fraudpipeline.processor.api.PipelineQueries.PipelineStats;
import org.ascaso.fraudpipeline.processor.config.HateoasConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.test.StepVerifier;

@WebFluxTest(PipelineApi.class)
@Import({HateoasConfig.class, AlertFeed.class})
class PipelineApiTest {

    private static final Instant NOW = Instant.parse("2026-11-02T10:00:00Z");

    @Autowired
    private WebTestClient client;

    @Autowired
    private AlertFeed feed;

    @MockitoBean
    private PipelineQueries queries;

    private static FraudAlert alert(String id, String card) {
        return new FraudAlert(id, card, AlertRule.VELOCITY, "10 operaciones en 60 s", new BigDecimal("15.00"), null, null, NOW);
    }

    @Test
    void rootLinksToEveryResource() {
        client.get().uri("/api").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$._links.alerts.href").isEqualTo("/api/alerts{?cardId,limit}")
                .jsonPath("$._links.stats.href").isEqualTo("/api/stats");
    }

    @Test
    void alertsCarryLinksToThemselvesAndTheirCard() {
        feed.publish(alert("a-1", "card-1"));

        client.get().uri("/api/alerts?cardId=card-1").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$._embedded.fraudAlertList[0].alertId").isEqualTo("a-1")
                .jsonPath("$._embedded.fraudAlertList[0]._links.self.href").isEqualTo("/api/alerts/a-1")
                .jsonPath("$._embedded.fraudAlertList[0]._links.card.href").isEqualTo("/api/cards/card-1");
    }

    @Test
    void alertsCanBeFilteredByCard() {
        feed.publish(alert("a-2", "card-2"));
        feed.publish(alert("a-3", "card-3"));

        client.get().uri("/api/alerts?cardId=card-3").exchange()
                .expectBody()
                .jsonPath("$._embedded.fraudAlertList.length()").isEqualTo(1)
                .jsonPath("$._embedded.fraudAlertList[0].alertId").isEqualTo("a-3");
    }

    @Test
    void unknownAlertIs404() {
        client.get().uri("/api/alerts/missing").exchange().expectStatus().isNotFound();
    }

    @Test
    void cardComesFromInteractiveQuery() {
        when(queries.card("card-1")).thenReturn(Optional.of(new CardView("card-1", 3, new BigDecimal("30.00"), 12,
                new BigDecimal("25.00"), "Madrid", "ES", NOW)));

        client.get().uri("/api/cards/card-1").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.lastCity").isEqualTo("Madrid")
                .jsonPath("$._links.alerts.href").isEqualTo("/api/alerts?cardId=card-1");
    }

    @Test
    void unknownCardIs404() {
        when(queries.card("nope")).thenReturn(Optional.empty());

        client.get().uri("/api/cards/nope").exchange().expectStatus().isNotFound();
    }

    @Test
    void whileRebalancingAnswers503ProblemDetail() {
        when(queries.stats()).thenThrow(new PipelineNotReadyException(null));

        client.get().uri("/api/stats").exchange()
                .expectStatus().isEqualTo(503)
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.title").isEqualTo("Pipeline no disponible");
    }

    @Test
    void statsExposeCounters() {
        when(queries.stats()).thenReturn(new PipelineStats("RUNNING", 100, 2, 1, 5, 9, List.of(), List.of()));

        client.get().uri("/api/stats").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.accepted").isEqualTo(100)
                .jsonPath("$._links.events.href").isEqualTo("/api/stats/events");
    }

    @Test
    void alertEventsPushNewAlerts() {
        var events = client.get().uri("/api/alerts/events").accept(MediaType.TEXT_EVENT_STREAM).exchange()
                .expectStatus().isOk()
                .returnResult(new org.springframework.core.ParameterizedTypeReference<ServerSentEvent<FraudAlert>>() { })
                .getResponseBody()
                .filter(event -> event.data() != null);

        StepVerifier.create(events)
                .then(() -> feed.publish(alert("a-sse", "card-sse")))
                .expectNextMatches(event -> "alert".equals(event.event()) && event.data().alertId().equals("a-sse"))
                .thenCancel()
                .verify();
    }
}
