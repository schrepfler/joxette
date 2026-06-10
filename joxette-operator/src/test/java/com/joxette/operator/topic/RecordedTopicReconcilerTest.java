package com.joxette.operator.topic;

import com.joxette.operator.rest.JoxetteRestClient;
import com.joxette.operator.rest.RestClientFactory;
import com.sun.net.httpserver.HttpServer;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Verifies the cluster-not-ready behaviour of {@link RecordedTopicReconciler}:
 * when the target cluster's readiness probe fails, the reconciler reports
 * {@code Progressing} and reschedules instead of erroring.
 */
class RecordedTopicReconcilerTest {

    private HttpServer server;

    @AfterEach
    void stop() {
        if (server != null) server.stop(0);
    }

    /** Factory that returns a client pointed at the given base URL. */
    private RestClientFactory factoryFor(String baseUrl) {
        return new RestClientFactory() {
            @Override
            public JoxetteRestClient forBaseUrl(String ignored) {
                return new JoxetteRestClient(baseUrl);
            }
        };
    }

    private RecordedTopic topic() {
        RecordedTopic t = new RecordedTopic();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("orders");
        meta.setNamespace("joxette");
        meta.setGeneration(1L);
        t.setMetadata(meta);
        RecordedTopicSpec spec = new RecordedTopicSpec();
        spec.getClusterRef().setName("prod");
        spec.setTopic("orders.events");
        spec.setMode("both");
        t.setSpec(spec);
        return t;
    }

    @SuppressWarnings("unchecked")
    private Context<RecordedTopic> ctx() {
        return mock(Context.class);
    }

    @Test
    void clusterNotReadyReportsProgressingAndReschedules() {
        // Unreachable cluster (nothing listening) => ready() false.
        RecordedTopicReconciler reconciler =
                new RecordedTopicReconciler(factoryFor("http://127.0.0.1:59998"));
        RecordedTopic t = topic();

        UpdateControl<RecordedTopic> control = reconciler.reconcile(t, ctx());

        assertThat(t.getStatus().getPhase()).isEqualTo("Progressing");
        assertThat(t.getStatus().getRegistered()).isFalse();
        assertThat(control.getScheduleDelay()).isPresent();
    }

    @Test
    void clusterReadyConvergesAndReportsReady() throws IOException {
        // Stand up a server that is ready and accepts the topic create.
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/actuator/health/readiness", ex -> {
            ex.sendResponseHeaders(200, -1); ex.close();
        });
        server.createContext("/topics", ex -> {
            // GET /topics/orders.events -> 404 (absent); POST /topics -> 201
            if (ex.getRequestMethod().equals("GET")) {
                ex.sendResponseHeaders(404, -1);
            } else {
                ex.sendResponseHeaders(201, -1);
            }
            ex.close();
        });
        server.start();

        RecordedTopicReconciler reconciler =
                new RecordedTopicReconciler(factoryFor("http://127.0.0.1:" + server.getAddress().getPort()));
        RecordedTopic t = topic();

        UpdateControl<RecordedTopic> control = reconciler.reconcile(t, ctx());

        assertThat(t.getStatus().getPhase()).isEqualTo("Ready");
        assertThat(t.getStatus().getRegistered()).isTrue();
        assertThat(control.isPatchStatus()).isTrue();
    }
}
