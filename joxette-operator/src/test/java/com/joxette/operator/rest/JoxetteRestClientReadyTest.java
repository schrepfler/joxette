package com.joxette.operator.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link JoxetteRestClient#ready()} must reflect the readiness endpoint and must
 * never throw — returning {@code false} when the cluster is down so the API
 * reconcilers can reschedule instead of erroring.
 */
class JoxetteRestClientReadyTest {

    private HttpServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private JoxetteRestClient clientFor(int port) {
        return new JoxetteRestClient("http://127.0.0.1:" + port,
                java.net.http.HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build(),
                new ObjectMapper(), Duration.ofSeconds(2));
    }

    @Test
    void readyTrueWhenReadinessReturns200() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/actuator/health/readiness", ex -> {
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });
        server.start();
        assertThat(clientFor(server.getAddress().getPort()).ready()).isTrue();
    }

    @Test
    void readyFalseWhenReadinessReturns503() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/actuator/health/readiness", ex -> {
            ex.sendResponseHeaders(503, -1);
            ex.close();
        });
        server.start();
        assertThat(clientFor(server.getAddress().getPort()).ready()).isFalse();
    }

    @Test
    void readyFalseWhenHostUnreachableAndDoesNotThrow() {
        // Nothing listening on this port — connection refused must become false, not an exception.
        assertThat(clientFor(59999).ready()).isFalse();
    }
}
