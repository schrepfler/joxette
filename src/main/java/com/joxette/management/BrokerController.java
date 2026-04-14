package com.joxette.management;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;

/**
 * Management API for Kafka broker connection configurations.
 *
 * <pre>
 * GET    /brokers               list all brokers (passwords masked)
 * POST   /brokers               register a new broker
 * GET    /brokers/{brokerId}    get a single broker's config (passwords masked)
 * PUT    /brokers/{brokerId}    create or replace a broker config
 * DELETE /brokers/{brokerId}    remove a broker (fails if topics reference it)
 * </pre>
 */
@RestController
@RequestMapping("/brokers")
public class BrokerController {

    private final BrokerRepository brokerRepository;

    record CreateBrokerRequest(
            String brokerId, String bootstrapServers, String securityProtocol,
            String saslMechanism, String saslUsername, String saslPassword,
            String sslTruststorePath, String sslTruststorePassword,
            String sslKeystorePath, String sslKeystorePassword
    ) {}

    record UpdateBrokerRequest(
            String bootstrapServers, String securityProtocol,
            String saslMechanism, String saslUsername, String saslPassword,
            String sslTruststorePath, String sslTruststorePassword,
            String sslKeystorePath, String sslKeystorePassword
    ) {}

    record BrokerConfigResponse(
            String brokerId, String bootstrapServers, String securityProtocol,
            String saslMechanism, String saslUsername, String saslPassword,
            String sslTruststorePath, String sslTruststorePassword,
            String sslKeystorePath, String sslKeystorePassword
    ) {
        static BrokerConfigResponse fromBrokerConfig(BrokerConfig cfg) {
            return new BrokerConfigResponse(
                    cfg.brokerId(),
                    cfg.bootstrapServers(),
                    cfg.securityProtocol(),
                    cfg.saslMechanism(),
                    cfg.saslUsername(),
                    cfg.saslPassword()           != null ? "****" : null,
                    cfg.sslTruststorePath(),
                    cfg.sslTruststorePassword()  != null ? "****" : null,
                    cfg.sslKeystorePath(),
                    cfg.sslKeystorePassword()    != null ? "****" : null
            );
        }
    }

    public BrokerController(BrokerRepository brokerRepository) {
        this.brokerRepository = brokerRepository;
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<BrokerConfigResponse> listBrokers() throws SQLException {
        return brokerRepository.listBrokers().stream()
                .map(BrokerConfigResponse::fromBrokerConfig)
                .toList();
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<BrokerConfigResponse> createBroker(@RequestBody CreateBrokerRequest body)
            throws SQLException {
        if (brokerRepository.findBroker(body.brokerId()).isPresent()) {
            return ResponseEntity.status(409).build();
        }
        BrokerConfig cfg = new BrokerConfig(
                body.brokerId(), body.bootstrapServers(), body.securityProtocol(),
                body.saslMechanism(), body.saslUsername(), body.saslPassword(),
                body.sslTruststorePath(), body.sslTruststorePassword(),
                body.sslKeystorePath(), body.sslKeystorePassword());
        BrokerConfig saved = brokerRepository.upsertBroker(cfg);
        return ResponseEntity.status(201).body(BrokerConfigResponse.fromBrokerConfig(saved));
    }

    @GetMapping(value = "/{brokerId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<BrokerConfigResponse> getBroker(@PathVariable String brokerId)
            throws SQLException {
        return brokerRepository.findBroker(brokerId)
                .map(BrokerConfigResponse::fromBrokerConfig)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping(value = "/{brokerId}",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<BrokerConfigResponse> upsertBroker(
            @PathVariable String brokerId,
            @RequestBody UpdateBrokerRequest body) throws SQLException {
        BrokerConfig cfg = new BrokerConfig(
                brokerId, body.bootstrapServers(), body.securityProtocol(),
                body.saslMechanism(), body.saslUsername(), body.saslPassword(),
                body.sslTruststorePath(), body.sslTruststorePassword(),
                body.sslKeystorePath(), body.sslKeystorePassword());
        BrokerConfig saved = brokerRepository.upsertBroker(cfg);
        return ResponseEntity.ok(BrokerConfigResponse.fromBrokerConfig(saved));
    }

    @DeleteMapping("/{brokerId}")
    public ResponseEntity<Void> deleteBroker(@PathVariable String brokerId) throws SQLException {
        boolean deleted = brokerRepository.deleteBroker(brokerId);
        return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<String> handleConflict(IllegalStateException ex) {
        return ResponseEntity.status(409).body(ex.getMessage());
    }

    @ExceptionHandler(SQLException.class)
    public ResponseEntity<String> handleSqlError(SQLException ex) {
        return ResponseEntity.internalServerError().body("Database error: " + ex.getMessage());
    }
}
