package com.joxette.management;

import com.joxette.config.BrokerConnectionFactory;
import com.joxette.kafka.ConsumerSettings;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
    private final BrokerConnectionFactory connectionFactory;
    private final ConfigRepository configRepository;

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

    public BrokerController(BrokerRepository brokerRepository,
                            BrokerConnectionFactory connectionFactory,
                            ConfigRepository configRepository) {
        this.brokerRepository = brokerRepository;
        this.connectionFactory = connectionFactory;
        this.configRepository = configRepository;
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

    @GetMapping(value = "/{brokerId}/topics", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> listBrokerTopics(
            @PathVariable String brokerId,
            @RequestParam(defaultValue = "false") boolean includeInternal,
            @RequestParam(required = false) String filter) {

        Map<String, TopicConfig> recordedTopics;
        try {
            recordedTopics = configRepository.listTopics().stream()
                    .collect(Collectors.toMap(TopicConfig::topic, t -> t));
        } catch (SQLException e) {
            return ResponseEntity.internalServerError().body("Database error: " + e.getMessage());
        }

        try (AdminClient adminClient = connectionFactory.adminClient(brokerId)) {
            Map<String, ?> listings = adminClient
                    .listTopics(new ListTopicsOptions().listInternal(includeInternal))
                    .namesToListings()
                    .get(5, TimeUnit.SECONDS);

            List<String> names = listings.keySet().stream()
                    .filter(n -> filter == null || filter.isBlank() || n.startsWith(filter))
                    .sorted()
                    .toList();

            Map<String, TopicDescription> descriptions = adminClient
                    .describeTopics(names)
                    .allTopicNames()
                    .get(5, TimeUnit.SECONDS);

            List<BrokerTopicInfo> result = new ArrayList<>(names.size());
            for (String name : names) {
                TopicDescription desc = descriptions.get(name);
                int partitionCount = desc != null ? desc.partitions().size() : 0;
                TopicConfig cfg = recordedTopics.get(name);
                result.add(new BrokerTopicInfo(
                        name,
                        partitionCount,
                        cfg != null,
                        cfg != null ? cfg.mode() : null));
            }
            return ResponseEntity.ok(result);
        } catch (ExecutionException e) {
            return ResponseEntity.status(503).body("Broker error: " + e.getCause().getMessage());
        } catch (TimeoutException e) {
            return ResponseEntity.status(503).body("Broker did not respond within 5s");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(503).body("Request interrupted");
        }
    }

    @GetMapping(value = "/{brokerId}/topics/{topic}/peek",
                produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<PeekMessage>> peekTopic(
            @PathVariable String brokerId,
            @PathVariable String topic,
            @RequestParam(defaultValue = "20") int limit) {
        if (limit < 1 || limit > 100) {
            return ResponseEntity.badRequest().build();
        }
        ConsumerSettings<String, byte[]> base = connectionFactory.consumerSettings(brokerId);
        Map<String, Object> props = new HashMap<>(base.toProperties());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "joxette-peek-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(
                props, base.keyDeserializer(), base.valueDeserializer())) {
            List<PartitionInfo> parts = consumer.partitionsFor(topic);
            if (parts == null || parts.isEmpty()) {
                return ResponseEntity.ok(List.of());
            }
            List<TopicPartition> tps = parts.stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();
            consumer.assign(tps);
            consumer.seekToEnd(tps);
            Map<TopicPartition, Long> ends = consumer.endOffsets(tps);
            for (TopicPartition tp : tps) {
                long end = ends.get(tp);
                long start = Math.max(0, end - (long) Math.ceil((double) limit / tps.size()));
                consumer.seek(tp, start);
            }
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(3));
            List<PeekMessage> messages = new ArrayList<>();
            records.forEach(r -> {
                if (messages.size() >= limit) return;
                byte[] rawValue = r.value();
                String encoding = rawValue != null ? detectEncoding(rawValue) : "utf8";
                String value = rawValue != null ? encodeValue(rawValue, encoding) : null;
                List<PeekMessage.HeaderEntry> headers = new ArrayList<>();
                r.headers().forEach(h -> headers.add(
                        new PeekMessage.HeaderEntry(h.key(),
                                h.value() != null ? new String(h.value(), StandardCharsets.UTF_8) : null)));
                messages.add(new PeekMessage(
                        r.partition(),
                        r.offset(),
                        Instant.ofEpochMilli(r.timestamp()).toString(),
                        r.key(),
                        value,
                        encoding,
                        headers));
            });
            messages.sort((a, b) -> a.timestamp().compareTo(b.timestamp()));
            return ResponseEntity.ok(messages);
        }
    }

    private static String detectEncoding(byte[] bytes) {
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return "utf8";
        } catch (CharacterCodingException e) {
            return "base64";
        }
    }

    private static String encodeValue(byte[] bytes, String encoding) {
        if ("base64".equals(encoding)) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        return new String(bytes, StandardCharsets.UTF_8);
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
