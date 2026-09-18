# Recording Pipeline Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate four correctness gaps in Joxette's per-partition Kafka recording pipeline — an orphaned-consumer leak on actor restart, an unbounded poison-message restart loop, silently swallowed entity-extraction failures, and a write-path latency coupling to slow SSE/NDJSON follow subscribers — so that a single bad message or a slow downstream reader can never durably wedge or duplicate a topic's ingestion.

**Architecture:** Each task targets one link in the `TopicLifecycleActor` → `TopicRecorder` → `DuckLakeWriteChannel` → `CassetteRecordingBus` chain. Task A adds a `PreRestart` cleanup hook so Pekko's backoff supervisor tears down the old per-partition `TopicRecorder`s before spawning new ones. Task B gives the write channel a bounded per-batch quarantine so a deterministically-failing batch is skipped (with a metric and an ERROR log) instead of looping forever. Task C makes `EntityIdExtractor`/`MessageRouter` distinguish and surface real extraction failures instead of swallowing them as silent no-matches. Task D moves `CassetteRecordingBus.publish` off the single DuckDB drain virtual thread so follow-subscriber fanout can never delay unrelated writes.

**Tech Stack:** Java 25, Spring Boot 4.0.5, Jox (softwaremill) flows + structured concurrency, Apache Pekko typed actors, DuckDB JDBC 1.5.3.0 / DuckLake, JUnit 5, Awaitility (for async assertions — never raw Thread.sleep before an assertion), Testcontainers (Kafka + DuckDB)

## Global Constraints

- Java 25 language features only (no Kotlin)
- Never use `Thread.sleep` before a test assertion — use `Awaitility.await().atMost(...).untilAsserted(...)`
- Prefer `@ParameterizedTest` (`@CsvSource` for scalars, `@MethodSource` for objects) over multiple near-identical `@Test` methods
- All new/modified JDBC operations on the shared `Connection` must be wrapped `synchronized(duckDB)` unless already inside a path that holds that lock
- Follow existing package conventions in `com.joxette.recording` — do not introduce new top-level packages

---

### Task A: Fix orphaned-consumer leak on `TopicLifecycleActor` restart

**Root cause:** `TopicLifecycleActor.recording()` throws on `RecorderFailed`/`SinkFailed` to trigger Pekko's `restartWithBackoff` supervisor (declared in `TopicLifecycleActor.create`, `joxette-service/src/main/java/com/joxette/recording/TopicLifecycleActor.java:91-97`). Per Apache Pekko's typed-actor fault-tolerance model, a supervised **restart** sends the actor's current behavior a `PreRestart` signal before discarding it and re-invoking the original `Behaviors.setup` factory — it does **not** send `PostStop` (that signal is reserved for a genuine stop, not a restart). `recording()`'s `Behaviors.receive(...)` chain (`TopicLifecycleActor.java:187-221`) declares no signal handler at all, so the `List<TopicRecorder> recorders` built in `starting()` (`TopicLifecycleActor.java:136-156`) is silently dropped on restart: nothing ever calls `TopicRecorder.stop()` on the healthy siblings of the partition that failed. Because per-partition recorders use manual `kc.assign()` (not `subscribe()` — see `TopicRecorder.java:216-225`), there is no group-coordination fencing to stop the orphaned `KafkaConsumer` from continuing to poll and write the same partition(s) the new generation now also owns.

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/recording/TopicLifecycleActor.java:10-27` (imports), `:170-221` (`recording()` behavior)
- Modify: `joxette-service/src/main/java/com/joxette/recording/TopicRecorder.java:73-79` (fields), `:207-322` (`run()`), `:330-347` (accessors)
- Test: `joxette-service/src/test/java/com/joxette/recording/TopicLifecycleActorRestartLeakTest.java` (new)

**Interfaces:**
- Consumes: `TopicRecorder.stop()` (existing, `TopicRecorder.java:324-328`), `TopicLifecycleActor.create(...)` (existing factory, `TopicLifecycleActor.java:73-97`), `RecorderStatus.assignedPartitions()` (existing)
- Produces: `TopicRecorder.liveConsumerCount()` — package-private static accessor other tasks/tests can use to detect consumer leaks; `TopicLifecycleActor.recording()`'s new `onSignal(PreRestart.class, ...)` handler (internal, no new public API)

- [ ] **Step 1: Write the failing test**

```java
package com.joxette.recording;

import com.joxette.config.BrokerConnectionFactory;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.BrokerRepository;
import com.joxette.management.ConfigRepository;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.replay.EntityIdExtractor;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.apache.pekko.actor.typed.ActorRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that a failure on ONE partition's recorder does not leak the OTHER,
 * healthy recorders when {@link TopicLifecycleActor} restarts.
 *
 * <p>Only partition 0 ever receives a message; the write for that message
 * fails with a non-retryable DuckDB "Catalog Error" because the general
 * cassette table is deliberately never created. This forces exactly one
 * {@code RecorderFailed}, which makes the whole actor throw and restart.
 * Partitions 1 and 2 never write anything and stay healthy the entire time —
 * they are exactly the siblings that leaked before this fix.
 */
@Testcontainers
class TopicLifecycleActorRestartLeakTest {

    private static final JoxetteMetrics TEST_METRICS = new JoxetteMetrics(new SimpleMeterRegistry());
    private static final String TOPIC = "lifecycle.leak.test.events";
    private static final int PARTITIONS = 3;

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    private ActorTestKit kit;
    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;
    private JoxetteProperties props;

    @BeforeEach
    void setUp() throws Exception {
        kit = ActorTestKit.create();
        duckDB = DuckDBTestSupport.newConnection();
        // Deliberately do NOT create lake.main.general_lifecycle_leak_test_events —
        // any write attempt fails with a non-retryable "Catalog Error".
        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO topic_configs (topic, mode) VALUES (?, 'general') ON CONFLICT DO NOTHING")) {
            ps.setString(1, TOPIC);
            ps.executeUpdate();
        }

        props = new JoxetteProperties();
        props.getKafka().setBootstrapServers(kafka.getBootstrapServers());
        props.getRecording().setRetryInitialIntervalMs(200);
        props.getRecording().setRetryMaxIntervalMs(500);
        props.getRecording().setBatchSize(10);
        props.getRecording().setBatchTimeoutMs(100);

        writeChannel = new DuckLakeWriteChannel(duckDB, props, new CassetteRecordingBus(props), TEST_METRICS);
        writeChannel.start();

        createTopic(TOPIC, PARTITIONS);
    }

    @AfterEach
    void tearDown() throws Exception {
        kit.shutdownTestKit();
        if (writeChannel != null) writeChannel.stop();
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
        deleteTopic(TOPIC);
    }

    @Test
    void restartAfterOnePartitionFailure_stopsAllOldRecordersInsteadOfLeakingHealthySiblings() throws Exception {
        BrokerRepository brokerRepository = new BrokerRepository(duckDB, props);
        BrokerConnectionFactory brokerFactory = new BrokerConnectionFactory(brokerRepository, props);
        ConfigRepository configRepo = new ConfigRepository(duckDB, props);
        MessageRouter router = new MessageRouter(configRepo, new EntityIdExtractor());
        KnownEntitiesRepository knownEntities =
                new KnownEntitiesRepository(org.jooq.impl.DSL.using(duckDB, org.jooq.SQLDialect.DUCKDB));
        Executor vtExecutor = Executors.newVirtualThreadPerTaskExecutor();

        ActorRef<TopicLifecycleActor.Cmd> actor = kit.spawn(
                TopicLifecycleActor.create(TOPIC, "earliest", Instant.now(), props, brokerFactory,
                        null, writeChannel, router, knownEntities, vtExecutor, TEST_METRICS));

        // Wait for all 3 per-partition recorders to open a live KafkaConsumer.
        await().atMost(Duration.ofSeconds(15))
               .untilAsserted(() -> assertThat(TopicRecorder.liveConsumerCount()).isEqualTo(PARTITIONS));

        // Only partition 0 ever gets a message; partitions 1 and 2 stay idle and
        // healthy while partition 0's write fails against the missing table.
        publishToPartition(0, "trigger");

        // Wait for the actor to come back up with a fresh generation of exactly
        // PARTITIONS recorders (proves the restart completed).
        TestProbe<RecorderStatus> probe = kit.createTestProbe(RecorderStatus.class);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            actor.tell(new TopicLifecycleActor.GetStatus(probe.ref()));
            RecorderStatus status = probe.receiveMessage(Duration.ofSeconds(2));
            assertThat(status.assignedPartitions()).hasSize(PARTITIONS);
        });

        // Before the fix: partitions 1 and 2's original KafkaConsumers are never
        // stopped, so once the new generation's 3 recorders also come up the live
        // count sits at 6 forever. After the fix: PreRestart cleanup stops all 3
        // old recorders, so the count settles back to exactly PARTITIONS.
        await().atMost(Duration.ofSeconds(15))
               .untilAsserted(() -> assertThat(TopicRecorder.liveConsumerCount())
                       .as("old generation's recorders must be stopped, not doubled up with the new generation")
                       .isEqualTo(PARTITIONS));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void publishToPartition(int partition, String value) throws Exception {
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"))) {
            producer.send(new ProducerRecord<>(TOPIC, partition, "k", value.getBytes(StandardCharsets.UTF_8))).get();
        }
    }

    private void createTopic(String topic, int partitions) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(15, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private void deleteTopic(String topic) {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.deleteTopics(List.of(topic)).all().get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }
}
```

- [ ] **Step 2: Run test to verify it fails**
  Run: `mvn -pl joxette-service test -Dtest=TopicLifecycleActorRestartLeakTest#restartAfterOnePartitionFailure_stopsAllOldRecordersInsteadOfLeakingHealthySiblings`
  Expected: FAIL — compilation error, because `TopicRecorder.liveConsumerCount()` does not exist yet (added in Step 3). Once that accessor is stubbed in isolation (temporarily, to confirm the *behavioral* half of the failure), the test times out on the final `await()`: `liveConsumerCount()` sits at `6` (3 leaked old + 3 new) instead of settling to `3`, because `TopicLifecycleActor.recording()` has no `PreRestart` handler to stop the old generation's recorders.

- [ ] **Step 3: Write minimal implementation**

`TopicRecorder.java` — add a package-visible live-consumer counter, incremented/decremented around the `KafkaConsumer` lifecycle in `run()`:

```java
// TopicRecorder.java:73-79 — add AtomicInteger import at top of file (near the
// existing java.util.concurrent.atomic.AtomicLong import) and a new static field:
import java.util.concurrent.atomic.AtomicInteger;
// ...
public class TopicRecorder {

    private static final Logger log = LoggerFactory.getLogger(TopicRecorder.class);
    private static final Duration POLL_TIMEOUT          = Duration.ofMillis(100);
    private static final long     KAFKA_RETRY_INITIAL_MS = 500;
    private static final double   KAFKA_RETRY_MULTIPLIER = 2.0;
    private static final long     KAFKA_RETRY_MAX_MS     = 30_000;
    /**
     * Count of TopicRecorder instances with a currently-open (unclosed)
     * KafkaConsumer. Incremented right after {@code settings.toConsumer()}
     * succeeds in {@link #run()}, decremented after {@code kc.close()} in the
     * matching {@code finally} block — regardless of how run() exits (clean
     * stop, exception, interrupt). Used to detect orphaned-consumer leaks
     * across actor restarts (see TopicLifecycleActorRestartLeakTest).
     */
    private static final AtomicInteger liveConsumerCount = new AtomicInteger(0);
```

```java
// TopicRecorder.java:207-212 — increment right after the consumer is created:
    public void run() throws Exception {
        String label = assignedPartition != null ? topic + "[" + assignedPartition + "]" : topic;
        log.info("Starting recorder for '{}'", label);

        KafkaConsumer<String, byte[]> kc = settings.toConsumer();
        liveConsumerCount.incrementAndGet();
        try {
            this.consumer = kc;
```

```java
// TopicRecorder.java:316-321 — decrement in the finally block, after close():
        } finally {
            this.consumer = null;
            assignedPartitions.clear();
            kc.close(Duration.ofSeconds(5));
            liveConsumerCount.decrementAndGet();
            log.info("Recorder for topic '{}' stopped", topic);
        }
    }
```

```java
// TopicRecorder.java:330-331 — add the accessor next to the other simple accessors:
    public boolean isStopped() { return stopped; }

    /** Package-visible: number of TopicRecorder instances with a live KafkaConsumer right now. */
    static int liveConsumerCount() { return liveConsumerCount.get(); }
```

`TopicLifecycleActor.java` — add the `PreRestart` cleanup hook:

```java
// TopicLifecycleActor.java:10-13 — add the PreRestart import next to the other pekko.actor.typed imports:
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PreRestart;
import org.apache.pekko.actor.typed.SupervisorStrategy;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
```

```java
// TopicLifecycleActor.java:212-221 — add an onSignal(PreRestart.class, ...) handler
// before .build() in recording(), so Pekko's restartWithBackoff supervisor stops
// every currently-tracked TopicRecorder before the new generation is spawned:
                .onMessage(SinkFailed.class, msg -> {
                    log.error("TopicLifecycleActor: write sink exhausted retries for topic '{}' — restarting actor: {}",
                            topic, msg.cause().getMessage());
                    joxetteMetrics.recordingMetrics(topic).restarts().increment();
                    // Consumers are already paused; Pekko will restart this actor via backoff.
                    // The next starting() call resets sink state so consumers can resume.
                    throw new RuntimeException("Sink failed for topic " + topic, msg.cause());
                })
                .onSignal(PreRestart.class, sig -> {
                    // Pekko sends PreRestart (not PostStop) to the CURRENT behavior before
                    // discarding it and re-invoking the original Behaviors.setup factory.
                    // Without this hook the `recorders` captured in this closure are simply
                    // dropped: their KafkaConsumers keep polling/writing/committing the same
                    // partitions the new generation is about to also claim.
                    log.warn("TopicLifecycleActor: restarting topic '{}' — stopping {} in-flight recorder(s) " +
                                    "to prevent an orphaned-consumer leak",
                            topic, recorders.size());
                    recorders.forEach(TopicRecorder::stop);
                    return Behaviors.same();
                })
                .build();
```

- [ ] **Step 4: Run test to verify it passes**
  Run: `mvn -pl joxette-service test -Dtest=TopicLifecycleActorRestartLeakTest#restartAfterOnePartitionFailure_stopsAllOldRecordersInsteadOfLeakingHealthySiblings`
  Expected: PASS

- [ ] **Step 5: Commit**
  ```bash
  git add joxette-service/src/main/java/com/joxette/recording/TopicRecorder.java \
          joxette-service/src/main/java/com/joxette/recording/TopicLifecycleActor.java \
          joxette-service/src/test/java/com/joxette/recording/TopicLifecycleActorRestartLeakTest.java
  git commit -m "fix(recording): stop all per-partition recorders via PreRestart before TopicLifecycleActor restart"
  ```

---

### Task B: Bound the poison-message restart loop

**Root cause:** `DuckLakeWriteChannel.processBatch`'s non-retryable branch (`joxette-service/src/main/java/com/joxette/recording/DuckLakeWriteChannel.java:269-274`) fails the batch immediately on the first attempt and returns — it never commits the offending offsets. `TopicRecorder.submitWriteBatch` (`TopicRecorder.java:420-450`) only calls `pendingCommit.set(buildOffsets(wb.sourceRecords()))` (`TopicRecorder.java:294`) *after* `writeChannel.submit(wb)` returns successfully, so a permanently-failing batch's offsets are never advanced. The exception propagates out of `TopicRecorder.run()` → becomes `RecorderFailed` → (after Task A) the actor restarts cleanly, but the new generation resumes from the *same* uncommitted offset, re-reads the *same* poison message(s), gets the *same* non-retryable error, and restarts again — forever, with zero progress and no operator-visible signal beyond a repeating ERROR log line.

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java:514-561` (`Threading` class)
- Modify: `joxette-service/src/main/java/com/joxette/recording/WriteBatch.java:1-13` (imports), `:58-64` (new method)
- Modify: `joxette-service/src/main/java/com/joxette/recording/DuckLakeWriteChannel.java:1-24` (imports), `:52-90` (fields/constructor), `:268-274` (quarantine logic), `:313-317` (new helper)
- Modify: `joxette-service/src/main/java/com/joxette/metrics/JoxetteMetrics.java:186-212` (new counter method)
- Test: `joxette-service/src/test/java/com/joxette/recording/DuckLakeWriteChannelQuarantineTest.java` (new)

**Interfaces:**
- Consumes: `DuckDbErrors.isTransient(Throwable)` (existing, unchanged), `WriteBatch.topic()`/`.sourceRecords()` (existing)
- Produces: `WriteBatch.minOffsetsByPartition()` — new method other tasks/tests can rely on for stable batch identity; `JoxetteProperties.Threading.getQuarantineAfterAttempts()`/`setQuarantineAfterAttempts(int)`; `JoxetteMetrics.batchesQuarantined(String topic)` returning a `Counter`

---

**REVISION (post-task-review, supersedes the identity/quarantine mechanism above — file list and interfaces above still apply as the starting point):**

The whole-batch composite identity (`batchIdentity()`, keyed by every partition's min-offset in the `WriteBatch`) is unstable on multi-partition topics under concurrent traffic: `TopicRecorder`'s `batchWeighted` coalescing mixes records from unrelated partitions into one `WriteBatch`, and which partitions land together varies by poll timing across restarts. A composite key built from that mix changes between restarts even when the same single partition is the actual poison source, so the failure counter can reset before reaching `quarantineAfterAttempts` — the restart loop this task exists to bound can persist indefinitely on busy multi-partition topics. It also means quarantining (when it does trigger) silently drops every record from every partition co-batched with the poison one, not just the poison partition's records.

**Corrected design — split by partition on non-retryable failure, quarantine tracking keyed by (topic, partition, min-offset) alone:**

1. **`WriteBatch.java`** — add `public List<WriteBatch> splitByPartition()`: groups `sourceRecords` by `ConsumerRecord::partition` (preserve encounter order, e.g. `Collectors.groupingBy(ConsumerRecord::partition, LinkedHashMap::new, Collectors.toList())`), and for each partition builds a sub-`WriteBatch` via the existing `WriteBatch.of(...)` factory: `sourceRecords` = that partition's slice; `generalRecords`/`generalMessageTypes` = the parallel-indexed subset where `generalRecords.get(i).partition() == partition` (filter both lists in lockstep by index — they must stay index-aligned); `entityItems` = the subset where `item.message().partition() == partition`. Each resulting sub-batch has exactly one partition, so `WriteBatch.of`'s own `partitions()` computation naturally yields a singleton set — this is what makes recursion terminate (see step 3).

2. **`DuckLakeWriteChannel.processBatch`** — in the non-retryable branch (currently: compute `batchIdentity`, count, quarantine-or-fail), insert a check **before** identity/quarantine logic: `if (batch.partitions().size() > 1) { splitAndProcessByPartition(batch, writers, e); return; }`. Multi-partition batches never reach the identity/quarantine code directly — they always get split first. Single-partition batches (including every sub-batch produced by a split) fall through to the existing identity/quarantine logic unchanged — `batchIdentity()` on a single-partition batch is already stable (one partition, one min-offset), so no change needed there.

3. **New method `splitAndProcessByPartition(WriteBatch batch, WriterSet writers, Exception firstFailure)`:**
   ```java
   private void splitAndProcessByPartition(WriteBatch batch, WriterSet writers, Exception firstFailure) {
       log.warn("Non-retryable write failure for multi-partition batch on topic '{}' (partitions={}) — " +
                       "splitting into per-partition sub-batches to isolate the failure: {}",
               batch.topic(), batch.partitions(), firstFailure.getMessage());
       sinkState.set(SinkState.HEALTHY); // not a storage issue — same as the existing non-retryable branch
       int totalWritten = 0;
       Exception firstSubFailure = null;
       for (WriteBatch sub : batch.splitByPartition()) {
           processBatch(sub, writers); // recursive call; sub.partitions().size() == 1 so this cannot re-split
           try {
               totalWritten += sub.result().join().recordsWritten();
           } catch (Exception subEx) {
               if (firstSubFailure == null) firstSubFailure = subEx;
           }
       }
       if (firstSubFailure != null) {
           batch.result().completeExceptionally(firstSubFailure);
       } else {
           batch.result().complete(new WriteResult(batch.topic(), totalWritten));
       }
   }
   ```
   `sub.result()` is always already completed by the time `processBatch(sub, writers)` returns (the drain loop is single-threaded and `processBatch` only completes the future synchronously before returning), so `.join()` does not block. This preserves the original umbrella `batch.result()` contract that `TopicRecorder.submitWriteBatch` blocks on: healthy partitions' writes succeed and their bytes are durably in DuckDB either way, but `TopicRecorder` only advances its Kafka offset commit for the *whole* original multi-partition assignment when no sub-batch failed exceptionally (matches existing all-or-nothing offset-commit semantics — a sub-batch that gets quarantined still completes its own future successfully with `recordsWritten=0`, so quarantine alone does not block the parent's offset commit; only a genuine unresolved transient failure on one of the partitions does, exactly like today).

4. **Test coverage must include the scenario the original test missed:** a `WriteBatch` spanning ≥2 partitions where partition A's records always fail non-retryably (missing table / catalog error) and partition B's records always succeed. Assert: (a) partition B's records ARE written (check via a query or writer spy) and are NOT lost; (b) after `quarantineAfterAttempts` restarts *of a batch containing only partition A's poison record* (i.e., simulate the realistic case where partition A's poison offset keeps recurring alone across restarts, not bundled identically with B every time), partition A quarantines; (c) partition B is never quarantined and never accumulates a false failure count from partition A's problem. The original single-batch, single-partition `DuckLakeWriteChannelQuarantineTest` scenario should be kept too (still valid, still exercises the identity/quarantine logic that now runs on single-partition batches, split or not).

5. **`TopicRecorder.java:435-450` (`submitWriteBatch`) — fix the `messagesWritten` metric to reflect actual writes, not attempted writes.** Currently `messagesWritten.addAndGet(recordCount)` (`TopicRecorder.java:448`) unconditionally uses `wb.sourceRecords().size()` — the full attempted count — regardless of what `writeChannel.submit(wb)` actually returned, so a quarantined (partially or fully dropped) batch is still counted as fully written in this throughput metric. `meters.writeDuration().record(() -> {...})` (`TopicRecorder.java:444-447`) currently discards `submit`'s return value because `Timer.record(Runnable)` returns `void`; change it to `Timer.recordCallable(Callable<T>)` (verify this exact method exists on whatever `meters.writeDuration()` returns — read `TopicRecorder`'s field declaration and the Micrometer `Timer` API before writing this; it throws `Exception` so the existing `InterruptedException` catch/rethrow-as-`RuntimeException` wrapping must move inside or around the callable accordingly) to capture the `WriteResult`, then do `messagesWritten.addAndGet(result.recordsWritten())` and `meters.messagesWritten().increment(result.recordsWritten())` using the real value instead of `recordCount`. Keep the log line at `TopicRecorder.java:441-442` using `recordCount` (attempted) since that log message is about what was submitted, not what ultimately got written — do not change its wording.

- [ ] **Step 1: Write the failing test**

```java
package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves that a batch which deterministically fails with a non-retryable DuckDB
 * error is skipped ("quarantined") after {@code quarantineAfterAttempts}
 * re-submissions of the SAME underlying offsets — mirroring what happens across
 * repeated actor restarts when a poison message can never be written — instead
 * of failing forever with zero progress.
 */
class DuckLakeWriteChannelQuarantineTest {

    private static final String TOPIC = "quarantine.test.events";
    private static final int QUARANTINE_AFTER_ATTEMPTS = 3;

    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;
    private SimpleMeterRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        // Deliberately never create lake.main.general_quarantine_test_events —
        // every write attempt fails with a non-retryable "Catalog Error".

        JoxetteProperties props = new JoxetteProperties();
        props.getThreading().setQuarantineAfterAttempts(QUARANTINE_AFTER_ATTEMPTS);

        registry = new SimpleMeterRegistry();
        writeChannel = new DuckLakeWriteChannel(duckDB, props, new CassetteRecordingBus(props),
                new JoxetteMetrics(registry));
        writeChannel.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writeChannel != null) writeChannel.stop();
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void samePoisonBatch_isQuarantinedAfterConfiguredAttempts_thenLetsTopicProgress() {
        // Attempts 1 and 2 (below the threshold): submit() must still throw — this
        // is today's existing fail-fast-but-never-committed behaviour, unchanged.
        for (int attempt = 1; attempt <= QUARANTINE_AFTER_ATTEMPTS - 1; attempt++) {
            WriteBatch batch = poisonBatch(0L);
            assertThatThrownBy(() -> writeChannel.submit(batch));
        }
        assertThat(extractionFailureFreeCounter()).isZero(); // sanity: nothing quarantined yet

        // Attempt == threshold: the channel gives up retrying this exact identity,
        // logs it, and completes the batch successfully with zero records written
        // so the recorder can commit past it instead of restarting forever.
        WriteBatch quarantined = poisonBatch(0L);
        WriteResult result;
        try {
            result = writeChannel.submit(quarantined);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertThat(result.recordsWritten()).isZero();

        assertThat(quarantinedCount(TOPIC)).isEqualTo(1.0);

        // A batch with a DIFFERENT offset identity must NOT be quarantined by the
        // previous batch's failures — only the specific poison identity is skipped.
        WriteBatch differentIdentity = poisonBatch(100L);
        assertThatThrownBy(() -> writeChannel.submit(differentIdentity));
    }

    private double quarantinedCount(String topic) {
        var counter = registry.find("joxette.recording.batches_quarantined").tag("topic", topic).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private double extractionFailureFreeCounter() {
        var counter = registry.find("joxette.recording.batches_quarantined").tag("topic", TOPIC).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private WriteBatch poisonBatch(long baseOffset) {
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
        List<String> types = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ConsumerRecord<String, byte[]> r = new ConsumerRecord<>(
                    TOPIC, 0, baseOffset + i,
                    1_700_000_000_000L + i, TimestampType.CREATE_TIME,
                    -1, -1,
                    "k" + i,
                    ("payload-" + i).getBytes(StandardCharsets.UTF_8),
                    new RecordHeaders(),
                    Optional.empty());
            records.add(r);
            types.add(null);
        }
        return WriteBatch.of(TOPIC, records, records, types, List.of());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**
  Run: `mvn -pl joxette-service test -Dtest=DuckLakeWriteChannelQuarantineTest#samePoisonBatch_isQuarantinedAfterConfiguredAttempts_thenLetsTopicProgress`
  Expected: FAIL — compilation error (`JoxetteProperties.Threading.setQuarantineAfterAttempts(int)` does not exist yet). After stubbing that setter alone, the test fails at `assertThat(result.recordsWritten()).isZero()` because `submit()` on the third attempt still throws (no quarantine logic exists), and `quarantinedCount(TOPIC)` stays `0.0`.

- [ ] **Step 3: Write minimal implementation**

`JoxetteProperties.java` — add the threshold property:

```java
// JoxetteProperties.java:535 — insert after writeRetryMaxAttempts's field declaration:
        private int writeRetryMaxAttempts = 10;
        /**
         * Number of consecutive non-retryable ("poison") write failures for the
         * SAME batch identity (same topic + per-partition starting offsets) before
         * the drain VT gives up retrying it, logs it at ERROR, and commits past it
         * so the topic keeps making progress instead of restarting forever.
         */
        private int quarantineAfterAttempts = 5;
```

```java
// JoxetteProperties.java:559-560 — insert after the writeRetryMaxAttempts getter/setter:
        public int getWriteRetryMaxAttempts() { return writeRetryMaxAttempts; }
        public void setWriteRetryMaxAttempts(int v) { this.writeRetryMaxAttempts = v; }

        public int getQuarantineAfterAttempts() { return quarantineAfterAttempts; }
        public void setQuarantineAfterAttempts(int v) { this.quarantineAfterAttempts = v; }
```

`WriteBatch.java` — add a stable per-partition identity helper:

```java
// WriteBatch.java:8-13 — add HashMap/Map imports:
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
```

```java
// WriteBatch.java:63 — insert after recordCount():
    public long recordCount() { return sourceRecords.size(); }

    /**
     * Minimum {@code kafka_offset} present in {@link #sourceRecords()} per
     * partition. Used by {@link DuckLakeWriteChannel} as a stable identity for a
     * "poison" batch: the {@code WriteBatch} object itself is a fresh instance on
     * every poll/restart (a new {@link CompletableFuture} each time), but the
     * earliest uncommitted offset per partition is deterministic across restarts
     * because a non-retryable write failure never advances the committed offset.
     */
    public Map<Integer, Long> minOffsetsByPartition() {
        Map<Integer, Long> mins = new HashMap<>();
        for (ConsumerRecord<String, byte[]> r : sourceRecords) {
            mins.merge(r.partition(), r.offset(), Math::min);
        }
        return mins;
    }
```

`DuckLakeWriteChannel.java` — quarantine logic:

```java
// DuckLakeWriteChannel.java:24 — add TreeMap import next to the other java.util imports:
import java.util.TreeMap;
```

```java
// DuckLakeWriteChannel.java:58 — add the field next to writeRetryMaxAttempts:
    private final int writeRetryMaxAttempts;
    private final int quarantineAfterAttempts;
```

```java
// DuckLakeWriteChannel.java:72-73 — add the failure-tracking map next to inFlight/stopped
// (only ever touched by the single drain VT — no synchronization needed):
    private final Set<WriteBatch> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    /**
     * Tracks consecutive non-retryable failures per "poison batch" identity (see
     * {@link #batchIdentity(WriteBatch)}) so a deterministically-failing batch can
     * be quarantined instead of looping forever across actor restarts.
     */
    private final Map<String, Integer> nonTransientFailureCounts = new HashMap<>();
```

```java
// DuckLakeWriteChannel.java:84 — populate the new field in the constructor:
        this.writeRetryMaxAttempts = properties.getThreading().getWriteRetryMaxAttempts();
        this.quarantineAfterAttempts = properties.getThreading().getQuarantineAfterAttempts();
```

```java
// DuckLakeWriteChannel.java:268-274 — replace the non-retryable branch:
            } catch (Exception e) {
                if (!DuckDbErrors.isTransient(e)) {
                    String identity = batchIdentity(batch);
                    int failures = nonTransientFailureCounts.merge(identity, 1, Integer::sum);
                    sinkState.set(SinkState.HEALTHY); // not a storage issue — stay healthy
                    if (failures >= quarantineAfterAttempts) {
                        nonTransientFailureCounts.remove(identity);
                        log.error("Quarantining permanently-failing batch for topic '{}' after {} non-retryable " +
                                        "attempts (identity={}, {} record(s) dropped) — committing past it so the " +
                                        "topic can make progress: {}",
                                batch.topic(), failures, identity, batch.sourceRecords().size(), e.getMessage(), e);
                        joxetteMetrics.batchesQuarantined(batch.topic()).increment();
                        batch.result().complete(new WriteResult(batch.topic(), 0));
                        return;
                    }
                    log.error("Non-retryable write failure for topic '{}' (attempt {}/{}, identity={}): {}",
                            batch.topic(), failures, quarantineAfterAttempts, identity, e.getMessage(), e);
                    batch.result().completeExceptionally(e);
                    return;
                }
```

```java
// DuckLakeWriteChannel.java:317 — add the identity helper next to rootMessage():
    private static String rootMessage(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null) cur = cur.getCause();
        return cur.getMessage();
    }

    /**
     * Stable identity for a batch's underlying Kafka records, independent of the
     * {@link WriteBatch} object's own identity (a fresh instance is built on every
     * poll/restart). Two batches that both start at the same offset(s) on the same
     * partition(s) are treated as "the same poison batch" for quarantine purposes.
     */
    private static String batchIdentity(WriteBatch batch) {
        var mins = new TreeMap<>(batch.minOffsetsByPartition());
        StringBuilder sb = new StringBuilder(batch.topic()).append('#');
        boolean first = true;
        for (var e : mins.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append(e.getKey()).append(':').append(e.getValue());
        }
        return sb.toString();
    }
```

`JoxetteMetrics.java` — new counter:

```java
// JoxetteMetrics.java:211 — insert after registerWriteChannelDepthGauge():
    /**
     * Counter incremented when {@link com.joxette.recording.DuckLakeWriteChannel}
     * gives up retrying a deterministically-failing (non-retryable) batch and
     * commits past it so the topic can keep making progress. See
     * {@code joxette.threading.quarantine-after-attempts}.
     */
    public Counter batchesQuarantined(String topic) {
        return Counter.builder("joxette.recording.batches_quarantined")
                .description("Batches permanently skipped after exceeding the non-retryable write-failure quarantine threshold")
                .tag("topic", topic)
                .register(registry);
    }
```

- [ ] **Step 4: Run test to verify it passes**
  Run: `mvn -pl joxette-service test -Dtest=DuckLakeWriteChannelQuarantineTest#samePoisonBatch_isQuarantinedAfterConfiguredAttempts_thenLetsTopicProgress`
  Expected: PASS

- [ ] **Step 5: Commit**
  ```bash
  git add joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java \
          joxette-service/src/main/java/com/joxette/recording/WriteBatch.java \
          joxette-service/src/main/java/com/joxette/recording/DuckLakeWriteChannel.java \
          joxette-service/src/main/java/com/joxette/metrics/JoxetteMetrics.java \
          joxette-service/src/test/java/com/joxette/recording/DuckLakeWriteChannelQuarantineTest.java
  git commit -m "fix(recording): quarantine deterministically-failing batches instead of restarting forever"
  ```

---

### Task C: Make entity-extraction failures observable

**Root cause:** `EntityIdExtractor.extractFromJson` (`joxette-service/src/main/java/com/joxette/replay/EntityIdExtractor.java:78-98`) catches `PathNotFoundException` (a normal "no id present" outcome) and every other `Exception` (a genuine parse/evaluation failure — malformed JSON, JsonPath engine error) identically: both become `Optional.empty()`, with zero logging. `MessageRouter.route` (`joxette-service/src/main/java/com/joxette/replay/MessageRouter.java:141-195`) only logs at TRACE, and only when *nothing at all* matched for the whole message (`MessageRouter.java:190-193`) — there is no way, from logs or metrics, to tell "this message legitimately has no entity id" apart from "the extraction expression is broken and every message on this topic is silently being dropped from its entity cassette."

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/EntityIdExtractor.java` (full rewrite of the extraction methods, `:1-109`)
- Modify: `joxette-service/src/main/java/com/joxette/replay/MessageRouter.java:1-20` (imports), `:53-74` (field/constructor), `:152-168` (route loop)
- Modify: `joxette-service/src/main/java/com/joxette/metrics/JoxetteMetrics.java` (new counter method, added next to Task B's `batchesQuarantined`)
- Modify: `joxette-service/src/test/java/com/joxette/recording/RebalanceIntegrationTest.java:98` (constructor call site)
- Modify: `joxette-service/src/test/java/com/joxette/recording/TopicRecorderTest.java:107` (constructor call site)
- Modify: `joxette-service/src/test/java/com/joxette/recording/TopicLifecycleActorRestartLeakTest.java` (constructor call site — added in Task A)
- Modify: `joxette-service/src/test/java/com/joxette/replay/MessageRouterTest.java:1-96` (imports, static metrics field, `routerFor`, new parameterized test)
- Test: `joxette-service/src/test/java/com/joxette/replay/EntityIdExtractorTest.java` (new parameterized tests appended)

**Interfaces:**
- Consumes: `KafkaMessage.topic()` (existing), `JoxetteMetrics` (existing class, new method added in this task)
- Produces: `EntityIdExtractor.Extraction` record (`value()`, `failed()`, `failureReason()`) and `EntityIdExtractor.extractDetailed(KafkaMessage, IdSource, String)` — new public API used by `MessageRouter`; `MessageRouter(ConfigRepository, EntityIdExtractor, JoxetteMetrics)` — new 3-arg constructor (breaking change, all call sites updated in this task); `JoxetteMetrics.entityExtractionFailures(String topic, String entityType)` returning a `Counter`

- [ ] **Step 1: Write the failing test**

Add to `EntityIdExtractorTest.java` (extractor-level: distinguishing failed vs. no-match):

```java
// Add to EntityIdExtractorTest.java — new imports at the top of the file:
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

// Add as new test methods inside EntityIdExtractorTest:

    // -----------------------------------------------------------------------
    // extractDetailed — distinguishes extraction failures from "no match"
    // -----------------------------------------------------------------------

    static Stream<Arguments> detailedExtractionCases() {
        return Stream.of(
                Arguments.of(
                        "malformed JSON is a failure",
                        "not-json".getBytes(StandardCharsets.UTF_8),
                        "$.order_id",
                        true,   // expectFailed
                        false   // expectPresent
                ),
                Arguments.of(
                        "valid JSON with a missing path is NOT a failure",
                        "{\"status\":\"pending\"}".getBytes(StandardCharsets.UTF_8),
                        "$.order_id",
                        false,
                        false
                ),
                Arguments.of(
                        "valid JSON with a matching path succeeds",
                        "{\"order_id\":\"ORD-1\"}".getBytes(StandardCharsets.UTF_8),
                        "$.order_id",
                        false,
                        true
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("detailedExtractionCases")
    void extractDetailed_distinguishesFailureFromNoMatch(
            String description, byte[] json, String expression, boolean expectFailed, boolean expectPresent) {
        KafkaMessage msg = message("orders.events", null, json);
        EntityIdExtractor.Extraction result = extractor.extractDetailed(msg, IdSource.VALUE, expression);

        assertThat(result.failed()).isEqualTo(expectFailed);
        assertThat(result.value().isPresent()).isEqualTo(expectPresent);
        if (expectFailed) {
            assertThat(result.failureReason()).isNotBlank();
        }
    }
```

Add to `MessageRouterTest.java` (router-level: proving the counter and route outcome differ correctly):

```java
// Add to MessageRouterTest.java — new imports at the top of the file:
import com.joxette.metrics.JoxetteMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

// Add as a static field inside MessageRouterTest, near the top of the class body:
    private static final SimpleMeterRegistry TEST_REGISTRY = new SimpleMeterRegistry();
    private static final JoxetteMetrics TEST_METRICS = new JoxetteMetrics(TEST_REGISTRY);

// Add as new test methods inside MessageRouterTest:

    static Stream<Arguments> extractionFailureCases() {
        return Stream.of(
                Arguments.of(
                        "malformed JSON increments the failure counter and yields no route",
                        "not-json".getBytes(StandardCharsets.UTF_8),
                        "$.order_id",
                        false,  // expectRoutePresent
                        1L      // expectedFailureCount
                ),
                Arguments.of(
                        "valid JSON with a non-existent path yields no route without incrementing the failure counter",
                        "{\"status\":\"pending\"}".getBytes(StandardCharsets.UTF_8),
                        "$.order_id",
                        false,
                        0L
                ),
                Arguments.of(
                        "valid JSON with a matching path yields a route without incrementing the failure counter",
                        "{\"order_id\":\"ORD-1\"}".getBytes(StandardCharsets.UTF_8),
                        "$.order_id",
                        true,
                        0L
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("extractionFailureCases")
    void route_distinguishesExtractionFailureFromNoMatch(
            String description, byte[] json, String expression, boolean expectRoutePresent, long expectedFailureCount) {
        String topic = "extraction.failure.topic";
        String entityType = "extraction-failure-" + java.util.UUID.randomUUID();
        StubConfigRepository repo = new StubConfigRepository();
        repo.addTopic(topic, TopicMode.BOTH);
        repo.addEntityType(entityType, 4);
        repo.addSource(entityType, topic, TopicMode.BOTH,
                List.of(matcher("created", IdSource.VALUE, expression)));

        MessageRouter router = new MessageRouter(repo, new EntityIdExtractor(), TEST_METRICS);
        KafkaMessage msg = new KafkaMessage(topic, 0, 0L, System.currentTimeMillis(), null, json, List.of());

        RouteDecision decision = router.route(msg);

        assertThat(decision.entityRoutes()).hasSize(expectRoutePresent ? 1 : 0);
        assertThat(extractionFailureCount(topic, entityType)).isEqualTo((double) expectedFailureCount);
    }

    private double extractionFailureCount(String topic, String entityType) {
        var counter = TEST_REGISTRY.find("joxette.recording.entity_extraction_failures")
                .tag("topic", topic)
                .tag("entity_type", entityType)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }
```

- [ ] **Step 2: Run test to verify it fails**
  Run: `mvn -pl joxette-service test -Dtest=EntityIdExtractorTest,MessageRouterTest`
  Expected: FAIL — compilation error: `EntityIdExtractor.extractDetailed(...)`/`EntityIdExtractor.Extraction` do not exist yet, and `MessageRouter`'s constructor does not accept a `JoxetteMetrics` argument yet.

- [ ] **Step 3: Write minimal implementation**

`EntityIdExtractor.java` — full replacement:

```java
package com.joxette.replay;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Extracts a scalar entity ID from a {@link KafkaMessage} using a configured
 * source/expression pair.
 *
 * <p>Three sources are supported:
 * <dl>
 *   <dt>{@code "key"}</dt>
 *   <dd>The raw message key string. {@code expression} is ignored.</dd>
 *   <dt>{@code "value"}</dt>
 *   <dd>The message value bytes parsed as JSON; {@code expression} is a
 *       JSONPath (e.g. {@code $.order_id}) applied to the parsed document.</dd>
 *   <dt>{@code "header"}</dt>
 *   <dd>The first header whose key equals {@code expression}, decoded as
 *       UTF-8.</dd>
 * </dl>
 *
 * <p>{@link #extract} returns {@link Optional#empty()} both when no id is
 * present (normal) and when extraction threw (a real error) — kept for
 * backward compatibility with callers that only care about the value.
 * {@link #extractDetailed} distinguishes the two via {@link Extraction#failed()}
 * so callers like {@link MessageRouter} can log/alert only on genuine failures.
 */
@Component
public class EntityIdExtractor {

    private static final Logger log = LoggerFactory.getLogger(EntityIdExtractor.class);

    /**
     * Cache of pre-compiled {@link JsonPath} instances, keyed on the raw
     * expression string (e.g. {@code $.order_id}).
     *
     * <p>Compiling a JsonPath expression is pure-syntax work that produces an
     * identical AST for the same string every time — it carries no
     * per-message or per-topic state.  The set of configured expressions is
     * small (typically &lt; 20) and fixed at startup, so the cache never
     * evicts in practice; {@code maximumSize} is a defensive safety cap.
     *
     * <p>{@code recordStats()} adds zero overhead on the hot path and allows
     * the stats to be exposed via Micrometer/Prometheus in the future.
     */
    private final LoadingCache<String, JsonPath> compiledPaths = Caffeine.newBuilder()
            .maximumSize(1_000)
            .recordStats()
            .build(JsonPath::compile);

    /**
     * Outcome of an extraction attempt.
     *
     * <p>{@code failed=true} means the extraction THREW (malformed JSON, JsonPath
     * evaluation error, etc.) — distinct from a normal "no id present" outcome
     * ({@code failed=false, value=Optional.empty()}), which happens whenever the
     * message simply doesn't carry this entity's id (missing key, path not
     * found, absent header). Callers that need to log/alert on genuine
     * extraction errors (see {@link MessageRouter#route}) should branch on
     * {@link #failed()}, not on {@link #value()} being empty.
     */
    public record Extraction(Optional<String> value, boolean failed, String failureReason) {
        static Extraction of(Optional<String> value) { return new Extraction(value, false, null); }
        static Extraction failure(String reason) { return new Extraction(Optional.empty(), true, reason); }
    }

    /**
     * Attempts to extract an entity ID from {@code message} using the given
     * {@code source} discriminant and {@code expression}.
     *
     * @param message    the Kafka message to inspect
     * @param source     where to evaluate the expression
     * @param expression JSONPath for {@code "value"} source; header name for
     *                   {@code "header"} source; ignored for {@code "key"} source
     * @return the extracted entity ID, or empty if no id is present OR extraction failed
     */
    public Optional<String> extract(KafkaMessage message,
                                     com.joxette.management.IdSource source,
                                     String expression) {
        return extractDetailed(message, source, expression).value();
    }

    /**
     * Like {@link #extract}, but distinguishes an extraction failure (an
     * exception thrown while parsing/evaluating) from a normal "no id present"
     * outcome via {@link Extraction#failed()}.
     */
    public Extraction extractDetailed(KafkaMessage message,
                                       com.joxette.management.IdSource source,
                                       String expression) {
        return switch (source) {
            case KEY    -> Extraction.of(extractFromKey(message.key()));
            case VALUE  -> extractFromJsonDetailed(message.value(), expression, message.topic());
            case HEADER -> Extraction.of(extractFromHeaders(message.headers(), expression));
        };
    }

    private Optional<String> extractFromKey(String key) {
        return Optional.ofNullable(key).filter(k -> !k.isBlank());
    }

    private Extraction extractFromJsonDetailed(byte[] value, String expression, String topic) {
        if (value == null || value.length == 0 || expression == null) {
            return Extraction.of(Optional.empty());
        }
        try {
            // compiledPaths.get() returns the cached compiled JsonPath after the
            // first call, avoiding repeated expression parsing on the hot path.
            // Decode to String before passing to JsonPath: avoids the InputStreamReader +
            // json-smart character-by-character reader path, reducing CPU by ~15% per profile.
            JsonPath compiled = compiledPaths.get(expression);
            Object result = compiled.read(new String(value, StandardCharsets.UTF_8));
            if (result == null) {
                return Extraction.of(Optional.empty());
            }
            return Extraction.of(Optional.of(result.toString()));
        } catch (PathNotFoundException e) {
            return Extraction.of(Optional.empty());
        } catch (Exception e) {
            log.warn("EntityIdExtractor: extraction failed for topic '{}', expression '{}': {}",
                    topic, expression, e.toString());
            return Extraction.failure(e.toString());
        }
    }

    private Optional<String> extractFromHeaders(List<KafkaMessage.Header> headers, String headerName) {
        if (headers == null || headerName == null) {
            return Optional.empty();
        }
        return headers.stream()
                .filter(h -> headerName.equals(h.key()))
                .findFirst()
                .map(h -> new String(h.value(), StandardCharsets.UTF_8));
    }
}
```

`MessageRouter.java` — inject `JoxetteMetrics` and branch on `extractDetailed`:

```java
// MessageRouter.java:1-13 — add the JoxetteMetrics import:
package com.joxette.replay;

import com.joxette.management.ConfigRepository;
import com.joxette.management.EntitySourceConfig;
import com.joxette.management.EntityTypeConfig;
import com.joxette.management.IdSource;
import com.joxette.management.TopicMatcherConfig;
import com.joxette.management.TopicMode;
import com.joxette.metrics.JoxetteMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
```

```java
// MessageRouter.java:59-74 — add the field and constructor param:
    private final ConfigRepository configRepo;
    private final EntityIdExtractor extractor;
    private final JoxetteMetrics metrics;

    /** Snapshot of routing state, replaced atomically on reload(). */
    private volatile RoutingTables tables;

    public MessageRouter(ConfigRepository configRepo, EntityIdExtractor extractor, JoxetteMetrics metrics) {
        this.configRepo = configRepo;
        this.extractor  = extractor;
        this.metrics    = metrics;
        try {
            reload();
        } catch (SQLException e) {
            log.warn("MessageRouter: initial config load from DB failed ({}); starting with empty routing tables", e.getMessage());
            this.tables = new RoutingTables(Map.of(), Map.of(), Map.of(), Map.of());
        }
    }
```

```java
// MessageRouter.java:152-168 — branch on extractDetailed():
            for (EntitySourceEntry entry : entries) {
                // Try each matcher in declaration order; stop at first match
                for (EntitySourceConfig.MatcherConfig matcher : entry.matchers()) {
                    EntityIdExtractor.Extraction extraction =
                            extractor.extractDetailed(message, matcher.idSource(), matcher.idExpression());
                    if (extraction.failed()) {
                        log.warn("MessageRouter: entity-id extraction failed topic='{}' entityType='{}' " +
                                        "messageType='{}' source={} expression='{}': {}",
                                message.topic(), entry.entityType(), matcher.messageType(),
                                matcher.idSource(), matcher.idExpression(), extraction.failureReason());
                        metrics.entityExtractionFailures(message.topic(), entry.entityType()).increment();
                        continue;
                    }
                    Optional<String> entityId = extraction.value();
                    if (entityId.isPresent()) {
                        int bucketCount = t.entityBuckets().getOrDefault(entry.entityType(), 256);
                        int bucket = computeBucket(entry.entityType(), entityId.get(), bucketCount);
                        entityRoutes.add(new EntityRoute(
                                entry.entityType(),
                                entityId.get(),
                                bucket,
                                matcher.messageType(),
                                message.topic()));
                        break; // one route per entity-source entry per message
                    }
                }
                // Per-mapping mode promotion
                if (entry.mappingMode() == TopicMode.BOTH) {
                    routeToGeneral = true;
                }
            }
```

`JoxetteMetrics.java` — new counter, added right after `batchesQuarantined` from Task B:

```java
    /**
     * Counter incremented when {@link com.joxette.replay.EntityIdExtractor}
     * throws while extracting an entity id (malformed JSON, JsonPath evaluation
     * error) — excludes the normal "no id present" outcome, which is not an
     * error and must not inflate this counter.
     */
    public Counter entityExtractionFailures(String topic, String entityType) {
        return Counter.builder("joxette.recording.entity_extraction_failures")
                .description("Entity-id extraction attempts that threw, tagged by topic and entity type")
                .tag("topic", topic)
                .tag("entity_type", entityType)
                .register(registry);
    }
```

Update the three existing production-code-adjacent call sites of the old 2-arg `MessageRouter` constructor so the build stays green:

```java
// RebalanceIntegrationTest.java:98 — was:
//   generalRouter = new MessageRouter(configRepo, new EntityIdExtractor());
        generalRouter = new MessageRouter(configRepo, new EntityIdExtractor(), TEST_METRICS);
```

```java
// TopicRecorderTest.java:107 — was:
//   generalRouter  = new MessageRouter(configRepo, new EntityIdExtractor());
        generalRouter  = new MessageRouter(configRepo, new EntityIdExtractor(), TEST_METRICS);
```

```java
// TopicLifecycleActorRestartLeakTest.java (from Task A) — was:
//   MessageRouter router = new MessageRouter(configRepo, new EntityIdExtractor());
        MessageRouter router = new MessageRouter(configRepo, new EntityIdExtractor(), TEST_METRICS);
```

```java
// MessageRouterTest.java:93-96 — was:
//   private static MessageRouter routerFor(StubConfigRepository repo) {
//       return new MessageRouter(repo, new EntityIdExtractor());
//   }
    private static MessageRouter routerFor(StubConfigRepository repo) {
        // Default InstanceRoles (all roles active) so entity-routing tests exercise the full path
        return new MessageRouter(repo, new EntityIdExtractor(), TEST_METRICS);
    }
```

- [ ] **Step 4: Run test to verify it passes**
  Run: `mvn -pl joxette-service test -Dtest=EntityIdExtractorTest,MessageRouterTest,RebalanceIntegrationTest,TopicRecorderTest,TopicLifecycleActorRestartLeakTest`
  Expected: PASS

- [ ] **Step 5: Commit**
  ```bash
  git add joxette-service/src/main/java/com/joxette/replay/EntityIdExtractor.java \
          joxette-service/src/main/java/com/joxette/replay/MessageRouter.java \
          joxette-service/src/main/java/com/joxette/metrics/JoxetteMetrics.java \
          joxette-service/src/test/java/com/joxette/recording/RebalanceIntegrationTest.java \
          joxette-service/src/test/java/com/joxette/recording/TopicRecorderTest.java \
          joxette-service/src/test/java/com/joxette/recording/TopicLifecycleActorRestartLeakTest.java \
          joxette-service/src/test/java/com/joxette/replay/MessageRouterTest.java \
          joxette-service/src/test/java/com/joxette/replay/EntityIdExtractorTest.java
  git commit -m "fix(routing): distinguish entity-id extraction failures from no-match and make them observable"
  ```

---

### Task D: Decouple `CassetteRecordingBus` fanout from the write-serialization critical section

**Root cause:** `DuckLakeWriteChannel.processBatch` (`joxette-service/src/main/java/com/joxette/recording/DuckLakeWriteChannel.java:258-266`) calls `bus.publish(batch)` synchronously and inline, on the single drain virtual thread, *before* `drain()`'s loop (`DuckLakeWriteChannel.java:209-230`) can call `channel.receiveOrClosed()` again for the next queued batch. `CassetteRecordingBus.publish` (`joxette-service/src/main/java/com/joxette/recording/CassetteRecordingBus.java:203-239`) fans out to every matching `follow=true` subscriber via non-blocking `queue.offer()` — the offer itself never blocks, but the fanout loop's CPU cost (record construction, header decoding, base64 encoding, one offer per subscriber) is O(records × subscribers) and runs entirely on the drain VT. Because `DuckLakeWriteChannel` is the single global write path for *every* topic (per `CLAUDE.md`'s "Per-Topic Write Isolation Tradeoff"), a burst of subscribers on one hot topic adds write-path latency for every other topic's writes too.

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/recording/DuckLakeWriteChannel.java:1-24` (imports), `:64-65` (field), `:102-121` (`stop()`), `:258-266` (dispatch)
- Test: `joxette-service/src/test/java/com/joxette/recording/DuckLakeWriteChannelBusDecouplingTest.java` (new)

**Interfaces:**
- Consumes: `CassetteRecordingBus.publish(WriteBatch)` (existing, public and non-final — overridable by the test double), `DuckLakeWriteChannel(Connection, JoxetteProperties, CassetteRecordingBus, JoxetteMetrics)` (existing constructor, unchanged signature)
- Produces: no new public API — `busPublishExecutor` is a private implementation detail of `DuckLakeWriteChannel`

---

**REVISION (post-task-review, supersedes the executor choice below — file list and interfaces above still apply as the starting point):**

`Executors.newVirtualThreadPerTaskExecutor()` gives each dispatched `publish()` call its own independently-scheduled virtual thread with no relative-order guarantee. A task reviewer measured this empirically (JDK 25, 100,000 trials of 20 sequential same-caller submissions): **~28% showed out-of-order execution** — not a theoretical risk. Two consecutive same-topic `WriteBatch`es can now be delivered to a `follow=true` SSE/NDJSON subscriber out of cursor order, and nothing downstream re-sorts (`CassetteRecordingBus.deliver()` offers straight into a plain FIFO `ArrayBlockingQueue`). The human product owner chose the fix: replace the per-task executor with a single dedicated sequential virtual-thread worker, preserving both the decoupling-from-the-drain-VT benefit and same-topic delivery order.

**Corrected design:**

1. **`DuckLakeWriteChannel.java` field** — replace:
   ```java
   private final ExecutorService busPublishExecutor = Executors.newVirtualThreadPerTaskExecutor();
   ```
   with:
   ```java
   /**
    * Dispatches {@link CassetteRecordingBus#publish} off the drain VT so a slow
    * or blocked follow-subscriber can never delay the next batch's write.
    *
    * <p>Backed by a single persistent virtual thread (not a per-task executor):
    * {@code newVirtualThreadPerTaskExecutor()} gives no relative-order guarantee
    * across independently-scheduled tasks (confirmed empirically — see task
    * review), which would let same-topic {@code follow=true} deliveries arrive
    * out of cursor order. A single worker draining an internal FIFO queue keeps
    * publishes for the same topic strictly in submission order while still
    * running off the write-serialization critical path.
    */
   private final ExecutorService busPublishExecutor =
           Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
   ```
   `Executors.newSingleThreadExecutor(ThreadFactory)` wraps a single worker thread draining an internal unbounded task queue in strict FIFO submission order — this is a standard JDK guarantee (`ThreadPoolExecutor` with corePoolSize=maximumPoolSize=1 and an unbounded queue), not specific to virtual threads; `Thread.ofVirtual().factory()` only changes what kind of thread backs that one worker. No other code in `processBatch` changes — the `busPublishExecutor.execute(() -> { ... })` call site and its try/catch body stay exactly as originally specified.

2. **Fix the stale javadoc this change left behind in `CassetteRecordingBus.java`** (found during task review, not part of the original file list — genuine gap, not related to the executor choice): the class javadoc (`CassetteRecordingBus.java:29-30`, *"The `DuckLakeWriteChannel` drain VT calls `publish(WriteBatch)` immediately after a batch's result future completes"*) and the section comment above `publish()` (`CassetteRecordingBus.java:188-190`, *"Publish — called by `DuckLakeWriteChannel` on the drain VT"*) both still claim `publish()` runs on the drain VT. Update both to state that `publish()` is dispatched from `DuckLakeWriteChannel`'s dedicated single-threaded bus-publish worker (off the drain VT), same-topic order preserved by that worker being single-threaded, but concurrent with the next batch's DuckDB write.

3. **Test coverage must include same-topic ordering**, not just the existing not-delayed-for-unrelated-topics scenario (keep that one — it's still valid). Extend `DuckLakeWriteChannelBusDecouplingTest.java` (or add a case to it) with a test that submits several consecutive `WriteBatch`es for the *same* topic through `DuckLakeWriteChannel.submit()`, using a `CassetteRecordingBus` subclass that records the order `publish()` is invoked in (e.g. append each batch's identifying offset to a thread-safe `List` inside the overridden `publish()`), and asserts — via `Awaitility.await().atMost(...).untilAsserted(...)` on the recorded list reaching the expected size, then a direct equality/order assertion — that the recorded order exactly matches submission order across many iterations (e.g. submit 50+ batches in a tight loop to make a reordering bug likely to surface if the fix regresses back to a per-task executor).

```java
package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that a slow/blocked {@link CassetteRecordingBus#publish} call for one
 * topic's batch does not delay {@link DuckLakeWriteChannel#submit} completing
 * for an UNRELATED batch. Uses a test-double bus subclass that blocks on a
 * latch inside publish() — {@code CassetteRecordingBus} is public and
 * non-final specifically so this kind of test double can be built without any
 * production-code changes to the bus itself.
 */
class DuckLakeWriteChannelBusDecouplingTest {

    private static final String TOPIC_SLOW = "bus.decoupling.slow.topic";
    private static final String TOPIC_FAST = "bus.decoupling.fast.topic";

    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC_SLOW);
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC_FAST);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writeChannel != null) writeChannel.stop();
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void slowSubscriberFanout_doesNotDelaySubmitForUnrelatedBatch() throws Exception {
        JoxetteProperties props = new JoxetteProperties();
        CountDownLatch releaseLatch = new CountDownLatch(1);
        CountDownLatch publishStarted = new CountDownLatch(1);
        SlowRecordingBus bus = new SlowRecordingBus(props, releaseLatch, publishStarted);

        writeChannel = new DuckLakeWriteChannel(duckDB, props, bus, new JoxetteMetrics(new SimpleMeterRegistry()));
        writeChannel.start();

        // This batch's write succeeds and its publish() call (on TOPIC_SLOW) blocks
        // on releaseLatch. submit() itself returns once the DB write completes —
        // completion happens before publish() is even dispatched — so wait for
        // publish() to have actually started before measuring the second submit.
        writeChannel.submit(generalBatch(TOPIC_SLOW, 1));
        assertThat(publishStarted.await(5, TimeUnit.SECONDS))
                .as("the slow bus.publish() call must have started")
                .isTrue();

        // A second, unrelated batch on a different topic must complete quickly
        // even though the first publish() call is still blocked on releaseLatch —
        // this is only true if publish() runs off the drain VT.
        long startNanos = System.nanoTime();
        writeChannel.submit(generalBatch(TOPIC_FAST, 1));
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertThat(elapsedMs)
                .as("submit() for an unrelated topic must not wait on a blocked bus.publish() call")
                .isLessThan(2_000);

        releaseLatch.countDown();
    }

    private static WriteBatch generalBatch(String topic, int count) {
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>(count);
        List<String> types = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ConsumerRecord<String, byte[]> r = new ConsumerRecord<>(
                    topic, 0, i,
                    1_700_000_000_000L + i, TimestampType.CREATE_TIME,
                    -1, -1,
                    "k" + i,
                    ("payload-" + i).getBytes(StandardCharsets.UTF_8),
                    new RecordHeaders(),
                    Optional.empty());
            records.add(r);
            types.add(null);
        }
        return WriteBatch.of(topic, records, records, types, List.of());
    }

    /** Test double: blocks inside publish() until released, to simulate a slow subscriber fanout. */
    private static final class SlowRecordingBus extends CassetteRecordingBus {
        private final CountDownLatch releaseLatch;
        private final CountDownLatch publishStarted;

        SlowRecordingBus(JoxetteProperties props, CountDownLatch releaseLatch, CountDownLatch publishStarted) {
            super(props);
            this.releaseLatch = releaseLatch;
            this.publishStarted = publishStarted;
        }

        @Override
        public void publish(WriteBatch batch) {
            publishStarted.countDown();
            try {
                if (!releaseLatch.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("releaseLatch not released within 10s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            super.publish(batch);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**
  Run: `mvn -pl joxette-service test -Dtest=DuckLakeWriteChannelBusDecouplingTest#slowSubscriberFanout_doesNotDelaySubmitForUnrelatedBatch`
  Expected: FAIL — the test compiles fine (no new API needed), but times out: `submit(generalBatch(TOPIC_FAST, 1))` blocks for ~10 seconds (until `SlowRecordingBus.publish()`'s own internal `releaseLatch.await(10, TimeUnit.SECONDS)` times out and throws, unblocking the drain loop), so `elapsedMs` is ~10000, failing the `isLessThan(2_000)` assertion. This is because the drain thread is still synchronously inside `bus.publish(slowBatch)` and cannot dequeue `fastBatch` until that call returns.

- [ ] **Step 3: Write minimal implementation**

```java
// DuckLakeWriteChannel.java:19-20 — add ExecutorService/Executors imports next to
// the existing java.util.concurrent imports:
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
```

```java
// DuckLakeWriteChannel.java:64-65 — add the dedicated dispatch executor next to drainThread:
    private Channel<WriteBatch> channel;
    private Thread drainThread;
    /**
     * Dispatches {@link CassetteRecordingBus#publish} off the drain VT so a slow
     * or blocked follow-subscriber can never delay the next batch's write — bus
     * delivery is explicitly best-effort (see {@link CassetteRecordingBus}'s
     * class javadoc) and must never sit on the same critical path as DuckDB
     * write serialization.
     */
    private final ExecutorService busPublishExecutor = Executors.newVirtualThreadPerTaskExecutor();
```

```java
// DuckLakeWriteChannel.java:102-121 — shut the executor down in stop():
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        if (channel != null) {
            channel.done();
        }
        if (drainThread != null) {
            try {
                drainThread.join(10_000);
                if (drainThread.isAlive()) {
                    log.warn("Write drain thread did not stop within 10 s; interrupting");
                    drainThread.interrupt();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        busPublishExecutor.shutdown();
        log.info("DuckLakeWriteChannel stopped");
    }
```

```java
// DuckLakeWriteChannel.java:258-266 — dispatch publish() onto its own VT instead
// of calling it inline on the drain thread:
                batch.result().complete(new WriteResult(batch.topic(), written));
                if (bus != null) {
                    busPublishExecutor.execute(() -> {
                        try {
                            bus.publish(batch);
                        } catch (RuntimeException be) {
                            log.warn("Recording bus publish failed for topic '{}': {}",
                                    batch.topic(), be.getMessage(), be);
                        }
                    });
                }
                return;
```

- [ ] **Step 4: Run test to verify it passes**
  Run: `mvn -pl joxette-service test -Dtest=DuckLakeWriteChannelBusDecouplingTest#slowSubscriberFanout_doesNotDelaySubmitForUnrelatedBatch`
  Expected: PASS

- [ ] **Step 5: Commit**
  ```bash
  git add joxette-service/src/main/java/com/joxette/recording/DuckLakeWriteChannel.java \
          joxette-service/src/test/java/com/joxette/recording/DuckLakeWriteChannelBusDecouplingTest.java
  git commit -m "fix(recording): dispatch CassetteRecordingBus fanout off the DuckDB write-drain thread"
  ```
