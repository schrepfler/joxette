package com.joxette.replay.transform;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.EntityRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Mutable carrier for a single message travelling through a {@link TransformPipeline}.
 *
 * <p>Pipeline steps read and write the public fields of this object directly.
 * The {@code headers} list is always a mutable {@link ArrayList}; steps may
 * add, remove, or reorder entries freely.
 *
 * <p>Constructed from an immutable {@link CassetteRecord} or {@link EntityRecord}
 * at the start of each pipeline invocation via the corresponding constructor.
 * After all steps complete, callers convert back to the appropriate immutable record
 * via {@link #toCassetteRecord()} or {@link #toEntityRecord()}.
 *
 * <p>{@code entityId} is non-null only when the message originates from an
 * {@link EntityRecord}. {@code messageType} is non-null only when the original
 * record carried a message-type label.
 */
public final class ReplayMessage {

    /** Kafka topic the message was recorded from (or redirected to by a step). */
    public String topic;

    /** Kafka partition number. */
    public int partition;

    /** Kafka offset within the partition. */
    public long offset;

    /** Kafka producer timestamp (event time). */
    public Instant timestamp;

    /** Wall-clock time when the message was recorded into the cassette. */
    public Instant recordedAt;

    /** Base64url-encoded Kafka message key (no padding). Null if absent. */
    public String key;

    /** Base64url-encoded Kafka message value (no padding). Null if value was null. */
    public String value;

    /** Mutable list of Kafka headers; never null, may be empty. */
    public List<CassetteRecord.Header> headers;

    /** Message type label from a matcher rule. Null if none matched. */
    public String messageType;

    /** Entity identifier. Non-null only when sourced from an {@link EntityRecord}. */
    public String entityId;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** Copy constructor — produces a deep copy with an independent header list. */
    public ReplayMessage(ReplayMessage source) {
        this.topic       = source.topic;
        this.partition   = source.partition;
        this.offset      = source.offset;
        this.timestamp   = source.timestamp;
        this.recordedAt  = source.recordedAt;
        this.key         = source.key;
        this.value       = source.value;
        this.headers     = new ArrayList<>(source.headers);
        this.messageType = source.messageType;
        this.entityId    = source.entityId;
    }

    /** Returns a deep copy of this message with an independent mutable header list. */
    public ReplayMessage copy() {
        return new ReplayMessage(this);
    }

    /** Constructs from a {@link CassetteRecord}. {@code entityId} will be null. */
    public ReplayMessage(CassetteRecord r) {
        this.topic       = r.topic();
        this.partition   = r.partition();
        this.offset      = r.offset();
        this.timestamp   = r.timestamp();
        this.recordedAt  = r.recordedAt();
        this.key         = r.key();
        this.value       = r.value();
        this.headers     = r.headers() != null
                           ? new ArrayList<>(r.headers())
                           : new ArrayList<>();
        this.messageType = r.messageType();
        this.entityId    = null;
    }

    /** Constructs from an {@link EntityRecord}. {@code entityId} will be set. */
    public ReplayMessage(EntityRecord r) {
        this.topic       = r.topic();
        this.partition   = r.partition();
        this.offset      = r.offset();
        this.timestamp   = r.timestamp();
        this.recordedAt  = r.recordedAt();
        this.key         = r.key();
        this.value       = r.value();
        this.headers     = r.headers() != null
                           ? new ArrayList<>(r.headers())
                           : new ArrayList<>();
        this.messageType = r.messageType();
        this.entityId    = r.entityId();
    }

    // -------------------------------------------------------------------------
    // Conversion back to immutable records
    // -------------------------------------------------------------------------

    /**
     * Produces an immutable {@link CassetteRecord} snapshot of this message.
     * {@code entityId} is not part of {@link CassetteRecord} and is discarded.
     */
    public CassetteRecord toCassetteRecord() {
        return new CassetteRecord(
                topic, partition, offset,
                timestamp, recordedAt,
                key, value,
                headers.isEmpty() ? null : List.copyOf(headers),
                messageType);
    }

    /**
     * Produces an immutable {@link EntityRecord} snapshot of this message.
     * Requires {@code entityId} to be non-null.
     *
     * @throws IllegalStateException if {@code entityId} is null
     */
    public EntityRecord toEntityRecord() {
        if (entityId == null) {
            throw new IllegalStateException(
                    "Cannot convert to EntityRecord: entityId is null");
        }
        return new EntityRecord(
                entityId, messageType,
                topic, partition, offset,
                timestamp, recordedAt,
                key, value,
                headers.isEmpty() ? null : List.copyOf(headers));
    }
}
