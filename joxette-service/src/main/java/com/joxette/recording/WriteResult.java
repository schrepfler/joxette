package com.joxette.recording;

/**
 * Outcome of a single {@link WriteBatch} processed by {@link DuckLakeWriteChannel}.
 *
 * @param topic               the batch's topic
 * @param recordsWritten      rows written — general-cassette rows plus entity routes.
 *                            A single source message can fan out into more than one row
 *                            (one general row and N entity routes for {@code both}/
 *                            {@code entity_only} mode topics), so this is a row count,
 *                            not a source-message count.
 * @param sourceRecordsWritten count of distinct source Kafka messages actually written —
 *                            {@code batch.sourceRecords().size()} on a fully successful
 *                            write, {@code 0} on a quarantined batch. Use this (not
 *                            {@code recordsWritten}) for any "messages written" accounting
 *                            that must stay comparable to "messages consumed", e.g.
 *                            {@code TopicRecorder.messagesWritten} and the
 *                            {@code joxette.messages.written} metric.
 */
public record WriteResult(String topic, int recordsWritten, int sourceRecordsWritten) {}
