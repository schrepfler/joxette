package com.joxette.testkit;

import com.joxette.replay.CassetteSource;
import com.joxette.replay.EntityCassetteSource;
import com.joxette.replay.ReplayEngine;
import com.joxette.replay.sink.RecordSink;

/**
 * Fluent builder that wires a {@link ReplayEngine} with explicit sources and a
 * sink. Provided so test callers don't need to keep track of which parameters are
 * mandatory (all three) versus optional — currently none. The main benefit is
 * symmetry with future sinks and sources, and an obvious defaulting for the
 * "topic-only" and "entity-only" cases.
 *
 * <p>Example — replay a pre-seeded cassette into a capturing sink:
 * <pre>{@code
 *   var engine = ReplayEngineBuilder.create()
 *       .cassetteSource(new InMemoryCassetteSource().add(rec1).add(rec2))
 *       .sink(capturingSink)
 *       .build();
 *   engine.replayTopic("t", req, 1.0, progress::add);
 * }</pre>
 */
public final class ReplayEngineBuilder {

    private CassetteSource       cassetteSource;
    private EntityCassetteSource entitySource;
    private RecordSink           sink;

    private ReplayEngineBuilder() {}

    public static ReplayEngineBuilder create() {
        return new ReplayEngineBuilder();
    }

    public ReplayEngineBuilder cassetteSource(CassetteSource cassetteSource) {
        this.cassetteSource = cassetteSource;
        return this;
    }

    public ReplayEngineBuilder entitySource(EntityCassetteSource entitySource) {
        this.entitySource = entitySource;
        return this;
    }

    public ReplayEngineBuilder sink(RecordSink sink) {
        this.sink = sink;
        return this;
    }

    /**
     * Builds the engine. Sources default to empty in-memory instances when not
     * supplied, so a test that only needs topic replay doesn't have to create an
     * entity source.
     */
    public ReplayEngine build() {
        if (sink == null) {
            throw new IllegalStateException("A RecordSink is required");
        }
        CassetteSource       cs = cassetteSource != null ? cassetteSource : new InMemoryCassetteSource();
        EntityCassetteSource es = entitySource   != null ? entitySource   : InMemoryEntityCassetteSource.empty();
        return new ReplayEngine(cs, es, sink);
    }
}
