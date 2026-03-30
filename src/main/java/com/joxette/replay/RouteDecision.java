package com.joxette.replay;

import java.util.List;

/**
 * Full routing decision for a single {@link KafkaMessage}.
 *
 * <p>Produced by {@link MessageRouter} and consumed by the write stage of the
 * recording pipeline. A message may be routed to:
 * <ul>
 *   <li>the general cassette only ({@code mode = "general"}),</li>
 *   <li>one or more entity cassettes only ({@code mode = "entity_only"}),</li>
 *   <li>both ({@code mode = "both"}).</li>
 * </ul>
 *
 * <p>If entity ID extraction fails for a given mapping (e.g. missing JSON
 * field, null key), that mapping is silently skipped and no {@link EntityRoute}
 * is added. {@code entityRoutes} may therefore be empty even when the topic
 * mode includes entity routing.
 */
public record RouteDecision(
        KafkaMessage message,
        boolean routeToGeneral,
        List<EntityRoute> entityRoutes
) {}
