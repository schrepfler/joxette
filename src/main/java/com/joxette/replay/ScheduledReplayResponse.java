package com.joxette.replay;

import java.time.Instant;

/**
 * Body returned with HTTP 202 when a paginated-JSON replay request includes
 * a {@code start_at} or {@code start_delay_ms} scheduling parameter.
 */
public record ScheduledReplayResponse(String scheduledReplayId, Instant scheduledAt) {}
