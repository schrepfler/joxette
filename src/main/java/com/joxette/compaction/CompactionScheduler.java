package com.joxette.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the periodic retention and compaction runs using a Spring
 * {@code @Scheduled} cron job.
 *
 * <p>The cron expression is read from {@code joxette.compaction.schedule}
 * (default {@code "0 0 3 * * *"} — daily at 03:00:00 local time).  It must
 * use Spring's 6-field cron syntax: {@code <sec> <min> <hour> <dom> <month> <dow>}.
 *
 * <p>On each trigger, retention enforcement runs first (deleting rows older than
 * each topic's / entity type's {@code retention_days}), then compaction runs on
 * the remaining data.  Both phases use their own {@link java.util.concurrent.atomic.AtomicBoolean}
 * guards to skip overlapping runs independently.
 *
 * <p>The actual thread is provided by the {@code compactionTaskScheduler} bean
 * configured in {@link com.joxette.config.SchedulingConfig}, which is a
 * single-threaded pool with a descriptive name prefix
 * ({@code joxette-compaction-}).
 */
@Component
public class CompactionScheduler {

    private static final Logger log = LoggerFactory.getLogger(CompactionScheduler.class);

    private final RetentionService retentionService;
    private final CompactionService compactionService;

    public CompactionScheduler(RetentionService retentionService,
                               CompactionService compactionService) {
        this.retentionService  = retentionService;
        this.compactionService = compactionService;
    }

    @Scheduled(cron = "${joxette.compaction.schedule}")
    public void runCompaction() {
        log.info("Scheduled retention starting");
        retentionService.runScheduled();
        log.info("Scheduled compaction starting");
        compactionService.runScheduled();
    }
}
