package com.joxette.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the periodic compaction run using a Spring {@code @Scheduled} cron job.
 *
 * <p>The cron expression is read from {@code joxette.compaction.schedule}
 * (default {@code "0 0 3 * * *"} — daily at 03:00:00 local time).  It must
 * use Spring's 6-field cron syntax: {@code <sec> <min> <hour> <dom> <month> <dow>}.
 *
 * <p>The method delegates to {@link CompactionService#runScheduled()}, which
 * uses an {@link java.util.concurrent.atomic.AtomicBoolean} guard to skip the
 * run if a previous compaction is still executing.  Overlapping runs are therefore
 * impossible regardless of how short the cron interval is.
 *
 * <p>The actual thread is provided by the {@code compactionTaskScheduler} bean
 * configured in {@link com.joxette.config.SchedulingConfig}, which is a
 * single-threaded pool with a descriptive name prefix
 * ({@code joxette-compaction-}).
 */
@Component
public class CompactionScheduler {

    private static final Logger log = LoggerFactory.getLogger(CompactionScheduler.class);

    private final CompactionService compactionService;

    public CompactionScheduler(CompactionService compactionService) {
        this.compactionService = compactionService;
    }

    @Scheduled(cron = "${joxette.compaction.schedule}")
    public void runCompaction() {
        log.info("Scheduled compaction starting");
        compactionService.runScheduled();
    }
}
