package com.joxette.compaction;

import com.joxette.config.ConditionalOnRole;
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
 * <p>Retention enforcement runs on its own separate schedule via
 * {@link RetentionScheduler} (default: daily at 01:00).  Running retention two
 * hours before compaction means the compaction job operates on already-pruned
 * data, avoiding unnecessary I/O on rows about to be deleted.
 *
 * <p>The actual thread is provided by the {@code compactionTaskScheduler} bean
 * configured in {@link com.joxette.config.SchedulingConfig}, which is a
 * single-threaded pool with a descriptive name prefix
 * ({@code joxette-compaction-}).
 */
@Component
@ConditionalOnRole("compaction")
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
