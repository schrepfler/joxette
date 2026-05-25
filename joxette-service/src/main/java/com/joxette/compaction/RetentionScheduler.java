package com.joxette.compaction;

import com.joxette.config.ConditionalOnRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the periodic retention enforcement runs using a Spring
 * {@code @Scheduled} cron job, on a schedule independent of compaction.
 *
 * <p>The cron expression is read from {@code joxette.retention.schedule}
 * (default {@code "0 0 1 * * *"} — daily at 01:00:00 local time).  It must
 * use Spring's 6-field cron syntax: {@code <sec> <min> <hour> <dom> <month> <dow>}.
 *
 * <p>Running retention before compaction (01:00 vs 03:00) means the compaction
 * job operates on already-pruned data, avoiding unnecessary I/O on rows that
 * are about to be deleted.
 *
 * <p>The actual thread is provided by the {@code compactionTaskScheduler} bean
 * configured in {@link com.joxette.config.SchedulingConfig}, which is a
 * single-threaded pool.  The {@link RetentionService} guards against overlapping
 * runs with an {@link java.util.concurrent.atomic.AtomicBoolean}.
 */
@Component
@ConditionalOnRole("compaction")
public class RetentionScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetentionScheduler.class);

    private final RetentionService retentionService;

    public RetentionScheduler(RetentionService retentionService) {
        this.retentionService = retentionService;
    }

    @Scheduled(cron = "${joxette.retention.schedule:0 0 1 * * *}")
    public void runRetention() {
        log.info("Scheduled retention starting");
        retentionService.runScheduled();
    }
}
