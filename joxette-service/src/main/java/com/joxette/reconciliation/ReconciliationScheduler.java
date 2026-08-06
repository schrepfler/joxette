package com.joxette.reconciliation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the periodic catalog/object-storage reconciliation audit using a Spring
 * {@code @Scheduled} cron job, independent of compaction and retention.
 *
 * <p>The cron expression is read from {@code joxette.reconciliation.schedule}
 * (default {@code "0 0 4 * * *"} — daily at 04:00:00 local time, after retention
 * at 01:00 and compaction at 03:00). Must use Spring's 6-field cron syntax:
 * {@code <sec> <min> <hour> <dom> <month> <dow>}.
 *
 * <p>The actual thread is provided by the {@code compactionTaskScheduler} bean
 * configured in {@link com.joxette.config.SchedulingConfig}. {@link ReconciliationService}
 * guards against overlapping runs with an {@link java.util.concurrent.atomic.AtomicBoolean}
 * plus the cross-instance {@code compaction_locks} distributed lock. Scheduled runs
 * never request orphan recovery — only a manual {@code POST /compaction/trigger-reconciliation}
 * call can opt into that.
 */
@Component
@ConditionalOnProperty(name = "joxette.reconciliation.enabled", matchIfMissing = true)
public class ReconciliationScheduler {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationScheduler.class);

    private final ReconciliationService reconciliationService;

    public ReconciliationScheduler(ReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }

    @Scheduled(cron = "${joxette.reconciliation.schedule:0 0 4 * * *}")
    public void runReconciliation() {
        log.info("Scheduled reconciliation starting");
        reconciliationService.runScheduled();
    }
}
