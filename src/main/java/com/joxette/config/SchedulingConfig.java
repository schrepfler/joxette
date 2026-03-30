package com.joxette.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Configures Spring's scheduling infrastructure used by the compaction cron job.
 *
 * <p>{@link EnableScheduling} is declared on {@link com.joxette.JoxetteApplication}
 * so that it applies to the full application context.  This class customises the
 * scheduler thread pool and naming so that compaction threads are easy to
 * identify in thread dumps and logs.
 */
@Configuration
public class SchedulingConfig {

    /**
     * Single-threaded scheduler dedicated to compaction.
     *
     * <p>Compaction is a sequential, I/O-heavy background operation.  A single
     * scheduler thread avoids overlapping runs and makes the scheduling model
     * easy to reason about.  If a compaction run is still active when the next
     * cron trigger fires, the trigger is skipped (default Spring behaviour for
     * fixed-delay tasks).
     */
    @Bean
    public TaskScheduler compactionTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("joxette-compaction-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(60);
        return scheduler;
    }
}
