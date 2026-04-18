package com.joxette.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

/**
 * Configures Spring's scheduling infrastructure used by the compaction cron job.
 *
 * <p>{@link EnableScheduling} is declared on {@link com.joxette.JoxetteApplication}
 * so that it applies to the full application context.  This class customises the
 * scheduler thread pool and naming so that compaction threads are easy to
 * identify in thread dumps and logs.
 *
 * <p>Implementing {@link SchedulingConfigurer} routes all {@code @Scheduled}
 * tasks through the {@link #compactionTaskScheduler()} bean, ensuring they run
 * on the named, single-threaded compaction pool.
 */
@Configuration
public class SchedulingConfig implements SchedulingConfigurer {

    /**
     * Routes all {@code @Scheduled} tasks to the dedicated compaction pool.
     *
     * <p>Because this class is a CGLIB-proxied {@code @Configuration}, calling
     * {@link #compactionTaskScheduler()} here returns the same Spring-managed
     * bean instance — not a new object.
     */
    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.setScheduler(compactionTaskScheduler());
    }

    /**
     * Single-threaded scheduler dedicated to compaction.
     *
     * <p>Compaction is a sequential, I/O-heavy background operation.  A single
     * scheduler thread avoids overlapping runs and makes the scheduling model
     * easy to reason about.  If a compaction run is still active when the next
     * cron trigger fires, the trigger is skipped via the
     * {@link java.util.concurrent.atomic.AtomicBoolean} guard in
     * {@code CompactionService}.
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
