package com.joxette.management;

import com.joxette.config.InstanceRoles;
import com.joxette.recording.RecordingCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.sql.SQLException;

/**
 * Starts recording for all non-paused topics after the full application
 * context has been initialised.
 *
 * <p>Separating this from {@link ConfigRepository} breaks the bean cycle:
 * {@code ConfigRepository} no longer needs {@link RecordingCoordinator}
 * at construction time, so the chain
 * {@code CompactionController → CompactionService → ConfigRepository → RecordingCoordinator → …}
 * is severed.
 *
 * <p>When the {@code recorder} role is not active on this instance (see
 * {@link com.joxette.config.InstanceRoles}), the runner exits immediately
 * without starting any Kafka consumers.
 */
@Component
public class RecordingStartupRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RecordingStartupRunner.class);

    private final ConfigRepository configRepository;
    private final RecordingCoordinator coordinator;
    private final InstanceRoles instanceRoles;

    public RecordingStartupRunner(ConfigRepository configRepository,
                                  @Lazy RecordingCoordinator coordinator,
                                  InstanceRoles instanceRoles) {
        this.configRepository = configRepository;
        this.coordinator      = coordinator;
        this.instanceRoles    = instanceRoles;
    }

    @Override
    public void run(ApplicationArguments args) throws SQLException {
        if (!instanceRoles.isRecorder()) {
            log.info("RecordingStartupRunner: 'recorder' role not active — skipping Kafka consumer startup");
            return;
        }
        int started = 0;
        int skipped = 0;
        for (TopicConfig tc : configRepository.listTopics()) {
            if (!tc.paused()) {
                coordinator.startTopic(tc.topic(), tc.startFrom());
                started++;
            } else {
                log.info("RecordingStartupRunner: skipping paused topic '{}'", tc.topic());
                skipped++;
            }
        }
        log.info("RecordingStartupRunner: started {} topic recorder(s), skipped {} paused topic(s)",
                started, skipped);
    }
}
