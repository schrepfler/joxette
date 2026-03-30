package com.joxette.bootstrap;

import com.joxette.config.JoxetteProperties;
import com.joxette.model.EntitySourceMapping;
import com.joxette.model.EntityTypeConfig;
import com.joxette.model.TopicConfig;
import com.joxette.repository.ConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Loads the bootstrap seed configuration from {@code application.yml} into the
 * DuckDB config tables on first application start.
 *
 * <p>The check is based on {@code topic_configs} row count. If the table already
 * has rows the loader is a no-op, making the REST API the sole source of truth
 * from the second start onward.
 */
@Component
public class BootstrapConfigLoader implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(BootstrapConfigLoader.class);

    private final ConfigRepository repo;
    private final JoxetteProperties properties;

    public BootstrapConfigLoader(ConfigRepository repo, JoxetteProperties properties) {
        this.repo = repo;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!repo.isTopicConfigEmpty()) {
            log.info("Config tables already populated — skipping bootstrap seed");
            return;
        }
        log.info("Config tables are empty — loading bootstrap seed from application.yml");
        seed();
    }

    private void seed() throws Exception {
        JoxetteProperties.Bootstrap bootstrap = properties.getBootstrap();

        for (JoxetteProperties.Bootstrap.TopicEntry entry : bootstrap.getTopics()) {
            repo.upsertTopic(new TopicConfig(entry.getTopic(), entry.getMode()));
            log.debug("Seeded topic: {} mode={}", entry.getTopic(), entry.getMode());
        }

        for (JoxetteProperties.Bootstrap.EntityEntry entity : bootstrap.getEntities()) {
            repo.upsertEntityType(new EntityTypeConfig(entity.getType(), entity.getBuckets()));
            log.debug("Seeded entity type: {} buckets={}", entity.getType(), entity.getBuckets());

            for (JoxetteProperties.Bootstrap.EntityEntry.SourceMapping source : entity.getSources()) {
                repo.upsertMapping(new EntitySourceMapping(
                        entity.getType(),
                        source.getTopic(),
                        source.getEntityId().getSource(),
                        source.getEntityId().getExpression()
                ));
                log.debug("Seeded mapping: {} <- {} expression={}",
                        entity.getType(), source.getTopic(), source.getEntityId().getExpression());
            }
        }

        log.info("Bootstrap seed complete: {} topic(s), {} entity type(s) loaded",
                bootstrap.getTopics().size(), bootstrap.getEntities().size());
    }
}
