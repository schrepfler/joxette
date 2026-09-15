package com.joxette.config;

import com.joxette.replay.transform.TransformStepJacksonModule;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI joxetteOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Joxette API")
                        .version("0.1.0")
                        .description("Kafka topic cassette recorder backed by DuckLake"));
    }

    /**
     * Shared Jackson 3 mapper for the whole application. Java-time (de)serialization
     * is built into jackson-databind in Jackson 3 (no JavaTimeModule needed);
     * {@code WRITE_DATES_AS_TIMESTAMPS} is disabled so dates render as ISO-8601
     * strings, matching the documented Replay Message Format.
     */
    @Bean
    @Primary
    public JsonMapper jsonMapper() {
        return JsonMapper.builder()
                .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
                .addModule(new TransformStepJacksonModule())
                .build();
    }
}
