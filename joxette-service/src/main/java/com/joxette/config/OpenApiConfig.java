package com.joxette.config;

import com.joxette.replay.transform.TransformStepJacksonModule;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.cfg.DateTimeFeature;

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
     * Customizes the application's auto-configured Jackson 3 {@code JsonMapper} rather
     * than replacing it outright: defining a {@code JsonMapper} bean directly disables
     * <em>all</em> of Spring Boot's own JsonMapper auto-configuration, including the
     * {@code ProblemDetailJsonMapperBuilderCustomizer} that makes {@code ProblemDetail}
     * flatten its {@code properties} map to top-level JSON fields — which
     * {@link com.joxette.api.error.GlobalExceptionHandler} depends on. Going through a
     * {@link JsonMapperBuilderCustomizer} bean instead keeps that (and any other
     * Boot-provided customization) intact.
     *
     * <p>Java-time (de)serialization is built into jackson-databind in Jackson 3 (no
     * JavaTimeModule needed); {@code WRITE_DATES_AS_TIMESTAMPS} is disabled so dates
     * render as ISO-8601 strings, matching the documented Replay Message Format.
     */
    @Bean
    public JsonMapperBuilderCustomizer joxetteJsonMapperBuilderCustomizer() {
        return builder -> builder
                .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
                .addModule(new TransformStepJacksonModule());
    }
}
