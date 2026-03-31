package com.joxette.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
}
