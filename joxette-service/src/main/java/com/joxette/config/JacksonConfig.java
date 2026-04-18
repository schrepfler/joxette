package com.joxette.config;

import com.fasterxml.jackson.databind.Module;
import com.joxette.replay.transform.TransformStepJacksonModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Jackson customisation beans.
 *
 * <p>Spring Boot auto-configuration picks up any {@link Module} beans in the context and
 * registers them with the shared {@code ObjectMapper}, so no explicit wiring is needed.
 */
@Configuration
public class JacksonConfig {

    /**
     * Registers {@link TransformStepJacksonModule} with the application's
     * {@code ObjectMapper}.
     *
     * <p>This enables deserialization of {@code List<TransformStep>} pipeline
     * definitions (including the optional per-step {@code "when"} guard field)
     * while keeping the deserializer scoped to the interface type — concrete
     * step classes can still be deserialized directly without interference.
     */
    @Bean
    public Module transformStepJacksonModule() {
        return new TransformStepJacksonModule();
    }
}
