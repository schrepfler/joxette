package com.joxette.replay.transform;

import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Jackson {@link SimpleModule} that registers {@link TransformStepDeserializer}
 * for the {@link TransformStep} interface only.
 *
 * <p>Registering via a module (instead of {@code @JsonDeserialize} on the interface)
 * scopes the deserializer to the exact {@code TransformStep.class} target type.
 * Jackson does <em>not</em> propagate module-registered deserializers to subtypes, so
 * deserializing a concrete step class directly (e.g. {@code om.readValue(json, RedactStep.class)})
 * uses the normal Jackson mechanism rather than triggering {@link TransformStepDeserializer}.
 *
 * <p>Register this module in the application's Jackson configuration:
 * <pre>{@code
 *   @Bean
 *   public Module transformStepJacksonModule() {
 *       return new TransformStepJacksonModule();
 *   }
 * }</pre>
 */
public class TransformStepJacksonModule extends SimpleModule {

    public TransformStepJacksonModule() {
        super("TransformStepModule");
        addDeserializer(TransformStep.class, new TransformStepDeserializer());
    }
}
