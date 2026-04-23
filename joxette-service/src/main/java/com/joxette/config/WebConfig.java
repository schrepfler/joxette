package com.joxette.config;

import com.joxette.replay.Order;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

/**
 * Global CORS configuration.
 *
 * <p>By default allows requests from the Vite dev-server ({@code http://localhost:5173})
 * and the bundled UI served from the same origin as the backend.
 * Override {@code joxette.cors.allowed-origins} in your profile-specific
 * {@code application-{profile}.yml} or via the environment variable
 * {@code JOXETTE_CORS_ALLOWED-ORIGINS} for production deployments.</p>
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final List<String> allowedOrigins;

    public WebConfig(
            @Value("${joxette.cors.allowed-origins:http://localhost:5173,http://localhost:4173}")
            List<String> allowedOrigins) {
        this.allowedOrigins = allowedOrigins;
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins(allowedOrigins.toArray(String[]::new))
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD")
                .allowedHeaders("*")
                .allowCredentials(false)
                .maxAge(3600);
    }

    @Override
    public void addFormatters(FormatterRegistry registry) {
        registry.addConverter(new Converter<String, Order>() {
            @Override
            public Order convert(String source) {
                return Order.parse(source);
            }
        });
    }
}
