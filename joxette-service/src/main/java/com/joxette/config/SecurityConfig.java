package com.joxette.config;

import tools.jackson.databind.ObjectMapper;
import com.joxette.api.error.ProblemDetailSupport;
import com.joxette.api.error.UnauthorizedException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.ProblemDetail;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Minimal API-key authentication for state-changing endpoints.
 *
 * <p>GET/HEAD requests are always permitted unauthenticated. POST/PUT/DELETE/PATCH
 * requests require a matching {@code X-API-Key} header when {@code joxette.security.api-key}
 * is set. When the property is blank or unset, mutating endpoints are also permitted
 * unauthenticated — a startup WARN log flags this so it is not accidentally left that way
 * outside local development.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, JoxetteProperties properties,
                                                     ObjectMapper objectMapper) throws Exception {
        String apiKey = properties.getSecurity().getApiKey();
        if (apiKey == null || apiKey.isBlank()) {
            log.warn("joxette.security.api-key is not set — POST/PUT/DELETE/PATCH endpoints are " +
                    "UNAUTHENTICATED. Set joxette.security.api-key for any non-local deployment.");
        }
        http
            .csrf(csrf -> csrf.disable())
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                    .requestMatchers(HttpMethod.GET, "/**").permitAll()
                    .requestMatchers(HttpMethod.HEAD, "/**").permitAll()
                    .anyRequest().authenticated())
            .exceptionHandling(e -> e.authenticationEntryPoint(new ApiKeyAuthenticationEntryPoint(objectMapper)))
            .addFilterBefore(new ApiKeyAuthenticationFilter(apiKey), UsernamePasswordAuthenticationFilter.class);
        return http.build();
    }

    /**
     * Authenticates mutating requests (POST/PUT/DELETE/PATCH) against the configured
     * {@code X-API-Key} header. GET/HEAD and, when {@code apiKey} is blank, every request
     * is granted an authenticated anonymous principal so {@code anyRequest().authenticated()}
     * passes without requiring a header.
     */
    static final class ApiKeyAuthenticationFilter extends OncePerRequestFilter {

        private static final String HEADER = "X-API-Key";

        private final String apiKey;

        ApiKeyAuthenticationFilter(String apiKey) {
            this.apiKey = apiKey;
        }

        @Override
        protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                         FilterChain chain) throws ServletException, IOException {
            String method = request.getMethod();
            boolean mutating = "POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)
                    || "DELETE".equalsIgnoreCase(method) || "PATCH".equalsIgnoreCase(method);

            if (!mutating || apiKey == null || apiKey.isBlank()) {
                SecurityContextHolder.getContext().setAuthentication(
                        new UsernamePasswordAuthenticationToken("anonymous", null, List.of()));
                chain.doFilter(request, response);
                return;
            }

            if (apiKey.equals(request.getHeader(HEADER))) {
                SecurityContextHolder.getContext().setAuthentication(
                        new UsernamePasswordAuthenticationToken("api-key-client", null,
                                List.of(new SimpleGrantedAuthority("ROLE_API_CLIENT"))));
            }
            chain.doFilter(request, response);
        }
    }

    /**
     * Renders the same RFC 7807 {@code application/problem+json} shape as
     * {@link com.joxette.api.error.GlobalExceptionHandler} for requests rejected before
     * reaching the DispatcherServlet. Spring Security's filter chain runs upstream of
     * {@code @RestControllerAdvice}, so {@link UnauthorizedException} cannot be thrown and
     * caught there — this entry point builds the {@link ProblemDetail} itself and shares
     * {@link ProblemDetailSupport} with {@code GlobalExceptionHandler} for the extension
     * fields, so the two can't silently drift apart on the field set.
     *
     * <p>Deliberately serializes a flat {@link Map} (via {@link ProblemDetailSupport#toFlatMap})
     * rather than the {@link ProblemDetail} directly: {@code ProblemDetail}'s extension
     * properties are only flattened to the top level by {@code ProblemDetailJacksonMixin}, which
     * Spring MVC applies inside its {@code MappingJackson2HttpMessageConverter} — not on the
     * shared {@link ObjectMapper} bean. Serializing a {@code ProblemDetail} directly through the
     * raw bean (as this filter must, since it runs before the DispatcherServlet) nests those
     * fields under a {@code "properties"} object instead of matching
     * {@code GlobalExceptionHandler}'s flattened contract.
     */
    static final class ApiKeyAuthenticationEntryPoint implements AuthenticationEntryPoint {

        private final ObjectMapper objectMapper;

        ApiKeyAuthenticationEntryPoint(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void commence(HttpServletRequest request, HttpServletResponse response,
                              AuthenticationException authException) throws IOException {
            UnauthorizedException ex = UnauthorizedException.missingOrInvalidApiKey();
            ProblemDetail problem = ProblemDetail.forStatusAndDetail(ex.status(), ex.detail());
            problem.setType(ex.type());
            problem.setTitle(ex.title());
            ProblemDetailSupport.decorate(problem, ex.errorCode(), request.getRequestURI());
            Map<String, Object> body = ProblemDetailSupport.toFlatMap(problem);
            response.setStatus(ex.status().value());
            response.setHeader("WWW-Authenticate", "ApiKey realm=\"joxette\"");
            response.setContentType("application/problem+json");
            objectMapper.writeValue(response.getOutputStream(), body);
        }
    }
}
