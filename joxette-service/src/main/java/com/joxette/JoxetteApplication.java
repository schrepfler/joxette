package com.joxette;

import com.joxette.config.JoxetteProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableConfigurationProperties(JoxetteProperties.class)
@EnableScheduling
public class JoxetteApplication {

    public static void main(String[] args) {
        System.setProperty("org.jooq.no-logo", "true");
        System.setProperty("org.jooq.no-tips",  "true");
        SpringApplication.run(JoxetteApplication.class, args);
    }
}
