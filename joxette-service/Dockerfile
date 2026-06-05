# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM maven:3-eclipse-temurin-25 AS builder
WORKDIR /build

# Copy all module POMs first — this layer is cached as long as no POM changes.
COPY pom.xml .
COPY sol/pom.xml sol/pom.xml
COPY joxette-core/pom.xml joxette-core/pom.xml
COPY joxette-kafka/pom.xml joxette-kafka/pom.xml
COPY joxette-sol/pom.xml joxette-sol/pom.xml
COPY joxette-service/pom.xml joxette-service/pom.xml
COPY joxette-test-kit/pom.xml joxette-test-kit/pom.xml

# Pre-fetch external dependencies into the local Maven cache.
RUN mvn -pl joxette-service -am dependency:go-offline -q

# Copy sources and build the fat jar.
COPY sol/src sol/src
COPY joxette-core/src joxette-core/src
COPY joxette-kafka/src joxette-kafka/src
COPY joxette-sol/src joxette-sol/src
COPY joxette-service/src joxette-service/src
RUN mvn -pl joxette-service -am package -DskipTests -q

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre-alpine AS runtime
WORKDIR /app

RUN apk add --no-cache curl \
 && addgroup -S joxette \
 && adduser -S -G joxette joxette \
 && mkdir /data \
 && chown joxette:joxette /data

COPY --from=builder /build/joxette-service/target/joxette-service-*.jar /app/joxette-service.jar

USER joxette

EXPOSE 8080

ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "--enable-preview", \
  "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", \
  "-jar", "/app/joxette-service.jar"]
