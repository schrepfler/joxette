package com.joxette.management;

public record BrokerConfig(
        String brokerId,
        String bootstrapServers,
        String securityProtocol,
        String saslMechanism,
        String saslUsername,
        String saslPassword,
        String sslTruststorePath,
        String sslTruststorePassword,
        String sslKeystorePath,
        String sslKeystorePassword
) {
    public static final String DEFAULT_BROKER_ID = "default";

    public boolean requiresSasl() {
        return "SASL_PLAINTEXT".equals(securityProtocol) || "SASL_SSL".equals(securityProtocol);
    }

    public boolean requiresSsl() {
        return "SSL".equals(securityProtocol) || "SASL_SSL".equals(securityProtocol);
    }
}
