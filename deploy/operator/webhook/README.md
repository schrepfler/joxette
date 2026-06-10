# Validating admission webhook

Rejects an unsafe `JoxetteCluster` (embedded catalog with replicas>1 / split
scale-out / pekko-management, or a shared backend without a URI) at
`kubectl apply` time, using the same `CatalogGuardrail` the reconciler enforces.
The reconcile-time guardrail remains the backstop, so the webhook is **optional**
— it only moves rejection earlier and gives immediate kubectl feedback.

## TLS

The Kubernetes API server calls webhooks over HTTPS and must trust the serving
cert. `validatingwebhook.yaml` uses **cert-manager**:

1. A self-signed `Issuer` + `Certificate` mint a serving cert into the
   `joxette-operator-webhook-tls` Secret.
2. `cert-manager.io/inject-ca-from` injects the CA bundle into the
   `ValidatingWebhookConfiguration`.

The operator must serve HTTPS on the `webhook` port using that Secret. Mount it
and point Spring Boot's SSL connector at it (the operator listens on 8081 for
actuator/HTTP and a separate 8443 for the webhook). Add to the operator
Deployment:

```yaml
        ports:
          - { name: http, containerPort: 8081 }
          - { name: webhook, containerPort: 8443 }
        volumeMounts:
          - name: webhook-tls
            mountPath: /etc/webhook/tls
            readOnly: true
        env:
          - { name: SERVER_SSL_ENABLED, value: "false" }   # 8081 stays plain HTTP
          # A dedicated SSL connector for 8443 is configured in application.yml
      volumes:
        - name: webhook-tls
          secret:
            secretName: joxette-operator-webhook-tls
```

## Install

```bash
# Requires cert-manager in the cluster.
kubectl apply -f deploy/operator/webhook/validatingwebhook.yaml
```

## Without cert-manager

Generate a CA + serving cert yourself, put the serving cert/key in
`joxette-operator-webhook-tls`, and set the `ValidatingWebhookConfiguration`'s
`clientConfig.caBundle` to the base64 CA (remove the `inject-ca-from`
annotation).
