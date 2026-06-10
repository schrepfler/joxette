package com.joxette.operator.webhook;

import com.joxette.operator.cluster.CatalogGuardrail;
import com.joxette.operator.cluster.JoxetteCluster;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionResponse;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReview;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

/**
 * Validating admission webhook for {@link JoxetteCluster}: rejects an unsafe spec
 * at {@code kubectl apply} time using the same {@link CatalogGuardrail} the
 * reconciler enforces. This moves rejection earlier (apply-time vs reconcile-time)
 * and gives the user immediate kubectl feedback; the reconcile-time guardrail
 * remains the backstop.
 *
 * <p>Receives a {@code v1 AdmissionReview} and returns one with the response set.
 * Requires TLS serving certs and a {@code ValidatingWebhookConfiguration} pointing
 * here — see {@code deploy/operator/webhook/}.
 */
@RestController
public class JoxetteClusterValidatingWebhook {

    private static final Logger log = LoggerFactory.getLogger(JoxetteClusterValidatingWebhook.class);

    @PostMapping(value = "/validate/joxettecluster",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public AdmissionReview validate(@RequestBody AdmissionReview review) {
        AdmissionResponse response = new AdmissionResponse();
        if (review.getRequest() != null) {
            response.setUid(review.getRequest().getUid());
        }

        Optional<String> rejection = evaluate(review);
        if (rejection.isEmpty()) {
            response.setAllowed(true);
        } else {
            response.setAllowed(false);
            Status status = new StatusBuilder()
                    .withCode(409)
                    .withMessage(rejection.get())
                    .build();
            response.setStatus(status);
            log.info("Admission denied: {}", rejection.get());
        }

        review.setResponse(response);
        return review;
    }

    /** @return rejection reason if the reviewed JoxetteCluster is unsafe, else empty. */
    private Optional<String> evaluate(AdmissionReview review) {
        if (review.getRequest() == null || review.getRequest().getObject() == null) {
            return Optional.empty(); // nothing to validate => allow
        }
        JoxetteCluster cluster = convert(review.getRequest().getObject());
        if (cluster == null || cluster.getSpec() == null) {
            return Optional.empty();
        }
        return CatalogGuardrail.validate(cluster.getSpec());
    }

    /**
     * The admission object arrives as a generic map (Jackson) or a Fabric8 model.
     * Round-trip through Fabric8's Serialization to get a typed JoxetteCluster.
     */
    static JoxetteCluster convert(Object raw) {
        try {
            if (raw instanceof JoxetteCluster jc) {
                return jc;
            }
            String json = Serialization.asJson(raw);
            return Serialization.unmarshal(json, JoxetteCluster.class);
        } catch (Exception e) {
            // Can't parse => don't block; the reconcile-time guardrail still applies.
            return null;
        }
    }
}
