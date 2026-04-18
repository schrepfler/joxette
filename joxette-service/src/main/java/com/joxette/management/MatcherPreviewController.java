package com.joxette.management;

import com.joxette.replay.EntityIdExtractor;
import com.joxette.replay.KafkaMessage;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/matchers")
public class MatcherPreviewController {

    private final EntityIdExtractor extractor;

    public MatcherPreviewController(EntityIdExtractor extractor) {
        this.extractor = extractor;
    }

    record HeaderDto(String key, String value) {}

    record PreviewRequest(
        String key,
        String value,
        List<HeaderDto> headers,
        String idSource,
        String idExpression
    ) {}

    record PreviewResponse(boolean matched, String entityId, String error) {}

    @PostMapping("/preview")
    public ResponseEntity<PreviewResponse> preview(@RequestBody PreviewRequest req) {
        if (!List.of("key", "value", "header").contains(req.idSource())) {
            return ResponseEntity.ok(new PreviewResponse(false, null,
                "Invalid idSource: must be key, value, or header"));
        }
        try {
            byte[] valueBytes = req.value() != null
                ? req.value().getBytes(StandardCharsets.UTF_8)
                : null;
            List<KafkaMessage.Header> headers = req.headers() == null ? List.of() :
                req.headers().stream()
                    .map(h -> new KafkaMessage.Header(h.key(),
                        h.value() != null ? h.value().getBytes(StandardCharsets.UTF_8) : new byte[0]))
                    .toList();
            KafkaMessage msg = new KafkaMessage(
                "preview", 0, 0, System.currentTimeMillis(),
                req.key(), valueBytes, headers
            );
            Optional<String> result = extractor.extract(msg, req.idSource(), req.idExpression());
            return ResponseEntity.ok(new PreviewResponse(result.isPresent(), result.orElse(null), null));
        } catch (Exception e) {
            return ResponseEntity.ok(new PreviewResponse(false, null, e.getMessage()));
        }
    }
}
