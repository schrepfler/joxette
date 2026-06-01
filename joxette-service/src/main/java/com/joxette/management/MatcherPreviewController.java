package com.joxette.management;

import com.joxette.replay.EntityIdExtractor;
import com.joxette.replay.KafkaMessage;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
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
        @NotBlank String idSource,
        @NotBlank String idExpression
    ) {}

    record PreviewResponse(boolean matched, String entityId) {}

    @PostMapping("/preview")
    public PreviewResponse preview(@Valid @RequestBody PreviewRequest req) {
        IdSource source = IdSource.fromValue(req.idSource()); // throws ValidationException on unknown value
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
        Optional<String> result = extractor.extract(msg, source, req.idExpression());
        return new PreviewResponse(result.isPresent(), result.orElse(null));
    }
}
