package com.joxette.api.error;

import org.springframework.test.web.servlet.ResultMatcher;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Shared matchers for asserting the Joxette RFC 7807 error contract against
 * a MockMvc response. Tests use these helpers to enforce that every error
 * response carries the canonical set of fields: {@code type}, {@code title},
 * {@code status}, {@code detail}, {@code errorCode}, {@code timestamp}, and
 * {@code path}.
 */
final class ProblemDetailAssertions {

    private ProblemDetailAssertions() {}

    static ResultMatcher[] problemDetail(int expectedStatus, String expectedType, String expectedErrorCode, String expectedPath) {
        return new ResultMatcher[] {
                status().is(expectedStatus),
                content().contentTypeCompatibleWith("application/problem+json"),
                jsonPath("$.type").value(expectedType),
                jsonPath("$.status").value(expectedStatus),
                jsonPath("$.title").exists(),
                jsonPath("$.detail").exists(),
                jsonPath("$.errorCode").value(expectedErrorCode),
                jsonPath("$.timestamp").exists(),
                jsonPath("$.path").value(expectedPath)
        };
    }

    static ResultMatcher contentTypeProblem() {
        return content().contentTypeCompatibleWith("application/problem+json");
    }

    static ResultMatcher noStackTraceLeak() {
        // Detail must not expose internal class names or typical stack-trace markers.
        return result -> {
            String body = result.getResponse().getContentAsString();
            if (body.contains("at java.") || body.contains("at com.joxette.") || body.contains("Exception:")) {
                throw new AssertionError("Response body appears to contain a stack trace: " + body);
            }
        };
    }

    static ResultMatcher hasHeaderProblemJson() {
        return header().string("Content-Type", org.hamcrest.Matchers.containsString("application/problem+json"));
    }
}
