package com.joxette.exports;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.GlobalExceptionHandler;
import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.api.error.ValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class ExportControllerTest {

    @Mock ExportService service;

    private MockMvc mvc;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        mvc = MockMvcBuilders.standaloneSetup(new ExportController(service))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    private ExportJob pendingJob(String id) {
        return new ExportJob(id, "order", List.of("ORD-1"), null, null, null,
                ExportOutputFormat.PARQUET, ExportStatus.PENDING,
                null, null, null, Instant.parse("2025-01-01T00:00:00Z"), null, null);
    }

    private ExportJob completedJob(String id) {
        return new ExportJob(id, "order", List.of("ORD-1"), null, null, null,
                ExportOutputFormat.PARQUET, ExportStatus.COMPLETED,
                "s3://bucket/exports/" + id + ".parquet", 5L, null,
                Instant.parse("2025-01-01T00:00:00Z"),
                Instant.parse("2025-01-01T00:00:01Z"),
                Instant.parse("2025-01-01T00:00:02Z"));
    }

    // -------------------------------------------------------------------------
    // POST /exports
    // -------------------------------------------------------------------------

    @Test
    void submit_validRequest_returns202() throws Exception {
        when(service.submit(eq("order"), anyList(), isNull(), isNull(), isNull(),
                            eq(ExportOutputFormat.PARQUET)))
                .thenReturn(pendingJob("job-1"));

        mvc.perform(post("/exports")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"entityType\":\"order\",\"entityIds\":[\"ORD-1\"]}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").value("job-1"))
                .andExpect(jsonPath("$.status").value("pending"));
    }

    @Test
    void submit_missingEntityType_returns400() throws Exception {
        mvc.perform(post("/exports")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"entityIds\":[\"ORD-1\"]}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void submit_emptyEntityIds_returns400() throws Exception {
        mvc.perform(post("/exports")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"entityType\":\"order\",\"entityIds\":[]}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void submit_serviceValidationException_returns400() throws Exception {
        when(service.submit(any(), anyList(), any(), any(), any(), any()))
                .thenThrow(new ValidationException("entityIds list exceeds the maximum"));

        String body = mapper.writeValueAsString(
                new ExportController.CreateExportRequest(
                        "order",
                        List.of("X"),
                        null, null, null,
                        ExportOutputFormat.PARQUET));

        mvc.perform(post("/exports")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    // -------------------------------------------------------------------------
    // GET /exports
    // -------------------------------------------------------------------------

    @Test
    void list_noFilter_returnsAllJobs() throws Exception {
        when(service.list(null)).thenReturn(List.of(pendingJob("j1"), completedJob("j2")));

        mvc.perform(get("/exports"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    void list_withEntityTypeFilter_delegatesToService() throws Exception {
        when(service.list("order")).thenReturn(List.of(pendingJob("j1")));

        mvc.perform(get("/exports").param("entityType", "order"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1));
    }

    // -------------------------------------------------------------------------
    // GET /exports/{id}
    // -------------------------------------------------------------------------

    @Test
    void get_existingJob_returnsIt() throws Exception {
        when(service.get("job-1")).thenReturn(completedJob("job-1"));

        mvc.perform(get("/exports/job-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value("job-1"))
                .andExpect(jsonPath("$.status").value("completed"))
                .andExpect(jsonPath("$.outputPath").value("s3://bucket/exports/job-1.parquet"))
                .andExpect(jsonPath("$.rowCount").value(5));
    }

    @Test
    void get_unknownId_returns404() throws Exception {
        when(service.get("ghost")).thenThrow(ResourceNotFoundException.exportJob("ghost"));

        mvc.perform(get("/exports/ghost"))
                .andExpect(status().isNotFound());
    }

    // -------------------------------------------------------------------------
    // DELETE /exports/{id}
    // -------------------------------------------------------------------------

    @Test
    void delete_completedJob_returns204() throws Exception {
        mvc.perform(delete("/exports/job-1"))
                .andExpect(status().isNoContent());
    }

    @Test
    void delete_runningJob_returns400() throws Exception {
        org.mockito.Mockito.doThrow(new ValidationException("Cannot delete an export job in status: running"))
                .when(service).delete("job-running");

        mvc.perform(delete("/exports/job-running"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void delete_unknownJob_returns404() throws Exception {
        org.mockito.Mockito.doThrow(ResourceNotFoundException.exportJob("ghost"))
                .when(service).delete("ghost");

        mvc.perform(delete("/exports/ghost"))
                .andExpect(status().isNotFound());
    }
}
