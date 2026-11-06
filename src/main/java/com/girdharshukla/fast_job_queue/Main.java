package com.girdharshukla.fast_job_queue;

import io.helidon.logging.common.LogConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.helidon.config.Config;
import io.helidon.http.HeaderNames;
import io.helidon.service.registry.Services;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

public class Main {

    private Main() {
    }

    private static final ObjectMapper mapper = new ObjectMapper();

    record SubmiteRequest(String type, JsonNode payload, Integer maxAttempts, Long timeoutMs) {
    }

    record JobView(String id, String type, @JsonRawValue String payload, Job.Status status,
            int attemptsCompleted, int maxAttempts, long runAt, long timeoutMs,
            long createdAt, long updatedAt, String lastError) {

        static JobView of(Job j) {
            String payload = j.payload().length == 0 ? null : new String(j.payload(), StandardCharsets.UTF_8);
            return new JobView(j.id(), j.type(), payload, j.status(), j.attemptsCompleted(),
                    j.maxAttempts(), j.runAt(), j.timeoutMs(), j.createdAt(), j.updatedAt(), j.lastError());
        }
    }

    public static void main(String[] args) throws IOException {

        LogConfig.configureRuntime();

        Config config = Config.create();
        Services.set(Config.class, config);

        Files.createDirectories(Path.of("data"));

        RocksJobStore jobStore = new RocksJobStore("data/path");
        Engine engine = new Engine(jobStore, 4, 1000);

        engine.register("random", new Handlers.Random());
        engine.register("sleep", new Handlers.Sleep());
        engine.register("fail", new Handlers.Fail());

        WebServer server = WebServer.builder()
                .config(config.get("server"))
                .routing(r -> r
                        .post("/jobs", (req, res) -> submitJob(engine, req, res))
                        .get("/jobs/{id}", (req, res) -> getJobById(jobStore, req, res))
                        .get("/jobs", (req, res) -> list(jobStore, req, res)))
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            try {
                engine.shutdown(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            jobStore.close();
        }));

        System.out.println("Server started at port: " + server.port());

    }

    private static void submitJob(Engine engine, ServerRequest req, ServerResponse resp)
            throws JsonProcessingException {
        String id;
        try {
            SubmiteRequest body = mapper.readValue(req.content().as(String.class), SubmiteRequest.class);

            if (body.type() == null) {
                throw new IllegalArgumentException("Type is required");
            }

            byte[] payload = body.payload == null ? new byte[0] : mapper.writeValueAsBytes(body.payload);

            int maxAttempts = body.maxAttempts() == null ? 3 : body.maxAttempts();
            long timeoutMs = body.timeoutMs() == null ? 50000 : body.timeoutMs();
            id = engine.submit(body.type(), payload, maxAttempts, timeoutMs);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            resp.status(400)
                    .header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send(mapper.writeValueAsBytes(Map.of("error", String.valueOf(e.getMessage()))));
            return;
        }

        resp.status(202)
                .header(HeaderNames.CONTENT_TYPE, "application/json")
                .send(mapper.writeValueAsString(Map.of("id", id, "status", "PENDING")));
    }

    private static void getJobById(JobStore jobStore, ServerRequest req, ServerResponse resp)
            throws JsonProcessingException {
        String id = req.path().pathParameters().get("id");
        Job job = jobStore.getJob(id).orElse(null);

        if (job == null) {
            resp.status(404)
                    .header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send(mapper.writeValueAsString(Map.of("error", "No job found with id: " + id)));

            return;
        }

        resp.status(200)
                .header(HeaderNames.CONTENT_TYPE, "application/json")
                .send(mapper.writeValueAsString(JobView.of(job)));
    }

    private static void list(JobStore store, ServerRequest req, ServerResponse res) throws JsonProcessingException {
        Job.Status status = null;
        if (req.query().contains("status")) {
            try {
                status = Job.Status.valueOf(req.query().get("status").toUpperCase());
            } catch (IllegalArgumentException e) {
            }
        }
        if (status == null) {
            res.status(400)
                    .header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send(mapper.writeValueAsString(
                            Map.of("error", "status must be PENDING, RUNNING, COMPLETED or DEAD")));
            return;
        }

        List<JobView> jobs = store.listByStatus(status).stream().map(JobView::of).toList();
        res.status(200)
                .header(HeaderNames.CONTENT_TYPE, "application/json")
                .send(mapper.writeValueAsString(jobs));
    }

}