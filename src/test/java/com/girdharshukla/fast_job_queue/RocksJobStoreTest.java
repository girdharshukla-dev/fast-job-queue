package com.girdharshukla.fast_job_queue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RocksJobStoreTest {

    @TempDir
    Path tempDir;

    RocksJobStore store;

    @BeforeEach
    void open() {
        store = new RocksJobStore(tempDir.resolve("db").toString());
    }

    @AfterEach
    void close() {
        store.close();
    }

    private Job newJob(String id) {
        return new Job(id, "email", "hello".getBytes(), Job.Status.PENDING,
                0, 3, 1000L, 5000L, 1000L, 1000L, null);
    }

    @Test
    void saveThenGetReturnsSameJob() {
        Job job = newJob("a");
        store.save(job);

        assertArrayEquals(job.encode(), store.getJob("a").orElseThrow().encode());
    }

    @Test
    void getUnknownIdIsEmpty() {
        assertTrue(store.getJob("missing").isEmpty());
    }

    @Test
    void savingSameIdReplacesOldRecord() {
        Job job = newJob("a");
        store.save(job);
        store.save(job.updateStatus(Job.Status.RUNNING, 2000L));

        assertEquals(Job.Status.RUNNING, store.getJob("a").orElseThrow().status());
        assertEquals(0, store.listByStatus(Job.Status.PENDING).size());
    }

    @Test
    void listByStatusReturnsOnlyMatchingJobs() {
        store.save(newJob("p"));
        store.save(newJob("r").updateStatus(Job.Status.RUNNING, 2000L));

        List<Job> result = store.listByStatus(Job.Status.PENDING);
        assertEquals(1, result.size());
        assertEquals("p", result.get(0).id());
    }
}