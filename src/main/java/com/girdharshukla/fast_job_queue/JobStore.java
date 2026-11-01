package com.girdharshukla.fast_job_queue;

import java.util.List;
import java.util.Optional;

public interface JobStore extends AutoCloseable{
    void save(Job job);
    Optional<Job> getJob(String id);
    List<Job> listByStatus(Job.Status status);

    @Override
    void close();
}
