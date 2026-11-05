package com.girdharshukla.fast_job_queue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Engine {

    @FunctionalInterface
    public interface Handler {
        void handle(Job job) throws Exception;
    }

    private final JobStore jobStore;
    private final BlockingQueue<Job> queue;
    private final ExecutorService workers;
    private final Map<String, Handler> handlers = new ConcurrentHashMap<>();
    private volatile boolean running = true;
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
    private final int workerCount;
    private final ScheduledExecutorService refiller = Executors.newSingleThreadScheduledExecutor();

    public Engine(JobStore jobStore, int workerCount, int queueSize) {
        this.jobStore = jobStore;
        this.workerCount = workerCount;
        this.workers = Executors.newFixedThreadPool(workerCount);
        this.queue = new ArrayBlockingQueue<>(queueSize);
    }

    public void register(String type, Handler handler) {
        handlers.put(type, handler);
    }

    public void start() {
        long now = System.currentTimeMillis();
        for(Job j: jobStore.listByStatus(Job.Status.RUNNING)) {
            jobStore.save(j.update(Job.Status.PENDING,
                j.attemptsCompleted(),
                j.runAt(),
                j.lastError(),
                now));
        }

        for(int i = 0; i < this.workerCount; i++){
            workers.execute(this::workerLoop);
        }

        refiller.scheduleWithFixedDelay(this::refill, 0, 1, TimeUnit.SECONDS);
    }

    private void workerLoop() {
        while (running) {
            try {
                Job job = queue.poll(100, TimeUnit.MILLISECONDS);
                process(job);
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                e.printStackTrace();
            }
        }
    }

    private void refill() {
        try {
            long now = System.currentTimeMillis();
            for (Job job : jobStore.listByStatus(Job.Status.PENDING)) {
                if (job.runAt() <= now) {
                    inFlight.add(job.id());
                    if (!queue.offer(job))
                        inFlight.remove(job.id());
                }
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    private void process(Job job) {
        try {
            Job started = job.update(Job.Status.RUNNING,
                    job.attemptsCompleted() + 1,
                    job.runAt(), job.lastError(),
                    System.currentTimeMillis());

            jobStore.save(started);

            String error = null;

            try {
                Handler handler = handlers.get(started.type());

                if (handler == null) {
                    throw new IllegalStateException("No handler for job: " + started.id() + " " + started.type());
                }
                handler.handle(started);
            } catch (Exception e) {
                error = e.toString();
            }

            long now = System.currentTimeMillis();

            if (error == null) {
                jobStore.save(started.update(Job.Status.COMPLETED,
                        started.attemptsCompleted(),
                        started.runAt(),
                        null,
                        now));
            } else if (started.attemptsCompleted() >= started.maxAttempts()) {
                jobStore.save(started.update(Job.Status.DEAD,
                        started.attemptsCompleted(),
                        started.runAt(),
                        error,
                        now));
            } else {
                jobStore.save(started.update(Job.Status.PENDING,
                        started.attemptsCompleted(),
                        started.runAt(),
                        error,
                        now));
            }

        } finally {
            inFlight.remove(job.id());
        }
    }

    public void shutdown(long deadlineMs) throws InterruptedException {
        running = false;
        refiller.shutdownNow();
        workers.shutdown();
        if(!workers.awaitTermination(deadlineMs, TimeUnit.MILLISECONDS)){
            workers.shutdownNow();
        }
    }

}
