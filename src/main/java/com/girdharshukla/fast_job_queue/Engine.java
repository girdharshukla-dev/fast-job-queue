package com.girdharshukla.fast_job_queue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    private final ExecutorService handleExec;

    public Engine(JobStore jobStore, int workerCount, int queueSize) {
        this.jobStore = jobStore;
        this.workerCount = workerCount;
        this.workers = Executors.newFixedThreadPool(workerCount);
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.handleExec = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void register(String type, Handler handler) {
        handlers.put(type, handler);
    }

    public void start() {
        long now = System.currentTimeMillis();
        for (Job j : jobStore.listByStatus(Job.Status.RUNNING)) {
            jobStore.save(j.update(Job.Status.PENDING,
                    j.attemptsCompleted(),
                    j.runAt(),
                    j.lastError(),
                    now));
        }

        for (int i = 0; i < this.workerCount; i++) {
            workers.execute(this::workerLoop);
        }

        refiller.scheduleWithFixedDelay(this::refill, 0, 1, TimeUnit.SECONDS);
    }

    public String submit(String type, byte[] payload, int maxAttempts, long timeoutMs) {
        if (!running)
            throw new IllegalStateException("Engine is shutting down");
        if (!handlers.containsKey(type))
            throw new IllegalArgumentException("Unknown job type: " + type);
        if (maxAttempts < 1 || timeoutMs <= 0)
            throw new IllegalArgumentException("Bad arguments, maxAttempts or timeoutMs");

        long now = System.currentTimeMillis();
        Job job = Job.create(type, payload, maxAttempts, now, timeoutMs, now);
        jobStore.save(job);
        // the refill will put this in the queue
        return job.id();
    }

    private void workerLoop() {
        while (running) {
            try {
                Job job = queue.poll(100, TimeUnit.MILLISECONDS);
                if (job != null)
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
                    if (inFlight.add(job.id())) {
                        if (!queue.offer(job))
                            inFlight.remove(job.id());
                    }
                }
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    private void process(Job job) throws InterruptedException {
        try {
            Job started = job.update(Job.Status.RUNNING,
                    job.attemptsCompleted() + 1,
                    job.runAt(), job.lastError(),
                    System.currentTimeMillis());

            jobStore.save(started);

            String error = null;

            Handler handler = handlers.get(started.type());
            Future<?> f = handleExec.submit(() -> {
                handler.handle(started);
                return null;
            });

            try {
                f.get(started.timeoutMs(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                f.cancel(true);
                error = "Job: " + started.id() + " timeout out after " + started.timeoutMs() + " ms";
            } catch (ExecutionException e) {
                error = e.getCause().toString();
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
                        started.runAt() + (1000L << Math.min(job.attemptsCompleted(), 6)),
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
        if (!workers.awaitTermination(deadlineMs, TimeUnit.MILLISECONDS)) {
            workers.shutdownNow();
        }

        handleExec.shutdownNow();
    }

}
