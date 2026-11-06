package com.girdharshukla.fast_job_queue;

public class Handlers {

    public static class Random implements Engine.Handler {
        @Override
        public void handle(Job job) {
            System.out.println("Random number " + Math.random() + " job id: " + job.id());
        }
    }

    public static class Sleep implements Engine.Handler {
        @Override
        public void handle(Job job) throws Exception {
            Thread.sleep(100);
            System.out.println("Job id: " + job.id());
        }
    }

    public static class Fail implements Engine.Handler {
        @Override
        public void handle(Job job) {
            throw new RuntimeException("Failed job id: " + job.id());
        }
    }
}