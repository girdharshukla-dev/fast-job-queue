package com.girdharshukla.fast_job_queue;

public record Job (
    String id,
    String type,
    byte[] payload, 
    Status status, 
    int attempts,
    int maxAttempts,
    long runAt,
    long timeoutMs,
    long createdAt,
    long updatedAt, 
    String lastError
) {
    
}
