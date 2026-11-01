package com.girdharshukla.fast_job_queue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

import io.helidon.common.UncheckedException;

public record Job(
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
        String lastError) {
    public enum Status {
        PENDING, RUNNING, COMPLETED, DEAD;

        public boolean canMoveTo(Status next) {
            return switch (this) {
                case PENDING -> next == RUNNING;
                case RUNNING -> next == COMPLETED || next == DEAD || next == PENDING;
                case COMPLETED, DEAD -> false;
            };
        }
    }

    public static Job create(String type, byte[] payload, int maxAttempts, long runAt, long timeoutMs, long nowMs) {
        return new Job(UUID.randomUUID().toString(),
                type, payload,
                Status.PENDING,
                0,
                maxAttempts,
                runAt,
                timeoutMs,
                nowMs,
                nowMs,
                null);
    }

    public Job updateStatus(Status next, long nowMs) {
        if (!this.status.canMoveTo(next)) {
            throw new IllegalStateException("Status invalid");
        }

        return new Job(id, type, payload, next, attempts, maxAttempts, runAt, timeoutMs, createdAt, nowMs, lastError);
    }

    public byte[] encode() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF(id);
            out.writeUTF(type);
            out.writeInt(payload.length);
            out.write(payload);
            out.writeByte(status.ordinal());
            out.writeInt(attempts);
            out.writeInt(maxAttempts);
            out.writeLong(runAt);
            out.writeLong(timeoutMs);
            out.writeLong(createdAt);
            out.writeLong(updatedAt);
            out.writeBoolean(lastError != null);
            if (lastError != null)
                out.writeUTF(lastError);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedException(e);
        }
    }

    public static Job decode(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            String id = in.readUTF();
            String type = in.readUTF();
            byte[] payload = new byte[in.readInt()];
            in.readFully(payload);
            Status status = Status.values()[in.readByte()];
            int attempts = in.readInt();
            int maxAttempts = in.readInt();
            long runAt = in.readLong();
            long timeoutMs = in.readLong();
            long createdAt = in.readLong();
            long updatedAt = in.readLong();
            String lastError = in.readBoolean() ? in.readUTF() : null;
            return new Job(id, type, payload, status, attempts, maxAttempts,
                    runAt, timeoutMs, createdAt, updatedAt, lastError);
        } catch (IOException e) {
            throw new IllegalArgumentException("Corrupted job record", e);
        }
    }
}
