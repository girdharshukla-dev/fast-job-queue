package com.girdharshukla.fast_job_queue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.ArrayList;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import com.girdharshukla.fast_job_queue.Job.Status;

public class RocksJobStore implements JobStore {

    static {
        RocksDB.loadLibrary();
    }

    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;

    public RocksJobStore(String path) {
        options = new Options().setCreateIfMissing(true);
        writeOptions = new WriteOptions().setSync(true);
        try {
            db = RocksDB.open(options, path);
        } catch (RocksDBException e) {
            options.close();
            writeOptions.close();
            throw new IllegalStateException("Could not open rock store at path: " + path, e);
        }
    }

    @Override
    public void save(Job job) {
        try {
            db.put(writeOptions, job.id().getBytes(StandardCharsets.UTF_8), job.encode());
        } catch (RocksDBException e) {
            throw new IllegalStateException("Error in saving job: " + job.id(), e);
        }
    }

    @Override
    public Optional<Job> getJob(String id) {
        try {
            byte[] val = db.get(id.getBytes(StandardCharsets.UTF_8));
            return Optional.ofNullable(val).map(Job::decode);
        } catch (RocksDBException e) {
            throw new IllegalStateException("Error in getting job: " + id, e);
        }
    }

    @Override
    public List<Job> listByStatus(Status status) {
        List<Job> result = new ArrayList<>();
        try (RocksIterator it = db.newIterator()){
            for(it.seekToFirst(); it.isValid(); it.next()){
                Job job = Job.decode(it.value());
                if (job.status() == status) result.add(job);
            }
        }
        return result;
    }

    @Override
    public void close() {
        db.close();
        options.close();
        writeOptions.close();
    }

}
