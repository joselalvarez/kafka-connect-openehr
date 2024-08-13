package com.github.joselalvarez.openehr.connect.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

@Slf4j
public abstract class PeriodicSourceTask extends SourceTask {

    private long lastExecutionTimeMs;
    private long pollIntervalMs;
    private Supplier<Boolean> isClosedContext;

    public void start(long pollIntervalMs, Supplier<Boolean> isClosedContext) {
        this.pollIntervalMs = pollIntervalMs;
        this.isClosedContext = isClosedContext;
    }

    @Override
    public final List<SourceRecord> poll() throws InterruptedException {
        if (lastExecutionTimeMs < pollIntervalMs) {
            Thread.sleep(pollIntervalMs - lastExecutionTimeMs);
        }
        long start = System.currentTimeMillis();
        List<SourceRecord> records = !isClosedContext.get() ? periodicPoll() : Collections.emptyList();
        lastExecutionTimeMs = System.currentTimeMillis() - start;
        log.info("Periodic poll summary [records={}, time={}ms, interval={}ms]",
                records != null ? records.size() : 0, lastExecutionTimeMs, pollIntervalMs);
        return records;
    }

    public abstract List<SourceRecord> periodicPoll();

}
