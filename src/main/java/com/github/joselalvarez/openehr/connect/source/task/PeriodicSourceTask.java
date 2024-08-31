package com.github.joselalvarez.openehr.connect.source.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
        List<SourceRecord> records = new ArrayList<>();
        if (!isClosedContext.get()) {
            CompletableFuture<List<SourceRecord>> compositionPoll = CompletableFuture.supplyAsync(this::periodicCompositionPoll);
            CompletableFuture<List<SourceRecord>> ehrStatusPoll = CompletableFuture.supplyAsync(this::periodicEhrStatusPoll);

            records.addAll(CompletableFuture.allOf(compositionPoll, ehrStatusPoll)
                    .thenApply(voidResult -> {
                        List<SourceRecord> combinedList = new ArrayList<>();
                        combinedList.addAll(compositionPoll.join());
                        combinedList.addAll(ehrStatusPoll.join());
                        return combinedList;
                    }).join());
        }
        lastExecutionTimeMs = System.currentTimeMillis() - start;
        log.info("Periodic poll summary [records={}, time={}ms, interval={}ms]",
                records != null ? records.size() : 0, lastExecutionTimeMs, pollIntervalMs);
        return records;
    }

    public abstract List<SourceRecord> periodicCompositionPoll();

    public abstract List<SourceRecord> periodicEhrStatusPoll();

}
