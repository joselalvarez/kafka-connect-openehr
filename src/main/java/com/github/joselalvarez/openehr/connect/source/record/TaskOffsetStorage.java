package com.github.joselalvarez.openehr.connect.source.record;

import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceTaskConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.*;

@Slf4j
public class TaskOffsetStorage {

    private RecordPartitionFactory partitionFactory;
    private Map<Map<String, ?>, Map<String, ?>> offsetIndex = new LinkedHashMap<>();

    public TaskOffsetStorage(OffsetStorageReader reader, OpenEHRSourceTaskConfig taskConfig,
                             RecordPartitionFactory partitionFactory) {

        log.info("Task[name={}]: Load offsets", taskConfig.getTaskName());
        this.partitionFactory = partitionFactory;
        for (Map<String, ?> part : partitionFactory.getTaskPartitions(taskConfig.getTaskId())) {
            Map<String, ?> index = Collections.unmodifiableMap(new LinkedHashMap<>(part)); //Immutable
            Map<String, ?> offset = reader != null ? reader.offset(index) : null;
            offset = offset != null ? new LinkedHashMap<>(offset) : new LinkedHashMap<>(); //Copy
            log.info("Task[name={}]: [{}, {}]", taskConfig.getTaskName(), index, offset);
            offsetIndex.put(index, offset);
        }
    }

    public List<SourceRecord> updateWith(List<SourceRecord> records) {
        if (records != null && !records.isEmpty()) {
            for (SourceRecord record : records){
                Map<String, ?> index = Collections.unmodifiableMap(new LinkedHashMap<>(record.sourcePartition())); //Immutable
                offsetIndex.put(index, new LinkedHashMap<>(record.sourceOffset())); //Copy
            }
        }
        return records;
    }

    public List<RecordOffset> getOffsetListSnapshot(Class<?> clazz) {

        List<RecordOffset> offsetList = new ArrayList<>();
        for (Map.Entry<Map<String, ?>, Map<String, ?>> entry : offsetIndex.entrySet()) {
            RecordOffset offset = partitionFactory.build(entry.getKey(), entry.getValue());
            if (offset.supports(clazz))
                offsetList.add(offset);
        }
        return offsetList;
    }
}
