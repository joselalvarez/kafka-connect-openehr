package com.github.joselalvarez.openehr.connect.source.record;

import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.*;

@Slf4j
public class LocalOffsetStorage<T> {

    interface OffsetMapper<T> {
        List<Map<String, ?>> getTablePartitions(OpenEHRSourceConnectorConfig taskConfig);
        String mapHashPartition(Map<String, ?> partition);
        T mapOffset(Map<String, ?> partition, Map<String, ?> offset);
    }

    private OffsetMapper<T> mapper;
    private Map<String, Map<String, ?>> offsetIndex = new LinkedHashMap<>();
    private Map<String, Map<String, ?>> partitionIndex = new LinkedHashMap<>();

    public LocalOffsetStorage(OffsetStorageReader reader, OpenEHRSourceConnectorConfig taskConfig, OffsetMapper<T> mapper) {

        log.info("Task[name={}]: Load offsets", taskConfig.getTaskName());
        this.mapper = mapper;
        for (Map<String, ?> partition : mapper.getTablePartitions(taskConfig)) {
            String hash = mapper.mapHashPartition(partition);
            Map<String, ?> offset = reader.offset(partition);
            offset = offset != null ? offset : new HashMap<>();
            log.info("Task[name={}]: [{}, {}]", taskConfig.getTaskName(), partition, offset);
            offsetIndex.put(hash, offset);
            partitionIndex.put(hash, partition);
        }
    }

    public List<SourceRecord> registerOffsets(List<SourceRecord> pollList) {
        if (pollList != null && !pollList.isEmpty()) {
            for (SourceRecord record : pollList){
                String hash = mapper.mapHashPartition(record.sourcePartition());
                offsetIndex.put(hash, new LinkedHashMap<>(record.sourceOffset()));
            }
        }
        return pollList;
    }

    public List<T> getCurrentOffsetList() {
        List<T> offsetList = new ArrayList<>();
        for (Map.Entry<String, Map<String, ?>> entry : offsetIndex.entrySet()) {
            Map<String, ?> partition = partitionIndex.get(entry.getKey());
            offsetList.add(mapper.mapOffset(partition, entry.getValue()));
        }
        return offsetList;
    }
}
