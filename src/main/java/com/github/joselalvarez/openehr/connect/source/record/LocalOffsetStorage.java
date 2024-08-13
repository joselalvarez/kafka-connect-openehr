package com.github.joselalvarez.openehr.connect.source.record;

import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceConnectorConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class LocalOffsetStorage<T, O> {
    private Map<Integer, O> offsetIndex = new LinkedHashMap<>();

    public LocalOffsetStorage(OpenEHRSourceConnectorConfig taskConfig, OffsetStorageReader reader, RecordMapper<T, O> mapper) {


        log.info("Task[name={}]: Load offsets", taskConfig.getTaskName());
        for (int i = 0; i < taskConfig.getTablePatitionSize(); i++) {
            if (i % taskConfig.getTaskMax() == taskConfig.getTaskId()) {
                O offset = mapper.mapOffset(i);
                Map<String, ?> offsetMap = reader.offset(mapper.mapRecordPartition(offset));
                if (offsetMap != null) {
                    offset = mapper.mapOffset(i, offsetMap);
                }
                offsetIndex.put(i, offset);
                log.info("Task[name={}]: {}", taskConfig.getTaskName(), offset.toString());
            }
        }
    }

    public void updateOffsetsFrom(List<T> pollList, Function<T, O> offsetProvider, Function<O, Integer> partitionProvider) {
        for (T record : pollList) {
            O offset = offsetProvider.apply(record);
            offsetIndex.put(partitionProvider.apply(offset), offset);
        }
    }

    public List<O> getCurrentOffsetList() {
        return offsetIndex.values().stream().collect(Collectors.toUnmodifiableList());
    }
}
