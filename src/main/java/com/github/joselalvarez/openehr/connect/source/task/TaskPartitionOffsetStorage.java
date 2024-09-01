package com.github.joselalvarez.openehr.connect.source.task;

import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceTaskConfig;
import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffset;
import com.github.joselalvarez.openehr.connect.source.task.offset.PartitionOffsetFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.*;

@Slf4j
public class TaskPartitionOffsetStorage {

    private OffsetStorageReader offsetStorage;
    private OpenEHRSourceTaskConfig taskConfig;
    private PartitionOffsetFactory partitionOffsetFactory;

    private Map<Map<String, ?>, Map<String, ?>> compositionPartitionOffsetIndex = new LinkedHashMap<>();
    private Map<Map<String, ?>, Map<String, ?>> ehrStatusPartitionOffsetIndex = new LinkedHashMap<>();

    public TaskPartitionOffsetStorage(OffsetStorageReader reader, OpenEHRSourceTaskConfig taskConfig,
                                      PartitionOffsetFactory partitionFactory) {

        log.info("Task[name={}]: Load offsets", taskConfig.getTaskName());
        this.offsetStorage = reader;
        this.taskConfig = taskConfig;
        this.partitionOffsetFactory = partitionFactory;

        this.compositionPartitionOffsetIndex = mapOffsetIndex(partitionOffsetFactory.getCompositionChangePartitionsByTask(taskConfig.getTaskId()));
        this.ehrStatusPartitionOffsetIndex = mapOffsetIndex(partitionOffsetFactory.getEhrStatusChangePartitionsByTask(taskConfig.getTaskId()));
    }

    private Map<Map<String, ?>, Map<String, ?>> mapOffsetIndex(List<Map<String, ?>> partitions) {

        Map<Map<String, ?>, Map<String, ?>> offsetIndex = new LinkedHashMap<>();

        for (Map<String, ?> part : partitionOffsetFactory.getCompositionChangePartitionsByTask(taskConfig.getTaskId())) {
            Map<String, ?> index = Collections.unmodifiableMap(new LinkedHashMap<>(part)); //Immutable
            Map<String, ?> offset = offsetStorage != null ? offsetStorage.offset(index) : null;
            offset = offset != null ? new LinkedHashMap<>(offset) : new LinkedHashMap<>(); //Copy
            log.info("Task[name={}]: [{}, {}]", taskConfig.getTaskName(), index, offset);
            offsetIndex.put(index, offset);
        }

        return offsetIndex;
    }

    private SourceRecord registerPartitionOffset(Map<Map<String, ?>, Map<String, ?>> offsetIndex, SourceRecord record) {
        Map<String, ?> index = Collections.unmodifiableMap(new LinkedHashMap<>(record.sourcePartition())); //Immutable
        offsetIndex.put(index, new LinkedHashMap<>(record.sourceOffset())); //Copy
        return record;
    }

    public SourceRecord registerCompositionChangePartitionOffset(SourceRecord record) {
        return registerPartitionOffset(compositionPartitionOffsetIndex, record);
    }

    public SourceRecord registerEhrStatusChangePartitionOffset(SourceRecord record) {
        return registerPartitionOffset(ehrStatusPartitionOffsetIndex, record);
    }

    private List<PartitionOffset> getPartitionOffsetList(Map<Map<String, ?>, Map<String, ?>> offsetIndex) {
        List<PartitionOffset> offsetList = new ArrayList<>();
        for (Map.Entry<Map<String, ?>, Map<String, ?>> entry : offsetIndex.entrySet()) {
            offsetList.add(partitionOffsetFactory.buildPartitionOffset(entry.getKey(), entry.getValue()));
        }
        return offsetList;
    }

    public List<PartitionOffset> getCompositionChangePartitionOffsets() {
        return getPartitionOffsetList(compositionPartitionOffsetIndex);
    }

    public List<PartitionOffset> getEhrStatusChangePartitionOffsets() {
        return getPartitionOffsetList(ehrStatusPartitionOffsetIndex);
    }
}
