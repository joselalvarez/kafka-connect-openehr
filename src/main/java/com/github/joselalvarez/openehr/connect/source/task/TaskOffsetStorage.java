package com.github.joselalvarez.openehr.connect.source.task;

import com.github.joselalvarez.openehr.connect.source.config.OpenEHRSourceTaskConfig;
import com.github.joselalvarez.openehr.connect.source.task.offset.RecordOffset;
import com.github.joselalvarez.openehr.connect.source.task.offset.RecordOffsetFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.*;

@Slf4j
public class TaskOffsetStorage {

    private OffsetStorageReader reader;
    private OpenEHRSourceTaskConfig taskConfig;
    private RecordOffsetFactory recordOffsetFactory;

    private Map<Map<String, ?>, Map<String, ?>> compositionOffsetIndex = new LinkedHashMap<>();
    private Map<Map<String, ?>, Map<String, ?>> ehrStatusOffsetIndex = new LinkedHashMap<>();

    public TaskOffsetStorage(OffsetStorageReader reader, OpenEHRSourceTaskConfig taskConfig,
                             RecordOffsetFactory partitionFactory) {

        log.info("Task[name={}]: Load offsets", taskConfig.getTaskName());
        this.reader = reader;
        this.taskConfig = taskConfig;
        this.recordOffsetFactory = partitionFactory;

        this.compositionOffsetIndex = mapOffsetIndex(recordOffsetFactory.getCompositionChangePartitionsByTask(taskConfig.getTaskId()));
        this.ehrStatusOffsetIndex = mapOffsetIndex(recordOffsetFactory.getEhrStatusChangePartitionsByTask(taskConfig.getTaskId()));
    }

    private Map<Map<String, ?>, Map<String, ?>> mapOffsetIndex(List<Map<String, ?>> partitions) {

        Map<Map<String, ?>, Map<String, ?>> offsetIndex = new LinkedHashMap<>();

        for (Map<String, ?> part : recordOffsetFactory.getCompositionChangePartitionsByTask(taskConfig.getTaskId())) {
            Map<String, ?> index = Collections.unmodifiableMap(new LinkedHashMap<>(part)); //Immutable
            Map<String, ?> offset = reader != null ? reader.offset(index) : null;
            offset = offset != null ? new LinkedHashMap<>(offset) : new LinkedHashMap<>(); //Copy
            log.info("Task[name={}]: [{}, {}]", taskConfig.getTaskName(), index, offset);
            offsetIndex.put(index, offset);
        }

        return offsetIndex;
    }

    private SourceRecord registerOffset(Map<Map<String, ?>, Map<String, ?>> offsetIndex, SourceRecord record) {
        Map<String, ?> index = Collections.unmodifiableMap(new LinkedHashMap<>(record.sourcePartition())); //Immutable
        offsetIndex.put(index, new LinkedHashMap<>(record.sourceOffset())); //Copy
        return record;
    }

    public SourceRecord registerCompositionChangeOffset(SourceRecord record) {
        return registerOffset(compositionOffsetIndex, record);
    }

    public SourceRecord registerEhrStatusChangeOffset(SourceRecord record) {
        return registerOffset(ehrStatusOffsetIndex, record);
    }

    private List<RecordOffset> getOffsetList(Map<Map<String, ?>, Map<String, ?>> offsetIndex) {
        List<RecordOffset> offsetList = new ArrayList<>();
        for (Map.Entry<Map<String, ?>, Map<String, ?>> entry : offsetIndex.entrySet()) {
            offsetList.add(recordOffsetFactory.buildRecordOffset(entry.getKey(), entry.getValue()));
        }
        return offsetList;
    }

    public List<RecordOffset> getCompositionChangeOffsetList() {
        return getOffsetList(compositionOffsetIndex);
    }

    public List<RecordOffset> getEhrStatusChangeOffsetList() {
        return getOffsetList(ehrStatusOffsetIndex);
    }
}
